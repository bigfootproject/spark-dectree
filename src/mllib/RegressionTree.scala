package machinelearning

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.util.Random
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

/**
 * We will use this class to improve de-serialize speed in future, but not now !!!
 */
class MyRegistrator extends KryoRegistrator {
    override def registerClasses(kryo: Kryo) {
        kryo.register(classOf[RegressionTree])
        kryo.register(classOf[FeatureAggregateInfo])
        kryo.register(classOf[SplitPoint])
        kryo.register(classOf[FeatureSet])
    }
}

/**
 * This class is representive for each value of each feature in dataset
 * @index: index of feature in the whole data set, based zero
 * @xValue: value of current feature
 * @yValue: value of Y feature coresponding
 * @frequency : frequency of this value
 */
class FeatureAggregateInfo(val index: Int, var xValue: Any, var yValue: Double, var frequency: Int) extends Serializable {
    def addFrequency(acc: Int): FeatureAggregateInfo = { this.frequency = this.frequency + acc; this }
    def +(that: FeatureAggregateInfo) = {
        this.frequency = this.frequency + that.frequency
        this.yValue = this.yValue + that.yValue
        this
    }
    override def toString() = "Feature(index:" + index + " | xValue:" + xValue +
        " | yValue" + yValue + " | frequency:" + frequency + ")";
}

// index : index of feature
// point: the split point of this feature (it can be a Set, or a Double number)
// weight: the weight we get if we apply this splitting
class SplitPoint(val index: Int, val point: Any, val weight: Double) extends Serializable {
    override def toString = index.toString + "," + point.toString + "," + weight.toString // for debugging
}

class RegressionTree(dataRDD: RDD[String], metadataRDD: RDD[String], context: SparkContext, YIndex: Int = -1) extends Serializable {
    var delimiter = context.broadcast(',')
    val mydata = dataRDD.map(line => line.split(delimiter.value))
    val number_of_features = context.broadcast(mydata.take(1)(0).length)
    val featureSet = context.broadcast(new FeatureSet(metadataRDD))
    val featureTypes = context.broadcast(Array[String]() ++ featureSet.value.data.map(x => x.Type))
    val contextBroadcast = context.broadcast(context)
    var threshold = context.broadcast(0.1)

    // Index of Y feature
    var yIndex = context.broadcast(number_of_features.value - 1)
    var xIndexs = context.broadcast(featureSet.value.data.map(x => x.index).filter(x => (x != yIndex.value)).toSet[Int])

    // Tree model
    private var tree: Node = new Empty("Nil")

    // Minimum records to do a splitting
    var minsplit = context.broadcast(10)

    // user partitioner or not
    var usePartitioner = context.broadcast(true)
    val partitioner = context.broadcast(new HashPartitioner(contextBroadcast.value.defaultParallelism))

    def setDelimiter(c: Char) = { delimiter = contextBroadcast.value.broadcast(c) }

    /**
     * Set the minimum records of splitting
     * It's mean if a node have the number of records <= minsplit, it can't be splitted anymore
     * @xMinSplit: new minimum records for splitting
     */
    def setMinSplit(xMinSlit: Int) = { minsplit = contextBroadcast.value.broadcast(xMinSlit) }

    /**
     * Set threshold for stopping criterion. This threshold is coefficient of variation
     * A node will stop expand if Dev(Y)/E(Y) < threshold
     * In which:
     * Dev(Y) is standard deviation
     * E(Y) is medium of Y
     * @xThreshold: new threshold
     */
    def setThreshold(xThreshlod: Double) = { threshold = contextBroadcast.value.broadcast(xThreshlod) }

    def setUsingPartitioner(value: Boolean) = { usePartitioner = contextBroadcast.value.broadcast(value) }

    /**
     * Process a line of data set
     * For each value of each feature, encapsulate it into a FeatureAgregateInfo(fetureIndex, xValue, yValue, frequency)
     * @line: array of value of each feature in a "record"
     * @numbeFeatures: the TOTAL number of feature in data set (include features which may be not processed)
     * @fTypes: type of each feature in each line (in ordered)
     * @return: an array of FeatureAggregateInfo, each element is a value of each feature on this line
     */
    private def processLine(line: Array[String], numberFeatures: Int, fTypes: Array[String]): Array[FeatureAggregateInfo] = {
        val length = numberFeatures
        var i = -1;

        parseDouble(line(yIndex.value)) match {
            case Some(yValue) => { // check type of Y : if isn't continuos type, return nothing
                line.map(f => {
                    i = (i + 1) % length
                    if (xIndexs.value.contains(i)) {
                        fTypes(i) match {
                            case "c" => { // If this is a numerical feature ([c]ontinuos values) => parse value from string to double
                                val v = parseDouble(f);
                                v match {
                                    case Some(d) => new FeatureAggregateInfo(i, d, yValue, 1)
                                    case None => new FeatureAggregateInfo(-1, f, 0, 0)
                                }
                            }
                            // if this is a categorial feature ([d]iscrete values) => return a FeatureAggregateInfo
                            case "d" => new FeatureAggregateInfo(i, f, yValue, 1)
                        } // end match fType(i)
                    } // end if
                    else new FeatureAggregateInfo(-1, f, 0, 0)
                }) // end map
            } // case Some(yvalue)
            case None => { println("Y value is invalid:(%s)".format(line(yIndex.value))); Array[FeatureAggregateInfo]() }
        } // end match Y value
    }

    /**
     * Check a sub data set has meet stop criterion or not
     * @data: data set
     * @return: true/false and average of Y
     */
    def checkStopCriterion(data: RDD[FeatureAggregateInfo]): (Boolean, Double) = {
        val yFeature = data.filter(x => x.index == yIndex.value)

        val yValues = yFeature.groupBy(_.yValue)

        val yAggregate = yFeature.map(x => (x.yValue, x.yValue * x.yValue))

        val ySumValue = yAggregate.reduce((x, y) => (x._1 + y._1, x._2 + y._2))
        val numTotalRecs = yFeature.reduce(_ + _).frequency
        val EY = ySumValue._1 / numTotalRecs
        val EY2 = ySumValue._2 / numTotalRecs

        val standardDeviation = math.sqrt(EY2 - EY * EY)

        (
            (
                (numTotalRecs <= minsplit.value) // or the number of records is less than minimum
                || (((standardDeviation < threshold.value) && (EY == 0)) || (standardDeviation / EY < threshold.value)) // or the 
                ),
                EY)

    }

    /**
     * Building tree bases on:
     * @yFeature: predicted feature
     * @xFeature: input features
     * @return: root of tree
     */
    def buildTree(yFeature: String = featureSet.value.data(yIndex.value).Name, xFeatures: Set[String] = Set[String]()): Node = {

        //def buildIter(rawdata: RDD[Array[FeatureAggregateInfo]]): Node = {
        def buildIter(rawdata: RDD[(Int, Array[FeatureAggregateInfo])]): Node = {

            //var data = rawdata.flatMap(x => x.toSeq).cache
            var data = rawdata.flatMap(x => x._2.toSeq).cache

            val (stopExpand, eY) = checkStopCriterion(data)
            if (stopExpand) {
                new Empty(eY.toString)
            } else {
                val groupFeatureByIndexAndValue = if (usePartitioner.value)
                    data.groupBy(x => (x.index, x.xValue)).partitionBy(partitioner.value)
                else
                    data.groupBy(x => (x.index, x.xValue))

                var featureValueSorted = (
                    //data.groupBy(x => (x.index, x.xValue))
                    groupFeatureByIndexAndValue
                    .map(x => (new FeatureAggregateInfo(x._1._1, x._1._2, 0, 0)
                        + x._2.foldLeft(new FeatureAggregateInfo(x._1._1, x._1._2, 0, 0))(_ + _)))
                    // sample results
                    //Feature(index:2 | xValue:normal | yValue6.0 | frequency:7)
                    //Feature(index:1 | xValue:sunny | yValue2.0 | frequency:5)
                    //Feature(index:4 | xValue:14.5 | yValue1.0 | frequency:1)
                    //Feature(index:2 | xValue:high | yValue3.0 | frequency:7)

                    .groupBy(x => x.index)
                    .map(x =>
                        (x._1, x._2.toSeq.sortBy(
                            v => v.xValue match {
                                case d: Double => d // sort by xValue if this is numerical feature
                                case s: String => v.yValue / v.frequency // sort by the average of Y if this is categorical value
                            }))))

                var splittingPointFeature = featureValueSorted.map(x =>
                    x._2(0).xValue match {
                        case s: String => // process with categorical feature
                            {
                                var acc: Int = 0; // the number records on the left of current feature
                                var currentSumY: Double = 0 // current sum of Y of elements on the left of current feature
                                val numRecs: Int = x._2.foldLeft(0)(_ + _.frequency) // number of records
                                val sumY = x._2.foldLeft(0.0)(_ + _.yValue) // total sum of Y

                                var splitPoint: Set[String] = Set[String]()
                                var lastFeatureValue = new FeatureAggregateInfo(-1, 0, 0, 0)
                                try {
                                    x._2.map(f => {

                                        if (lastFeatureValue.index == -1) {
                                            lastFeatureValue = f
                                            new SplitPoint(x._1, Set(), 0.0)
                                        } else {
                                            currentSumY = currentSumY + lastFeatureValue.yValue
                                            splitPoint = splitPoint + lastFeatureValue.xValue.asInstanceOf[String]
                                            acc = acc + lastFeatureValue.frequency
                                            val weight = currentSumY * currentSumY / acc + (sumY - currentSumY) * (sumY - currentSumY) / (numRecs - acc)
                                            lastFeatureValue = f
                                            new SplitPoint(x._1, splitPoint, weight)
                                        }
                                    }).drop(1).maxBy(_.weight) // select the best split
                                    // we drop 1 element because with Set{A,B,C} , the best split point only be {A} or {A,B}
                                } catch {
                                    case e: UnsupportedOperationException => new SplitPoint(-1, 0.0, 0.0)
                                }
                            }
                        case d: Double => // process with numerical feature
                            {
                                var acc: Int = 0 // number of records on the left of the current element
                                val numRecs: Int = x._2.foldLeft(0)(_ + _.frequency)
                                var currentSumY: Double = 0
                                val sumY = x._2.foldLeft(0.0)(_ + _.yValue)
                                var posibleSplitPoint: Double = 0
                                var lastFeatureValue = new FeatureAggregateInfo(-1, 0, 0, 0)
                                try {
                                    x._2.map(f => {

                                        if (lastFeatureValue.index == -1) {
                                            lastFeatureValue = f
                                            new SplitPoint(x._1, 0.0, 0.0)
                                        } else {
                                            posibleSplitPoint = (f.xValue.asInstanceOf[Double] + lastFeatureValue.xValue.asInstanceOf[Double]) / 2;
                                            currentSumY = currentSumY + lastFeatureValue.yValue
                                            acc = acc + lastFeatureValue.frequency
                                            val weight = currentSumY * currentSumY / acc + (sumY - currentSumY) * (sumY - currentSumY) / (numRecs - acc)
                                            lastFeatureValue = f
                                            new SplitPoint(x._1, posibleSplitPoint, weight)
                                        }
                                    }).drop(1).maxBy(_.weight) // select the best split
                                } catch {
                                    case e: UnsupportedOperationException => new SplitPoint(-1, 0.0, 0.0)
                                }
                            } // end of matching double
                    } // end of matching xValue
                    ).
                    filter(_.index != yIndex.value).collect.maxBy(_.weight) // select best feature to split

                if (splittingPointFeature.index == -1) { // the chosen feature has only one value
                    //val commonValueY = yFeature.reduce((x, y) => if (x._2.length > y._2.length) x else y)._1
                    //new Empty(commonValueY.toString)
                    new Empty(eY.toString)
                } else {
                    val chosenFeatureInfo = contextBroadcast.value.broadcast(featureSet.value.data.filter(f => f.index == splittingPointFeature.index).first)

                    splittingPointFeature.point match {
                        case s: Set[String] => { // split on categorical feature
                            val left = rawdata.filter(x => s.contains(x._2(chosenFeatureInfo.value.index).xValue.asInstanceOf[String]))
                            val right = rawdata.filter(x => !s.contains(x._2(chosenFeatureInfo.value.index).xValue.asInstanceOf[String]))
                            new NonEmpty(
                                chosenFeatureInfo.value, // featureInfo
                                s, // left + right conditions
                                buildIter(left), // left
                                buildIter(right) // right
                                )
                        }
                        case d: Double => { // split on numerical feature
                            val left = rawdata.filter(x => (x._2(chosenFeatureInfo.value.index).xValue.asInstanceOf[Double] < d))
                            val right = rawdata.filter(x => (x._2(chosenFeatureInfo.value.index).xValue.asInstanceOf[Double] >= d))
                            new NonEmpty(
                                chosenFeatureInfo.value, // featureInfo
                                d, // left + right conditions
                                buildIter(left), // left
                                buildIter(right) // right
                                )
                        }
                    } // end of matching
                } // end of if index == -1
            }
        }

        var fYindex = featureSet.value.data.findIndexOf(p => p.Name == yFeature)

        if (fYindex >= 0) yIndex = contextBroadcast.value.broadcast(featureSet.value.data(fYindex).index)
        xIndexs =
            if (xFeatures.isEmpty) // if user didn't specify xFeature, we will process on all feature, include Y feature (to check stop criterion)
                contextBroadcast.value.broadcast(featureSet.value.data.map(x => x.index).toSet[Int])
            else contextBroadcast.value.broadcast(xFeatures.map(x => featureSet.value.getIndex(x)) + yIndex.value)

        println("Number observations:" + mydata.count)
        println("Total number of features:" + number_of_features.value)

        tree = if (usePartitioner.value) {
            buildIter(mydata.map(x =>
                (Random.nextInt(number_of_features.value),
                    processLine(x, number_of_features.value, featureTypes.value))).partitionBy(partitioner.value))
        } else {
            buildIter(mydata.map(x =>
                (Random.nextInt(number_of_features.value),
                    processLine(x, number_of_features.value, featureTypes.value))))
        }
        //buildIter(mydata.map(processLine(_, number_of_features.value, featureTypes.value)))
        tree

    }

    /**
     * Parse a string to double
     */
    private def parseDouble(s: String) = try { Some(s.toDouble) } catch { case _ => None }

    /**
     * Predict Y base on input features
     * @record: an array, which its each element is a value of each input feature
     */
    def predict(record: Array[String]): String = {

        def predictIter(root: Node): String = {
            if (root.isEmpty) root.value.toString
            else root.condition match {
                case s: Set[String] => {
                    if (s.contains(record(root.feature.index))) predictIter(root.left)
                    else predictIter(root.right)
                }
                case d: Double => {
                    if (record(root.feature.index).toDouble < d) predictIter(root.left)
                    else predictIter(root.right)
                }
            }

        }
        if (tree.isEmpty) "Please build tree first"
        else predictIter(tree)
    }

    def evaluate(input: RDD[String]) {
        if (!tree.isEmpty){
            val numTest = input.count
            val inputdata = input.map(x => x.split(delimiter.value))
            val diff = inputdata.map(x => (predict(x).toDouble, x(yIndex.value).toDouble)).map(x => (x._2 - x._1, (x._2 - x._1)*(x._2-x._1)))

            val sums = diff.reduce((x,y) => (x._1 + y._1, x._2 + y._2))

            val meanDiff = sums._1/numTest
            val meanDiffPower2 = sums._2/numTest
            val deviation = math.sqrt(meanDiffPower2 - meanDiff*meanDiff)
            val SE = deviation/numTest
            
            println("Mean of different:%f\nDeviation of different:%f\nSE of different:%f".format(meanDiff, deviation, SE) )
        }else {
            "Please build tree first"
        }
    }
}

