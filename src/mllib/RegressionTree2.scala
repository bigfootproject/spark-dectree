package mllib

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.util.Random
import scala.util.Marshal
import scala.io.Source
import java.io._
import scala.actors.remote.JavaSerializer
import java.io.DataOutputStream
import java.io.FileOutputStream
import java.io.DataInputStream
import java.io.FileInputStream

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

class Condition(val splitPoint : SplitPoint, val positive: Boolean = true) extends Serializable{
    def check(value : Any) ={
        splitPoint.point match {
            case s: Set[String] => if (positive) s.contains(value.asInstanceOf[String]) else !s.contains(value.asInstanceOf[String])
            case d: Double => if (positive) (value.asInstanceOf[Double] < d) else !(value.asInstanceOf[Double] < d)
        }
    }
}

class RegressionTree2(metadataRDD: RDD[String]) extends Serializable {
      
    // delimiter of fields in data set
    // pm: this is very generic. You could instead assume your input data is always
    // a CSV file. If it is not, then you have a pre-proccessing job to make it so
    var delimiter = ','
    
    // set of feature in dataset
    // pm: this is very generic, and indeed it depends on the input data
    // however, in "production", you wouldn't do this, as you specialize for a particular kind of data
    var featureSet = new FeatureSet(metadataRDD)
    
    // number of features
    // pm: this is "useless" as workers can derive it, since you're broadcasting featureSet to everybody
    val number_of_features = featureSet.data.length
    
    // coefficient of variation
    var threshold : Double = 0.1

    // Index of Y feature
    var yIndex = number_of_features - 1
    var xIndexs = featureSet.data.map(x => x.index).filter(x => (x != yIndex)).toSet[Int]

    // Tree model
    private var tree: Node = new Empty("Nil")

    // Minimum records to do a splitting
    var minsplit = 10

    // user partitioner or not
    //var usePartitioner = true
    //val partitioner = new HashPartitioner(contextBroadcast.value.defaultParallelism)

    def setDelimiter(c: Char) = { delimiter = c }

    /**
     * Set the minimum records of splitting
     * It's mean if a node have the number of records <= minsplit, it can't be splitted anymore
     * @xMinSplit: new minimum records for splitting
     */
    def setMinSplit(xMinSlit: Int) = { minsplit = xMinSlit }

    /**
     * Set threshold for stopping criterion. This threshold is coefficient of variation
     * A node will stop expand if Dev(Y)/E(Y) < threshold
     * In which:
     * Dev(Y) is standard deviation
     * E(Y) is medium of Y
     * @xThreshold: new threshold
     */
    def setThreshold(xThreshlod: Double) = { threshold = xThreshlod }

    //def setUsingPartitioner(value: Boolean) = { usePartitioner = value }

    /**
     * Process a line of data set
     * For each value of each feature, encapsulate it into a FeatureAgregateInfo(fetureIndex, xValue, yValue, frequency)
     * @line: array of value of each feature in a "record"
     * @numbeFeatures: the TOTAL number of feature in data set (include features which may be not processed)
     * @fTypes: type of each feature in each line (in ordered)
     * @return: an array of FeatureAggregateInfo, each element is a value of each feature on this line
     */
    private def processLine(line: Array[String], numberFeatures: Int, featureSet : FeatureSet): Array[FeatureValueAggregate] = {
        val length = numberFeatures
        var i = -1;

        parseDouble(line(yIndex)) match {
            case Some(yValue) => { // check type of Y : if isn't continuous type, return nothing
                line.map(f => { // this map is not parallel, it is executed by each worker on their part of the input RDD
                    i = (i + 1) % length
                    if (xIndexs.contains(i)) {
                        featureSet.data(i) match {
                            case nFeature : NumericalFeature => { // If this is a numerical feature => parse value from string to double
                                val v = parseDouble(f);
                                v match {
                                    case Some(d) => new FeatureValueAggregate(i, d, yValue, 1)
                                    case None => new FeatureValueAggregate(-1, f, 0, 0)
                                }
                            }
                            // if this is a categorical feature => return a FeatureAggregateInfo
                            case cFeature : CategoricalFeature => new FeatureValueAggregate(i, f, yValue, 1)
                        } // end match fType(i)
                    } // end if
                    else new FeatureValueAggregate(-1, f, 0, 0)
                }) // end map
            } // end case Some(yvalue)
            case None => { println("Y value is invalid:(%s)".format(line(yIndex))); Array[FeatureValueAggregate]() }
        } // end match Y value
    }

    /**
     * Check a sub data set has meet stop criterion or not
     * @data: data set
     * @return: true/false and average of Y
     */
    def checkStopCriterion(data: RDD[FeatureValueAggregate]): (Boolean, Double) = { //PM: since it operates on RDD it is parallel
        val yFeature = data.filter(x => x.index == yIndex)

        val yValues = yFeature.groupBy(_.yValue)

        val yAggregate = yFeature.map(x => (x.yValue, x.yValue * x.yValue))

        val ySumValue = yAggregate.reduce((x, y) => (x._1 + y._1, x._2 + y._2))
        val numTotalRecs = yFeature.reduce(_ + _).frequency
        val EY = ySumValue._1 / numTotalRecs
        val EY2 = ySumValue._2 / numTotalRecs

        val standardDeviation = math.sqrt(EY2 - EY * EY)

        (
            (
                (numTotalRecs <= minsplit) // or the number of records is less than minimum
                || (((standardDeviation < threshold) && (EY == 0)) || (standardDeviation / EY < threshold)) // or standard devariance of values of Y feature is small enough
                ),
                EY)

    }

    /**
     * Building tree bases on:
     * @yFeature: predicted feature
     * @xFeature: input features
     * @return: root of tree
     */
        def buildTree(trainedData: RDD[String], yFeature: String = featureSet.data(yIndex).Name, xFeatures: Set[String] = Set[String]()): Node = {
        // parse raw data
    	val mydata = trainedData.map(line => line.split(delimiter))
    
        var fYindex = featureSet.data.findIndexOf(p => p.Name == yFeature)

        // PM: You're sending from the "driver" to all workers the index of the Y feature, the one you're trying to predict
        if (fYindex >= 0) yIndex = featureSet.data(fYindex).index

        xIndexs =         // PM: why do you need to broadcast xIndexes?? Workers can compute it
            if (xFeatures.isEmpty) // if user didn't specify xFeature, we will process on all feature, include Y feature (to check stop criterion)
                featureSet.data.map(x => x.index).toSet[Int]
            else xFeatures.map(x => featureSet.getIndex(x)) + yIndex

        println("Number observations:" + mydata.count) // PM: This is done on the "driver", it's not parallel
        println("Total number of features:" + number_of_features) // PM: same as before

        val new_data = mydata.map(x =>  processLine(x, number_of_features, featureSet)).cache
        
        
        // build tree
        def buildIter(conditions : List[Condition]): Node = {
            
                var data = new_data.filter(
                        x => conditions.forall(
                                sp => {
                                    sp.check(x(sp.splitPoint.index).xValue)
                                }
                )).flatMap(x => x.toSeq)
            //var data = rawdata.flatMap(x => x.toSeq)
            
            val (stopExpand, eY) = checkStopCriterion(data)
            if (stopExpand) {
                new Empty(eY.toString)
            } else {
                val groupFeatureByIndexAndValue = 
                    data.groupBy(x => (x.index, x.xValue)) // PM: this operates on an RDD => in parallel

                var featureValueSorted = (
                    //data.groupBy(x => (x.index, x.xValue))
                    groupFeatureByIndexAndValue // PM: this is an RDD hence you do the map and fold in parallel (in MapReduce this would be the "reducer")
                    .map(x => (new FeatureValueAggregate(x._1._1, x._1._2, 0, 0)
                        + x._2.foldLeft(new FeatureValueAggregate(x._1._1, x._1._2, 0, 0))(_ + _)))
                    // sample results
                    //Feature(index:2 | xValue:normal | yValue6.0 | frequency:7)
                    //Feature(index:1 | xValue:sunny | yValue2.0 | frequency:5)
                    //Feature(index:2 | xValue:high | yValue3.0 | frequency:7)

                    .groupBy(x => x.index) // This is again operating on the RDD, and actually is like the continuation of the "reducer" code above
                    .map(x =>
                        (x._1, x._2.toSeq.sortBy(
                            v => v.xValue match {
                                case d: Double => d // sort by xValue if this is numerical feature
                                case s: String => v.yValue / v.frequency // sort by the average of Y if this is categorical value
                            }))))

                var splittingPointFeature = featureValueSorted.map(x => // operates on an RDD, so this is in parallel
                    x._2(0).xValue match {
                        case s: String => // process with categorical feature
                            {
                                var acc: Int = 0; // the number records on the left of current feature
                                var currentSumY: Double = 0 // current sum of Y of elements on the left of current feature
                                val numRecs: Int = x._2.foldLeft(0)(_ + _.frequency) // number of records
                                val sumY = x._2.foldLeft(0.0)(_ + _.yValue) // total sum of Y

                                var splitPoint: Set[String] = Set[String]()
                                var lastFeatureValue = new FeatureValueAggregate(-1, 0, 0, 0)
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
                                    }).drop(1).maxBy(_.weight) // select the best split // PM: please explain this trick with an example
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
                                var lastFeatureValue = new FeatureValueAggregate(-1, 0, 0, 0)
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
                    filter(_.index != yIndex).collect.
                    maxBy(_.weight) // select best feature to split
                    // PM: collect here means you're sending back all the data to a single machine (the driver).

                if (splittingPointFeature.index == -1) { // the chosen feature has only one value
                    //val commonValueY = yFeature.reduce((x, y) => if (x._2.length > y._2.length) x else y)._1
                    //new Empty(commonValueY.toString)
                    new Empty(eY.toString)
                } else {
                    val chosenFeatureInfo = featureSet.data.filter(f => f.index == splittingPointFeature.index).first

                    val leftCondition = conditions :+ new Condition(splittingPointFeature, true)
                    val rightCondition = conditions :+ new Condition(splittingPointFeature, false)
                    new NonEmpty(
                            chosenFeatureInfo,
                            splittingPointFeature.point,
                            buildIter(leftCondition),
                            buildIter(rightCondition)
                            )
                } // end of if index == -1
            }
        }

        tree = buildIter(List[Condition]())
                    
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

    
    /**
     * Evaluate the accuracy of regression tree
     * @input: an input record (uses the same delimiter with trained data set)
     */
    def evaluate(input: RDD[String], delimiter : Char = ',') {
        if (!tree.isEmpty){
            val numTest = input.count
            val inputdata = input.map(x => x.split(delimiter))
            val diff = inputdata.map(x => (predict(x).toDouble, x(yIndex).toDouble)).map(x => (x._2 - x._1, (x._2 - x._1)*(x._2-x._1)))

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
   def writeTreeToFile(path: String) = {
        
        val js = new JavaSerializer(null, null)
        val os = new DataOutputStream(new FileOutputStream(path))
        
        //js.writeObject(os, featureSet.data)
        //js.writeObject(os, tree)
        //js.writeObject(os, yIndex.value : Integer)
        js.writeObject(os, this)
        os.close
    }
}

object RegressionTree2 extends Serializable {
    
    def apply(metadataRDD: RDD[String]) = 
        new RegressionTree2(metadataRDD)
    /*
    def apply( context: SparkContext) = 
        new RegressionTree(context, context.makeRDD(List[String]()))
        * 
        */
}