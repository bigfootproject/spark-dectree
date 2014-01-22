package machinelearning
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

class OldRegressionTree {
/*
    val context = new SparkContext("local", "SparkContext")
    val dataInputURL = "/home/loveallufev/semester_project/input/small_input2"
    var featureSet = new FeatureSet("/home/loveallufev/semester_project/input/tag_small_input2", context)

    val myDataFile = context.textFile(dataInputURL, 1)
    var myDataFile2 = scala.io.Source.fromFile(dataInputURL).getLines.toStream

    var mydata = myDataFile2.map(line => line.split(","))
    val number_of_features = mydata(0).length

    if (number_of_features >= 2) println(BuildTree(mydata))
    else println("Need more than 1 feature")

    def BuildTree(data: Stream[Array[String]]): Node = {
        var i = 0;
        featureSet.data.map(x => x.clear)

        data.foreach(line => processLine(line))
        
        val yFeatureInfo = featureSet.data(number_of_features - 1)
        val yPossibleSplitPoints = yFeatureInfo.getPossibleSplitPoints
        if (yPossibleSplitPoints.length < 2) // all records has the same Y value
            if (yPossibleSplitPoints.length == 1) new Empty(yPossibleSplitPoints(0).toString) else new Empty()
        else {
            val bestSplitPoints = featureSet.data.map(x => (x, x.getBestSplitPoint)).dropRight(1)

            bestSplitPoints.foreach(println)
            val bestSplitPoint = (bestSplitPoints.maxBy(_._2._3))
            println("Best SplitPoint:" + bestSplitPoint)
            var left: Stream[Array[String]] = Stream[Array[String]]()
            var right: Stream[Array[String]] = Stream[Array[String]]()

            bestSplitPoint._2._2 match {
                case d: Double => { // This is a splitting point on numerical feature
                    left = data filter (x => (x(bestSplitPoint._1.index).toDouble < d))
                    right = data filter (x => (x(bestSplitPoint._1.index).toDouble >= d))

                    new NonEmpty(
                        bestSplitPoint._1, // featureInfo
                        (bestSplitPoint._1.Name + " < " + d, bestSplitPoint._1.Name + " >= " + d), // left + right conditions
                        BuildTree(left), // left
                        BuildTree(right) // right
                        )
                }
                case s: Set[Any] => {
                    left = data filter (x => s.contains(x(bestSplitPoint._1.index)))
                    right = data filter (x => !s.contains(x(bestSplitPoint._1.index)))
                    new NonEmpty(
                        bestSplitPoint._1, // featureInfo
                        (s.toString, (bestSplitPoint._1.getPossibleSplitPoints.toSet &~ s).toString), // left + right conditions
                        BuildTree(left), // left
                        BuildTree(right) // right
                        )
                }

            }
        }

    }

    def processLine(fields: Array[String]): Unit = {
        var i = 0;
        var yValue = parseDouble(fields(fields.length - 1))
        yValue match {
            case Some(yValueDouble) =>
                fields.map(f => {
                    featureSet.data(i).addValue(f, yValueDouble)
                    i = (i + 1) % featureSet.numberOfFeature
                })
            case None => "Invalid data"
        }
    }
    
    implicit def toString(s : Set[Any]) = "{" + s.mkString(",") + "}"

    private def parseDouble(s: String) = try { Some(s.toDouble) } catch { case _ => None }
    */ 
}