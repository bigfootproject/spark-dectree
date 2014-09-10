package test.location.id3

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import treelib._
import treelib.cart._
import treelib.evaluation.Evaluation
import treelib.core._
import scala.collection.immutable._
import bigfoot.helpers._
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import java.io.File
import treelib.cart.ClassificationTree
import treelib.id3.ID3TreeBuilder

object TestIndividualLocationID3 {

    def main(args: Array[String]): Unit = {
        val TIME_INTERVAL_LENGTH = 60

        val conf = new SparkConf()
            .setMaster("local")
            .setAppName("test location service")

        val context = new SparkContext(conf)
        val input = "/Users/loveallufev/Documents/MATLAB/data/newdata/datafortree/testingdata/data"
        val model_path = "/Users/loveallufev/Documents/MATLAB/output/singleid3tree";

        val rawTestingData = context.textFile(input, 1)

        var outputstring = "userid,trueprediction,total";
        var debugstr = "";

        for (i <- 1 to 106) {

            val globalTree = new ID3TreeBuilder()
            try {
                globalTree.loadModelFromFile(model_path + i)
                println("OK: Load tree from '%s' successfully".format(model_path + i))
                println("tree:\n" + globalTree.treeModel.tree)

                // the testing data is in schema:
                // userid|year|month|day|time1,locationAtTime1!time2,locationAtTime2!.....!timeN,locationAtTimeN
                // we want to transform it into:
                // userid,year,month,day,timeIndexN,locationAtTimeN
                // where "userid,year,month,day,timeN" is the input, and "locationAtTimeN" is the expected output
                // (we don't use "time1,locationAtTime1!time2,locationAtTime2!...!timeN-1,locationAtTimeN-1")
                // because they are not necessary
                // Note: we must transform from time to time index, because the decision tree was trained with "timeIndex"
                // for example: 02:30 will be transformed into index 2, with time interval M = 60 minutes

                val testingData = (rawTestingData.map {
                    line =>
                        {
                            //println("line:" + line)
                            val values = line.split('|')
                            //println(values.mkString("|"))
                            val userid = values(0)
                            val year = values(1)
                            val month = values(2)
                            val day = values(3)
                            val dayOfWeek = values(4)
                            val time_locationStrings = values(5).split('!')
                            val num = time_locationStrings.length
                            val currentTime_LocationString = time_locationStrings(num-2).split(',')
                            val nextTime_LocationString = time_locationStrings(num-1).split(',')
                            val (currentTimeIndex, currentLocation, nextTimeIndex, nextLocation) = 
                                (currentTime_LocationString(0).toInt/TIME_INTERVAL_LENGTH, currentTime_LocationString(1),
                                        nextTime_LocationString(0).toInt/TIME_INTERVAL_LENGTH, nextTime_LocationString(1))
                            "%s,%s,%s,%s,%s,%s,%s,%s,%s".format(userid, year, month, day, dayOfWeek, 
                                    currentTimeIndex, currentLocation, nextTimeIndex, nextLocation)
                        }
                } //.filter(x => x._1 == i.toString)
                )
                
                val dataForThisUser = testingData.filter(line => line.startsWith("%s,".format(i)))
                debugstr = "%s\n%s,%s".format(debugstr, i, dataForThisUser.count)

                val predictedValues = globalTree.predict(dataForThisUser)
                val actualValues = dataForThisUser.map(x => {
                    x.split(',').last
                })

                val (truePrediction, total) = Evaluation("misclassification").evaluate(predictedValues, actualValues)
                outputstring = "%s\n%s,%s,%s".format(outputstring,i,truePrediction, total )
            } catch {
                case e: Throwable => {
                    println("ERROR when loading and evaluation for user " + i)
                    e.printStackTrace()
                }
            }
        }
        
        println(outputstring)
        println("\n\n" + debugstr)

    }
}