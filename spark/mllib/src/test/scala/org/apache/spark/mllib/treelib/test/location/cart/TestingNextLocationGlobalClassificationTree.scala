package test.location.cart

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import treelib._
import treelib.evaluation.Evaluation
import treelib.core._
import scala.collection.immutable._
import bigfoot.helpers._
import treelib.cart.ClassificationTree

object TestingNextLocationGlobalClassificationTree {

    def main(args: Array[String]): Unit = {
        val TIME_INTERVAL_LENGTH = 60

        val conf = new SparkConf()
            .setMaster("local")
            .setAppName("test location service")

        val context = new SparkContext(conf)
        val input = "/Users/loveallufev/Documents/MATLAB/data/testingDataWindowTimeAllUsers/testing"
        val model_path = "/Users/loveallufev/Documents/MATLAB/output/globaltree";

        val rawTestingData = context.textFile(input, 1)

        var outputstring = "userid,total,result1,result2,result3";
        
        val globalTree = new ClassificationTree()
            try {
                globalTree.loadModelFromFile(model_path)
                println("OK: Load tree from '%s' successfully".format(model_path))
            } catch {
                case e: Throwable => {
                    println("ERROR: Couldn't load tree from '%s'".format(model_path))
                    e.printStackTrace()
                }
            }
        

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
                    val time_locationString = values(4).split('!').last
                    val temp = time_locationString.split(',')
                    val time = temp(0).toInt
                    val location = temp(1)
                    val time_intervalIndex = time/TIME_INTERVAL_LENGTH
                    "%s,%s,%s,%s,%s,%s".format(userid,year,month,day,time_intervalIndex,location)
                }
        } //.filter(x => x._1 == i.toString)
        )
        
        val startTime = System.nanoTime()

        
        val predictedValues = globalTree.predict(testingData)
        val actualValues = testingData.map (x => {
            x.split(',')(5)
        })
        
        Evaluation.evaluate(predictedValues, actualValues)
        
        val endTime = System.nanoTime
        
        println("Finish Evaluation in " + (endTime - startTime)/1E9)

    }
}