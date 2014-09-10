package test.location.id3

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import treelib._
import treelib.core._
import scala.collection.immutable._
import bigfoot.helpers._
import treelib.id3._
import treelib.id3.PruningForID3._

object BuildingNextLocIndividualUserModelUsingID3 {

    def main(args: Array[String]): Unit = {

        val LOCAL = true

        var inputTrainingPath = "/Users/loveallufev/Documents/MATLAB/data/newdata/datafortree/pair/data";
        var inputTestingPath = "/Users/loveallufev/Documents/MATLAB/data/newdata/datafortree/testingdata/data";
        val modelOutput =  "/Users/loveallufev/Documents/MATLAB/output/singleid3tree"
        var outputDir = "";
        var pathOfFullTree = ""
        var pathOfPrunedTree = ""

        var conf = (new SparkConf()
            .setMaster("local")
            .setAppName("Swisscom")
            )

        if (!LOCAL) {
            inputTrainingPath = "hdfs://spark-master-001:8020/user/ubuntu/input/MIT/mobile-locations-training.txt";
            inputTestingPath = "hdfs://spark-master-001:8020/user/ubuntu/input/MIT/mobile-locations-testing.txt";
            conf = (new SparkConf()
                .setMaster("spark://spark-master-001:7077")
                .setAppName("Swisscom")
                .setSparkHome("/opt/spark")
                .setJars(List("target/scala-2.10/rtree-example_2.10-1.0.jar"))
                .set("spark.executor.memory", "2000m"))
        }

        val context = new SparkContext(conf)

        val rawTrainingData = context.textFile(inputTrainingPath, 1)
        val testingData = context.textFile(inputTestingPath, 1)

        // transform data from schema:
        // Userid|year|month|day|locationAtTime1, locationAtTime2,...., locationAtTimeN
        // into:
        // userid,year,month,day,time1,locationAtTime1
        // userid,year,month,day,time2,locationAtTime2
        // ...
        
        val data = rawTrainingData.map {
            line => {
                val values = line.split('|')
                val userid = values(0)
                val year = values(1)
                val month = values(2)
                val dayOfMonth = values(3)
                val dayOfWeek = values(4)
                val currentTimeInterval = values(5)
                val currentLocation = values(6)
                val nextTimeInterval = values(7)
                val nextLocation = values(8)
                //locations.split(',').zipWithIndex.map{ 
                //    case (location, timeIndex)=>  "%s,%s,%s,%s,%s,%s".format(userid, year,month, day, timeIndex, location, 1) 
                //}
                // userid,month,dayOfMonth,dayOfWeek,currentTimeInterval,currentLoc,nextTimeInterval,nextLoc
                "%s,%s,%s,%s,%s,%s,%s,%s,%s".format(userid,year,month,dayOfMonth,dayOfWeek,currentTimeInterval, currentLocation, nextTimeInterval,nextLocation)
                //"%s,%s,%s,%s,%s".format(dayOfWeek,currentTimeInterval, currentLocation, nextTimeInterval,nextLocation)
            }
        }

        val startTime = System.nanoTime
        var count : Int = 0

        for (i <- 1 to 106) {
            try {
                val dataForThisUser = data.filter(line => line.startsWith("%s,".format(i)))
                
                val globalTree = new ID3TreeBuilder()
                globalTree.setMinSplit(10)
                globalTree.setDataset(dataForThisUser)
                globalTree.setFeatureNames(Array[String]("Userid", "Year", "Month", "DayOfMonth", "DayOfWeek", "CurrentTimeIndex", "CurrentLocation", "NextTimeIndex", "NextLocation"))
                globalTree.buildTree("NextLocation",
                Set[Any](as.String("DayOfWeek"), as.String("CurrentLocation"), as.Number("NextTimeIndex")))
                
                globalTree.treeModel.asInstanceOf[ID3TreeModel].Prune(0.01, dataForThisUser, 5)
                globalTree.writeModelToFile(modelOutput + i)
                count = count + 1

                //println("Tree:" + globalTree.treeModel)
            } catch {
                case e: Throwable => {
                    println("can not build tree for user " + i )
                    e.printStackTrace()
                }
            }
        }
        val endTime = System.nanoTime()
        println("Build tree for " + count + " users in " + (endTime - startTime)/1E9)
        
    }
}
