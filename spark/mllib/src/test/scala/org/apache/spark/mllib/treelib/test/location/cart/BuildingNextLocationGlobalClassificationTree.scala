package test.location.cart

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import treelib._
import treelib.core._
import scala.collection.immutable._
import bigfoot.helpers._
import treelib.cart.ClassificationTree
import scala.Array.canBuildFrom

object BuildingNextLocationGlobalClassificationTree  {
    def main(args: Array[String]): Unit = {

        val LOCAL = true

        var inputTrainingPath = "/Users/loveallufev/Documents/MATLAB/data/newdata/training";
        var inputTestingPath = "/Users/loveallufev/Documents/MATLAB/data/newdata/testing";
        val modelOutput =  "/Users/loveallufev/Documents/MATLAB/output/globaltree"
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
        
        val data = rawTrainingData.flatMap {
            line => {
                val values = line.split('|')
                val userid = values(0)
                val year = values(1)
                val month = values(2)
                val day = values(3)
                val locations = values(4)
                locations.split(',').zipWithIndex.map{ 
                    case (location, timeIndex)=>  "%s,%s,%s,%s,%s,%s".format(userid, year,month, day, timeIndex, location, 1) 
                }
            }
        }
        
        val startTime = System.nanoTime
        
        val globalTree = new ClassificationTree()
        globalTree.setDataset(data)
        globalTree.setFeatureNames(Array[String]("Userid", "Year", "Month", "Day", "TimeIndex", "Location"))
        globalTree.buildTree("Location", Set[Any](as.String("Month"), as.String("Day"), as.String("TimeIndex")))
        
        val endTime = System.nanoTime
        
        globalTree.writeModelToFile(modelOutput)
        
        println("Tree:" + globalTree.treeModel)
        println("Building tree in " + (endTime - startTime)/1E9 + " seconds")
        
    }
}
