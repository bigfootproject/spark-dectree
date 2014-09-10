import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import treelib._
import treelib.cart.RegressionTree
import treelib.evaluation.Evaluation
import treelib.core._
import scala.collection.immutable._
import bigfoot.helpers._
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import java.io.File

object NextLocationModelTester {
	def main(args: Array[String]): Unit = {
			
	    val LOCAL = true

        var inputTrainingPath = "/Users/loveallufev/Documents/MATLAB/mobile-locations-training.txt";
        var inputTestingPath = "/Users/loveallufev/Documents/MATLAB/mobile-locations-testing.txt";

        var conf = (new SparkConf()
            .setMaster("local")
            .setAppName("Swisscom"))

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

        val trainingData = context.textFile(inputTrainingPath, 1)
        val testingData = context.textFile(inputTestingPath, 1)
        
        var filteredData = testingData.filter(line => {
            var values = line.split(",")
            if (values(3) == "1") false
            else true
        }) // filter no signal records
        
        var USERID = "0"

        // EVALUATE USING THE GLOBAL MODEL (ONE MODEL FOR EVERYONE)
        val globalTree = new RegressionTree()
        globalTree.loadModelFromFile("/tmp/allusers.model")
            
        //println("\n\n================== EVALUATE GLOBAL MODEL ===================\n\n")
        
        Utility.printToFile(new File("globalmodel.result"))(p => {
        
        for (i <- (1 to 106)) {
            try {
                USERID = i.toString
                println("Evaluate on user " + USERID + "\n\n")
                var fiteredDataForSingleUser = filteredData.filter(line =>
                    {
                        var values = line.split(",")
                        if (values(0) != USERID) false
                        else true
                    })

                
                val individualTree = new RegressionTree()
                
                individualTree.loadModelFromFile("/tmp/user" + USERID + ".model")
                
                val actualValues = fiteredDataForSingleUser.map( line => {
                    var values = line.split(",")
                    values(8).toLong
                })
                
                val predictedValuesOfGlobalTree = globalTree.predict(fiteredDataForSingleUser)
                val predictedValuesOfIndividualTree = individualTree.predict(fiteredDataForSingleUser)
                
                val zipPredictedAndActualValuesOfGlobalTree = (predictedValuesOfGlobalTree zip actualValues)
                val zipPredictedAndActualValuesOfIndividualTree = (predictedValuesOfIndividualTree zip actualValues)
                
                val validZipPredictedAndActualValuesOfGlobalTree = zipPredictedAndActualValuesOfGlobalTree.filter{
                    case (predictedValue, actualValue) => {
                        if (predictedValue == "???") false
                        else true
                    }
                }
                
                validZipPredictedAndActualValuesOfGlobalTree.foreach(println)
                
                val validZipPredictedAndActualValuesOfIndividualTree = zipPredictedAndActualValuesOfIndividualTree.filter{
                    case (predictedValue, actualValue) => {
                        if (predictedValue == "???") false
                        else true
                    }
                }
                
                
                val validatedResultOfGlobalTree = validZipPredictedAndActualValuesOfGlobalTree.map {
                    case (predictedValue, actualValue) => {
                        if (math.round(predictedValue.toDouble) != actualValue)
                            0
                        else 1    
                    }
                }
                val validatedResultOfIndividualTree = validZipPredictedAndActualValuesOfIndividualTree.map {
                    case (predictedValue, actualValue) => {
                        if (math.round(predictedValue.toDouble) != actualValue)
                            0
                        else 1    
                    }
                }
                
                val numberTruePredictionOfGlobalTree = validatedResultOfGlobalTree.reduce((a,b) => a + b)
                val totalPredictionOfGlobalTree = validZipPredictedAndActualValuesOfGlobalTree.count
                
                val numberTruePredictionOfIndividualTree = validatedResultOfIndividualTree.reduce(_ + _)
                val totalPredictionOfIndividualTree = validatedResultOfIndividualTree.count
                
                
				  p.println (USERID + "," 
				           + numberTruePredictionOfGlobalTree + "," 
				           + totalPredictionOfGlobalTree + "," 
				           + numberTruePredictionOfGlobalTree/totalPredictionOfGlobalTree)
				  p.println (USERID + "," 
				           + numberTruePredictionOfIndividualTree + "," 
				           + totalPredictionOfIndividualTree + "," 
				           + numberTruePredictionOfIndividualTree/totalPredictionOfIndividualTree)
				           
	            
                
                println (USERID + "," 
				           + numberTruePredictionOfGlobalTree + "," 
				           + totalPredictionOfGlobalTree + "," 
				           + numberTruePredictionOfGlobalTree/totalPredictionOfGlobalTree)
				println (USERID + "," 
				           + numberTruePredictionOfIndividualTree + "," 
				           + totalPredictionOfIndividualTree + "," 
				           + numberTruePredictionOfIndividualTree/totalPredictionOfIndividualTree)
                
            } catch {
                case e: Throwable => {
                	println("Error when evaluating user " + USERID)
                }
            }
        }
        })
	}
}
