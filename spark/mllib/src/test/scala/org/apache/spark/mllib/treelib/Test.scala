//package main.scala

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import treelib._
import treelib.cart.RegressionTree
import treelib.evaluation.Evaluation
import treelib.core._
import treelib.utils._
import scala.collection.immutable._
import bigfoot.helpers._
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import java.io.File

class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Set[Int]])
    kryo.register(classOf[SplitPoint])
    kryo.register(classOf[Feature])
    kryo.register(classOf[FeatureSet])
    kryo.register(classOf[Array[String]])
    kryo.register(classOf[TreeBuilder])
    kryo.register(classOf[RegressionTree])
  }
}

object Test {
	def main(args : Array[String]) : Unit = {
	    
	    val parser : ArgumentParser = new ArgumentParser("sbt run")
	    parser.addArgument("mode", "operation what we want to do: train/prune/predict/evaluate", "train")
	    parser.addArgument("input", "input data file. It can be the input training data in mode 'train' " +
	            " or the input of orginal dataset in mode 'prune' " + 
	            " or the input of testing data in mode 'evaluate' " + 
	            " or the input observation in mode 'predict' )", "data/training-bodyfat.csv")
	    parser.addArgument("output", "output file", "/tmp/tree.model")
	    parser.addOption("-minsplit", "minsplit", "the minimum observations in each expanding node", "10")
	    parser.addOption("-cp", "complexity", "The maximum complexity (different between mode 'train' and 'prune')", "0.005")
	    parser.addOption("-depth", "maxdepth", "The maximum depth of the tree model", "63")
	    parser.addOption("-target", "target", "target feature name", "DEXfat")
	    parser.addOption("-targetindex", "testing-target", "The index of target feature in testing data (use for evaluation)", "2")
	    parser.addOption("-predictor", "predictor", "the name of predictors", "age;waistcirc;hipcirc;elbowbreadth;kneebreadth")
	    parser.addOption("-model", "model", "path of tree model", "")
	    //parser.addOption("-test", "testing-data", "Testing data URL", "data/training-bodyfat.csv")
	    parser.addOption("-spark-home", "spark-home", "spark home directory", "/opt/spark")
	    parser.addOption("-maxmem", "max-memory", "Maximum memory used by each worker", "2222m")
	    parser.addOption("-master", "master-node", "address of master node in spark-cluster", "local")
	    parser.addOption("-cv" , "cross-validation", "number of folds", "5")
	    
	    parser.parse(args)
	    
	    val mode 		= parser.get("mode")
	    val input 		= parser.get("input")
	    val output 		= parser.get("output")
	    val minsplit 	= parser.get("minsplit")
	    val complexity 	= parser.get("complexity")
	    val maxdepth 	= parser.get("maxdepth")
	    val master_node = parser.get("master-node")
	    val targetFName = parser.get("target")
	    val predictors 	= parser.get("predictor")
	    
	    var setOfPredictors = Set[Any]()
	    predictors.split(";").foreach(x => {
	        setOfPredictors = if (x.startsWith("(c)")) {
	            setOfPredictors + as.String(x.substring(3))
	        }else if (x.startsWith("(n)")){
	            setOfPredictors + as.Number(x.substring(3))
	        }
	        else {
	            setOfPredictors + x
	        }
	    })
	    
	    val conf = new SparkConf()
	        		.setMaster(master_node)
	        		.setAppName("demo regression tree")
	        		.setSparkHome(parser.get("spark-home"))
	        		.setJars(List("target/scala-2.10/rtree-example_2.10-1.0.jar"))
	        		.set("spark.executor.memory", parser.get("max-memory"))
	    val context = new SparkContext(conf)
	    var stime : Long = 0
	    
	    mode match {
	        case "train" => {
	            val trainingData = context.textFile(input, 1)
	            val tree = new RegressionTree()
	            tree.setDataset(trainingData)
	            tree.setMinSplit(minsplit.toInt)
	            tree.setMaximumComplexity(complexity.toDouble)
	            tree.setMaxDepth(maxdepth.toInt)
	            
	            stime = System.nanoTime()
	            println("predictor:" + setOfPredictors)
	            println(tree.buildTree(targetFName, setOfPredictors))
                println("\nBuild tree in %f second(s)".format((System.nanoTime() - stime)/1e9))
                tree.writeModelToFile(output)

            }
	        
	        
            case "prune" => {
                val treeFromFile = new RegressionTree()
                val trainingData = context.textFile(input, 1)
                val modelPath = parser.get("model")
                val foldTimes = parser.get("cross-validation").toInt
    
                if (modelPath == ""){
                    println("Missing path of tree model")
                    exit
                }
                
                try {
                    treeFromFile.loadModelFromFile(modelPath)
                    println("OK: Load tree from '%s' successfully".format(modelPath))
                    println("Ogirinal tree\n%s".format(treeFromFile.treeModel))
                    println("\n\nFinal tree:\n%s".format(Pruning.Prune(treeFromFile.treeModel, 0.01, trainingData, foldTimes)))
                    treeFromFile.writeModelToFile(output)
                } catch {
                    case e: Throwable => {
                        println("ERROR: Couldn't load tree from '%s'".format(modelPath))
                        e.printStackTrace()
                    }
                }
            }
            
            
	        case "evaluate" => {
	            val treeFromFile = new RegressionTree()
                val testingdata = context.textFile(input, 1)
                val modelPath = parser.get("model")
                val targetIndex = parser.get("testing-target").toInt
                if (modelPath == ""){
                    println("Missing path of tree model")
                    exit
                }
	            try {
                    treeFromFile.loadModelFromFile(modelPath)
                    println("OK: Load tree from '%s' successfully".format(modelPath))
                    treeFromFile.writeModelToFile(output)
                } catch {
                    case e: Throwable => {
                        println("ERROR: Couldn't load tree from '%s'".format(modelPath))
                        e.printStackTrace()
                    }
                }
	            println("Evaluation:")
	            val predictRDD = treeFromFile.predict(testingdata)
	            val actualValueRDD = testingdata.map(line => line.split(',')(targetIndex))	// 14 is the index of ArrDelay in csv file, based 0
	            
	            println("Tree:\n%s".format(treeFromFile.treeModel))
	            println("Evaluation of the tree:")
	            var eResult = Evaluation.evaluate(predictRDD, actualValueRDD)
	            Utility.printToFile(new File("example.txt"))(p => {
				  p.println (eResult)
	            })
	        }
	        
	        case "predict" => {
	            val treeFromFile = new RegressionTree()
	            val modelPath = parser.get("model")
	            try {
                    treeFromFile.loadModelFromFile(modelPath)
                    println("OK: Load tree from '%s' successfully".format(modelPath))
                    treeFromFile.writeModelToFile(output)
                } catch {
                    case e: Throwable => {
                        println("ERROR: Couldn't load tree from '%s'".format(modelPath))
                        e.printStackTrace()
                    }
                }
                println("predict result:" + treeFromFile.predictOneInstance(input.split(",")))
	        }
	        
	        case _ => { println("invalid mode"); println(parser.usage); exit }
	    }     
            
            /* TEST RECOVER MODE */
		    //val recoverTree = new RegressionTree()
		    //recoverTree.treeBuilder = new DataMarkerTreeBuilder(new FeatureSet(Array[String]()))
		    //recoverTree.continueFromIncompleteModel(bodyfat_data, "/tmp/model.temp3")	// temporary model file
		    //println("Model after re-run from the last state:\n" + recoverTree.treeModel)
            
	}
}