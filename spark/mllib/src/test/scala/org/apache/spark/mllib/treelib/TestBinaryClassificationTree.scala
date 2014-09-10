import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import treelib._
import treelib.core._
import treelib.evaluation.Evaluation
import treelib.cart.RegressionTree
import treelib.cart.ClassificationTree
import treelib.utils._

object TestBinaryClassificationTree {
	def main(args : Array[String]) : Unit = {
	    val IS_LOCAL = true

        //System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        //System.setProperty("spark.kryo.registrator", "MyRegistrator")

        val inputTrainingFile = (
            if (IS_LOCAL)
                "data/playgolf.csv"
            else
                "hdfs://spark-master-001:8020/user/ubuntu/input/AIRLINES/training/*")

        val inputTestingFile = (
            if (IS_LOCAL)
                "data/playgolf.csv"
            else
                "hdfs://spark-master-001:8020/user/ubuntu/input/AIRLINES/testing/*")

        val conf = (
            if (IS_LOCAL)
                new SparkConf()
                .setMaster("local").setAppName("test classification tree")
            else
                new SparkConf()
                    .setMaster("spark://spark-master-001:7077")
                    .setAppName("rtree example")
                    .setSparkHome("/opt/spark")
                    .setJars(List("target/scala-2.10/rtree-example_2.10-1.0.jar"))
                    .set("spark.executor.memory", "2222m"))

        val context = new SparkContext(conf)

        var stime: Long = 0

        val trainingData = context.textFile(inputTrainingFile, 1)
        val testingData = context.textFile(inputTestingFile, 1)

        val pathOfTreeModel = "/tmp/tree.model"
        val pathOfTheeFullTree = "/tmp/full-tree.model"
            
        //val tree = new ClassificationTree()
	    //tree.setDataset(trainingData, true)
        val tree = new ClassificationTree()
	    tree.setDataset(trainingData)
	    tree.setMinSplit(1)
	    //tree.setFeatureNames(Array[String]("Temperature","Outlook","Humidity","Windy","PlayGolf"))
	    //println("Tree:\n" + tree.buildTree("PlayGolf", Set()))
	    println("Tree:\n" + tree.buildTree())
	    
	    /* TEST WRITING TREE TO MODEL */
            tree.writeModelToFile(pathOfTheeFullTree)

            /* TEST PRUNING */
            println("Final tree:\n%s".format(Pruning.Prune(tree.treeModel, 0.01, trainingData)))

            /* TEST LOADING TREE FROM MODEL FILE */
            val treeFromFile = new RegressionTree()
            try {
                treeFromFile.loadModelFromFile(pathOfTheeFullTree)
                println("OK: Load tree from '%s' successfully".format(pathOfTreeModel))
            } catch {
                case e: Throwable => {
                    println("ERROR: Couldn't load tree from '%s'".format(pathOfTreeModel))
                    e.printStackTrace()
                }
            }

        println("Evaluation:")
        val predictRDDOfTheFullTree = treeFromFile.predict(testingData)
        val predictRDDOfThePrunedTree = tree.predict(testingData)
        val actualValueRDD = testingData.map(line => line.split(',').last) // 2 is the index of DEXfat in csv file, based 0
        //println("Original tree(full tree):\n%s".format(treeFromFile.treeModel))

        println("Original Tree:\n%s".format(treeFromFile.treeModel))
        println("Evaluation of the full tree:")
        Evaluation("misclassification").evaluate(predictRDDOfTheFullTree, actualValueRDD)

        println("Pruned Tree:\n%s".format(tree.treeModel))
        println("Evaluation of the pruned tree:")
        Evaluation("misclassification").evaluate(predictRDDOfThePrunedTree, actualValueRDD)
    }
}