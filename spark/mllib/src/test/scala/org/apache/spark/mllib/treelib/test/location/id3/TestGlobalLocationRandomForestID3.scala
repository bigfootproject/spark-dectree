package test.location.id3

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import treelib._
import treelib.core._
import treelib.evaluation.Evaluation
import treelib.id3.ID3TreeBuilder

object TestGlobalLocationRandomForestID3 {
    def main(args: Array[String]): Unit = {
        val TIME_INTERVAL_LENGTH = 60

        val conf = new SparkConf()
            .setMaster("local")
            .setAppName("test location service")

        val context = new SparkContext(conf)
        var inputTrainingPath = "/Users/loveallufev/Documents/MATLAB/data/newdata/datafortree/pair/data";
        val inputTestingPath = "/Users/loveallufev/Documents/MATLAB/data/newdata/datafortree/testingdata/data"
        val model_path = "/Users/loveallufev/Documents/MATLAB/output/globaltreeid3tree";

        val rawTrainingData = context.textFile(inputTrainingPath, 1)
        val rawTestingData = context.textFile(inputTestingPath, 1)

        var outputstring = "userid,trueprediction,total";

        var randomForest = new RandomForestBuilder()

        var startTime: Long = 0
        var endTime: Long = 0

        try {
            //globalTree.loadModelFromFile(model_path)
            //println("OK: Load tree from '%s' successfully".format(model_path))
            //println("tree:\n" + globalTree.treeModel.tree)

            val trainingData = rawTrainingData.map {
                line =>
                    {
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
                        "%s,%s,%s,%s,%s,%s,%s,%s,%s".format(userid, year, month, dayOfMonth, dayOfWeek, currentTimeInterval, currentLocation, nextTimeInterval, nextLocation)
                        //"%s,%s,%s,%s,%s".format(dayOfWeek,currentTimeInterval, currentLocation, nextTimeInterval,nextLocation)
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
                        val values = line.split('|')
                        //println(values.mkString("|"))
                        val userid = values(0)
                        val year = values(1)
                        val month = values(2)
                        val day = values(3)
                        val dayOfWeek = values(4)
                        val time_locationStrings = values(5).split('!')
                        val num = time_locationStrings.length
                        val currentTime_LocationString = time_locationStrings(num - 2).split(',')
                        val nextTime_LocationString = time_locationStrings(num - 1).split(',')
                        val (currentTimeIndex, currentLocation, nextTimeIndex, nextLocation) =
                            (currentTime_LocationString(0).toInt / TIME_INTERVAL_LENGTH, currentTime_LocationString(1),
                                nextTime_LocationString(0).toInt / TIME_INTERVAL_LENGTH, nextTime_LocationString(1))
                        "%s,%s,%s,%s,%s,%s,%s,%s,%s".format(userid, year, month, day, dayOfWeek,
                            currentTimeIndex, currentLocation, nextTimeIndex, nextLocation)
                    }
            } //.filter(x => x._1 == i.toString) // filter to get data of a specific user
            )

            randomForest.setData(trainingData)
            randomForest.setFeatureName(
                Array[String]("UserId", "Year", "Month", "DayOfMonth", "DayOfWeek", "CurrentTimeIndex", "CurrentLocation", "NextTimeIndex", "NextLocation"))
            randomForest.setNumberOfTree(50)

            startTime = System.nanoTime()
            val forest = randomForest.buildForest[ID3TreeBuilder]("NextLocation",
                Set[Any](as.String("DayOfWeek"), as.String("CurrentLocation"), as.Number("NextTimeIndex")))
                forest.writeToFile("/tmp/randomforest")
                
            endTime = System.nanoTime()
            //println("Forest:" + forest)
            outputstring = "%s\n%s".format(outputstring,
                "build tree in:" + (endTime - startTime) / 1E9)

            startTime = System.nanoTime()
            val predictedValues = forest.predict(testingData)
            val actualValues = testingData.map(x => {
                x.split(',').last
            })

            val (truePrediction, total) = Evaluation("misclassification").evaluate(predictedValues, actualValues)
            outputstring = "%s\n%s,%s".format(outputstring, truePrediction, total)

        } catch {
            case e: Throwable => {
                println("ERROR when loading and evaluation of random forest")
                e.printStackTrace()
            }
        }
        endTime = System.nanoTime()
        outputstring = "%s\n%s".format(outputstring,
            "testing in:" + (endTime - startTime) / 1E9)
        println(outputstring)

    }
}