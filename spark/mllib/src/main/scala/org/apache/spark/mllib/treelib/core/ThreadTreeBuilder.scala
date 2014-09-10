package treelib.core

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.collection.immutable.Queue
import scala.concurrent._


/**
 * This class will put each job into a separated thread.
 * Each job will try to find the best feature and the best split point of this feature
 * After that, these result will be used to construct a node in the tree
 *
 * @param featuresSet	the information of the features in dataset
 */
abstract class ThreadTreeBuilder(featuresSet: FeatureSet, usefulFeatureSet : FeatureSet)
    extends TreeBuilder() {

    /**
     *  queue of waiting jobs
     */
    var expandingJobs: Queue[JobInfo] = Queue[JobInfo]();

    /**
     * queue of finished jobs
     */
    var finishedJobs: Queue[JobInfo] = Queue[JobInfo]();

    /**
     *  queue of error jobs
     */
    var errorJobs: Queue[JobInfo] = Queue[JobInfo]();

    /**
     *  the number of currently running jobs
     *  A job is consider a running job from the time it is launched
     *  until it is PICK UP from finishedQueue
     */
    var numberOfRunningJobs = 0
    
    /**
     * Is model updated ?
     */
    var isModelChanged = false
    

    /**
     * Update tree model based on the result of a finished job
     *
     * @param finishJob a job has already finished. (can be an error job)
     */
        /*
    private def updateModel(finishJob: JobInfo) {

        println("Update model with finished job:" + finishJob)

        // if this job failed, add it to errorQueue and ignore it
        if (!finishJob.isSuccess) {
            errorJobs = errorJobs :+ finishJob
            println("ERROR: Node id=" + finishJob.ID + " failed\n" + finishJob.errorMessage)
            return
        }

        // if this job succeeded
        val newnode = (
            if (finishJob.isStopNode) { // leaf node
                new CARTLeafNode(finishJob.splitPoint.point.toString)
            } else { // the intermediate node
                val chosenFeatureInfoCandidate = featureSet.data.find(f => f.index == finishJob.splitPoint.index)

                chosenFeatureInfoCandidate match {
                    case Some(chosenFeatureInfo) => {
                        new CARTNonLeafNode(chosenFeatureInfo,
                            finishJob.splitPoint,
                            new CARTLeafNode("empty.left"),
                            new CARTLeafNode("empty.right"));
                    }
                    case None => { new CARTLeafNode(this.ERROR_SPLITPOINT_VALUE) }
                }
            }) // end of assign value for newnode

        if (newnode.value == this.ERROR_SPLITPOINT_VALUE) {
            println("Valud of job id=" + finishJob.ID + " is invalid")
            return
        }

        // If tree has zero node, create a root node
        if (treeModel.tree.isLeaf) {
            treeModel.tree = newnode;

        } else //  add new node to current model
        {

            val level = (Math.log(finishJob.ID.toDouble) / Math.log(2)).toInt
            var i: Int = level - 1
            var parent = treeModel.tree; // start adding from root node
            while (i > 0) {

                if ((finishJob.ID / (2 << i - 1)) % 2 == 0) {
                    // go to the left
                    parent = parent.asInstanceOf[CARTNode].left
                } else {
                    // go go the right
                    parent = parent.asInstanceOf[CARTNode].right
                }
                i -= 1
            } // end while

            if (finishJob.ID % 2 == 0) {
                parent.asInstanceOf[CARTNode].setLeft(newnode)
            } else {
                parent.asInstanceOf[CARTNode].setRight(newnode)
            }
        }
    }

    /**
     * Put a job in a thread and launch it.
     *
     * @param job		the expanding job
     * @param inputdata	the whole input data of job which will be launch
     */
    private def launchJob(job: JobInfo, 
            //inputData : RDD[scala.collection.SeqView[FeatureValueAggregate, Array[FeatureValueAggregate]]]
            inputData: RDD[Array[FeatureValueAggregate]]
    ) {
        var thread = new Thread(new JobExecutor(job, inputData.asInstanceOf[RDD[Array[FeatureValueAggregate]]], this))
        thread.start()
    }

    /**
     * Put a job into waiting queue
     *
     * @param job	new job
     */
    def addJobToExpandingQueue(job: JobInfo) {
        this.synchronized {
            this.expandingJobs = this.expandingJobs :+ job
            println("Add job id=" + job.ID + " into expanding queue")

        }
    }

    /**
     * Put a job into finished queue, which contains the jobs have finished already
     * and are waiting to be pick up to update model
     */
    def addJobToFinishedQueue(job: JobInfo) {
        this.synchronized {
            this.finishedJobs = this.finishedJobs :+ job
            println("Add job id=" + job.ID + " into finished queue")

        }
    }

    
    def encapsulateValueIntoObject(index : Int, value : String, yValue : Double, featureType : FeatureType.Value) 
    : FeatureValueAggregate  = {
        featureType match {
            case FeatureType.Categorical => new FeatureValueAggregate(index, value, yValue, yValue*yValue, 1)
            case FeatureType.Numerical => new FeatureValueAggregate(index, value.toDouble, yValue, yValue*yValue, 1)
        }
    }
    
    def validateArrayString(d : Array[String]) : (Boolean, Array[String]) = {
            try{
                var i = -1
                d.map(
                    element => {
                        
                        i = (i + 1) % featureSet.numberOfFeature
                        featureSet.data(i).Type match {
                                        case FeatureType.Categorical => element
                                        case FeatureType.Numerical => element.toDouble
                                    }
                        element
                    
                        }
                    )
                    (true, d)
            }
            catch {
                case _ : Throwable => (false, d)
            }
        }
    
    /**
     * Building tree, bases on:
     *
     * @parm yFeature 	predicted feature
     * @param xFeature	input features
     *
     * @return: <code>TreeModel</code> : root of the tree
     */
    override def startBuildTree(trainingData : RDD[String]) : Unit = {
     
        // parse raw data
        val mydata = trainingData.map(line => line.split(delimiter))
 
        /* REGION CLEANING */ 
        var checkedData = mydata.map( array => {
            validateArrayString(array)
        } )
           
        var cleanedData = checkedData.filter(x => x._1).map(x => x._2)
        
        /* END OF REGION CLEANING */ 
        
        /* REGION TRANSFORMING */
        
        // encapsulate each value of each feature in each line into a object
        var transformedData = cleanedData.map(
            arrayValues => {
              convertArrayValuesToObjects(arrayValues)
            })

        // filter the 'line' which contains the invalid or missing data
        transformedData = transformedData.filter(x => (x.length > 0))
        
        /* END OF REGION TRANSFORMING */
        
        
        // if we build a completely new tree, the expandingJobs is empty
        // otherwise, if we try to re-build an incomplete tree, the expandingJobs is not empty
        if (expandingJobs.isEmpty){
	        // create a first job, which will be expand the root node
	        val firstJob = new JobInfo(1, List[Condition]())
	
	        // add the first job into waiting queue
	        this.addJobToExpandingQueue(firstJob)	
        }

        // set number of running job is zero. 
        numberOfRunningJobs = 0

        /* END OF INIT */

        /**
         * Can the building tree algorithm finish ?
         */
        def finish() = {
            (this.numberOfRunningJobs == 0)
        }

        /* START ALGORITHMS */
        do {

            isModelChanged = false
            
            //get jobs from finishedJobs and update tree model
            this.synchronized {
                while (!finishedJobs.isEmpty) {
                    finishedJobs.dequeue match {
                        case (j, xs) => {
                            this.numberOfRunningJobs = this.numberOfRunningJobs - 1
                            println("Dequeue finished jobs id=" + j.ID + ". number running Jobs = " + this.numberOfRunningJobs.toString)
                            updateModel(j)
                            finishedJobs = xs
                            this.isModelChanged = true
                            this.treeModel.writeToFile(this.temporaryModelFile)
                        }
                    }
                }
            }

            //get jobs from expandingJobs and launch them
            this.synchronized {
                while (!expandingJobs.isEmpty && numberOfRunningJobs < MAXIMUM_PARALLEL_JOBS) {
                    expandingJobs.dequeue match {
                        case (j, xs) => {
                            println("Dequeue expanding jobs id=" + j.ID)
                            this.numberOfRunningJobs = this.numberOfRunningJobs + 1
                            expandingJobs = xs
                            println("Launch job id=" + j.ID + " number running Jobs=" + this.numberOfRunningJobs.toString)
                            launchJob(j, transformedData)
                        }
                    }
                }
            }
            
            //if (this.isModelChanged)
            //    this.treeModel.writeToFile(this.temporaryModelFile)

            //DelayedFuture( 5000L )(println("iter"))
        } while (!finish())

        /* END OF ALGORITHM */

        /* FINALIZE THE ALGORITHM */
        if (errorJobs.isEmpty){
            this.treeModel.isComplete = true
            println("\n------------------DONE WITHOUT ERROR------------------\n")
        }
        else{
            this.treeModel.isComplete = false
            println("\n--------FINISH with some failed jobs:----------\n" + errorJobs.toString + "\n")
            println("Temporaty Tree model is stored at " + this.temporaryModelFile + "\n")
        }
        
    }
    
     /**
     * Recover, repair and continue build tree from the last state
     * 
     * @throw Exception if the tree is never built before
     */
    override def continueFromIncompleteModel(trainingData: RDD[String]) = {
        if (treeModel == null){
            throw new Exception("The tree model is empty because of no building. Please build it first")
        }
        
        if (treeModel.isComplete){
            println("This model is already complete")
        }
        else {
            println("Recover from the last state")        
        
	        /* INITIALIZE */
	        this.featureSet = treeModel.fullFeatureSet
	        this.xIndexes = treeModel.xIndexes
	        this.yIndex = treeModel.yIndex
	        
	        expandingJobs = Queue[JobInfo]();
	        finishedJobs = Queue[JobInfo]();
	        errorJobs = Queue[JobInfo]();
	        numberOfRunningJobs = 0
	        
	        /* END OF INIT */
	        
	        // generate job from leaf nodes
	        generateJobFromLeafNodes()
	        
	        // start build tree
	        startBuildTree(trainingData)
        }
    }
    
    /**
     * Generate job from the leaf node
     */
    private def generateJobFromLeafNodes() = {
        
        var jobList = List[JobInfo]()
        
        def generateJobIter(currentNode : CARTNode, id: BigInt, conditions : List[Condition]) :Unit = {

            if (currentNode.isLeaf && 
                    (currentNode.value == "empty.left" || currentNode.value == "empty.right")){
	                var job = new JobInfo(id, conditions)
	                jobList = jobList :+ job
	            }
            
            if (!currentNode.isLeaf){	// it has 2 children
                    var newConditionsLeft = conditions :+ 
                    		new Condition(new SplitPoint(currentNode.feature.index, currentNode.splitpoint, 0), true)
                    generateJobIter(currentNode.left, id*2, newConditionsLeft)

                    var newConditionsRight = conditions :+ 
                    		new Condition(new SplitPoint(currentNode.feature.index, currentNode.splitpoint, 0), false)
                    generateJobIter(currentNode.right, id*2 + 1, newConditionsRight)
            }    
        }
        
        generateJobIter(treeModel.tree.asInstanceOf[CARTNode], 1, List[Condition]())
        
        jobList.sortBy(j => j.ID)	// sort jobs by ID because we want to do job which near root node first
        
        // add each job into expanding queue
        jobList.foreach(job => this.addJobToExpandingQueue(job))
        
    }
    
    override def createNewInstance(featureSet : FeatureSet, usefulFeatureSet : FeatureSet) = new ThreadTreeBuilder(featureSet, usefulFeatureSet)
  
  private def convertArrayValuesToObjects(arrayValues: Array[String]): Array[rtreelib.core.FeatureValueAggregate] = {
      var yValue = arrayValues(yIndex).toDouble
      var i = -1
                //Utility.parseDouble(arrayValues(yIndex)) match {
                //    case Some(yValue) => { // check type of Y : if isn't continuous type, return nothing
      arrayValues.map {
          element =>
              {   
                  i = (i + 1) % featureSet.numberOfFeature
                  if (!this.xIndexes.contains(i)){
                      var f = encapsulateValueIntoObject(-i -1, "0", 0, FeatureType.Numerical)
                      f.frequency = -1
                      f
                  }else
                  featureSet.data(i).Type match {
                      case FeatureType.Categorical => encapsulateValueIntoObject(i, element, yValue, FeatureType.Categorical)
                      case FeatureType.Numerical => encapsulateValueIntoObject(i, element, yValue, FeatureType.Numerical)
                  }
              }
      }
    }
    * 
    */
}
