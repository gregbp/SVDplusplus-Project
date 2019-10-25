import java.io._
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext, graphx}
import scala.collection.mutable.ArrayBuffer
import java.io.File
import org.apache.log4j.{Level, Logger}
import java.io.FileInputStream
import java.util.Properties
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap
import scala.util.Random


object MyProj_2_2_0 {

  def main(args: Array[String]): Unit = {

    // Spark parameters
    val confSpark = new SparkConf()
    confSpark.setAppName("MyProj_2_2_0")


    // Working directory & paths for input and output files
    val workingDir = System.getProperty("user.dir")
    var path_for_input = ""
    var path_for_output = ""


    // Reading parameters from the properties file
    val props = new Properties()
    val props_path = new FileInputStream(workingDir.concat("/config.properties"))
    props.load(props_path)

    var hw_architecture = props.getProperty("hw_architecture")  // 3 hardware architectures -> localhost, singlenode, cluster
    var first_sample = props.getProperty("first_sample").toInt  // the sample that is used first
    var last_sample = props.getProperty("last_sample").toInt  // the sample that is used last
    var folds = props.getProperty("folds").toInt  // number of folds in 10-fold cross validation - if we choose a number less than 10,
    // then the 10 fold CV will be executed only for the times indicated by this number and will end uncompleted
    var input_size = props.getProperty("input_size")  // sizes of input samples
    var dataset = props.getProperty("dataset")  // dataset name (in this case is epinions)
    var kFactors = props.getProperty("latent_factors").toInt  // number of latent factors in SVD++
    var k_infl = props.getProperty("k_influencers").toInt // number of 'influencers'
    var first_mode = props.getProperty("first_mode").toInt // the mode that is used first
    var last_mode = props.getProperty("last_mode").toInt  // the mode that is used last
    var create_subsamples = props.getProperty("create_subsamples").toInt  // defines if new subsamples will be created (1) or not (0)
    var charactInput = props.getProperty("charactInput")  // characterization for input file
    var charactOutput = props.getProperty("charactOutput")  // characterization for output file
    var selection = props.getProperty("selection").toInt  // selection or not - replicating only the ratings for items that already exist in each subsample (1) or not (0)
    var top_users = props.getProperty("top_users").toInt  // number of the users who rated the least rated items -
    // if we choose 6, then we'll take the user who has rated the most of the least rated items, the 2nd "top user" who rated these items, the 3rd "top user"... until the 6th "top user"
    var top_least_rating = props.getProperty("top_least_rating").toInt  //  it’s used as limit in function "findLeastRatedItems"
    // it defines the rating range for the LRI (least rated items), starting from 1 rating until this limit - [1,top_least_rating]

    // Converting the String property "input_size" to Int
    var strn = new Array[String](12)
    strn = input_size.split(", ")
    var inputSize = new Array[Int](12)
    for (i <- 0 to 11) {
      inputSize(i) = strn(i).toInt
    }

    // SVD++ parameters
    var conf = new lib.SVDPlusPlus.Conf(kFactors, 10, 1, 5, 0.007, 0.007, 0.005, 0.015)


    // Defining input & output paths depending on hardware architecture
    println("\nworkingDir = " + workingDir + "\n")
    if (hw_architecture == "localhost") {
      confSpark.setMaster("local[*]")
      path_for_input = workingDir.concat("/input")
      path_for_output = workingDir.concat("/output")
    } else if (hw_architecture == "cluster") {
      path_for_input = workingDir.concat("/gballas/input") // single node & cluster
      path_for_output = workingDir.concat("/gballas/output") // single node & cluster
    } else if (hw_architecture == "desktop") {
      path_for_input = workingDir.concat("/sparkProject/input") // single node & cluster
      path_for_output = workingDir.concat("/sparkProject/output") // single node & cluster
    }


    // Spark parameters
    val sc = new SparkContext(confSpark)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)


    // Info about the heap size
    val heapSize = Runtime.getRuntime.totalMemory / (1024 * 1024)
    val heapMaxSize = Runtime.getRuntime.maxMemory / (1024 * 1024)
    val heapFreeSize = Runtime.getRuntime.freeMemory / (1024 * 1024)
    println("heapSize = " + heapSize + " heapMaxSize = " + heapMaxSize + " heapFreeSize = " + heapFreeSize)


    // Array that stores useful info about plots
    var plotFormat = new Array[String](5)




    // Beginning of the execution
    var startTimeTOTAL = System.currentTimeMillis()
    for (smpl <- first_sample to last_sample) {
      print("\nCURRENT SAMPLE -> "+smpl+"\n\n")

      //    DURATIONS
      // For every fold there are 3 stages: train & test set creation, model training and evaluation.
      // Duration of each stage is calculated and also the total duration of the fold (sum of 3 stages)

      // Times for the case that there is not an initial split of the input sample
      var createTRAINandTESTtime = new Array[Long](10)
      var ModelTrainingTime = new Array[Long](10)
      var EvalauationTime = new Array[Long](10)
      var foldDuration = new Array[Long](10)


      // Times for the case that there is an initial split of the input sample and creation of 2 subsamples
      var createTRAINandTESTtime1_2: Array[Array[Long]] = new Array[Array[Long]](2)
      createTRAINandTESTtime1_2(0) = new Array[Long](10)
      createTRAINandTESTtime1_2(1) = new Array[Long](10)

      var ModelTrainingTime1_2: Array[Array[Long]] = new Array[Array[Long]](2)
      ModelTrainingTime1_2(0) = new Array[Long](10)
      ModelTrainingTime1_2(1) = new Array[Long](10)

      var EvalauationTime1_2: Array[Array[Long]] = new Array[Array[Long]](2)
      EvalauationTime1_2(0) = new Array[Long](10)
      EvalauationTime1_2(1) = new Array[Long](10)

      var foldDuration1_2: Array[Array[Long]] = new Array[Array[Long]](2)
      foldDuration1_2(0) = new Array[Long](10)
      foldDuration1_2(1) = new Array[Long](10)

      // Duration of sample splitting, influencer searching and copy of the influencer's ratings
      var splitTime = 0.0
      var findInfluencerTime = 0.0
      var copyInfluencersRatsTime = 0.0



      //    RMSEs, sparsity and exceptions

      // RMSEs
      var RMSE = 0.0  //RMSE of every fold
      var avgRMSE = 0.0  // Average RMSE when there in not input splitting
      var avgRMSE1 = 0.0  // Average RMSE for subsample1
      var avgRMSE2 = 0.0  // Average RMSE for subsample2
      var avgRMSEtotal = 0.0  // Sum of avgRMSE1 & avgRMSE2

      // Sparsity of the input sample
      var sparsity0 = 0.0

      // Exceptions
      // Info about exceptions for each fold
      var exceps_table: Array[Array[Double]] = new Array[Array[Double]](3)
      exceps_table(0) = new Array[Double](10) // Exception percentage
      exceps_table(1) = new Array[Double](10) // Number of exceptions
      exceps_table(2) = new Array[Double](10) // Number of exceptions

      var exceps_avg = 0.0  // Average number of exceptions



      //        INPUT

      // INPUT Part 1 - Input sample is NOT splitted
      // From the input txt file to RDD
      var final_path_for_input = path_for_input + "\\final\\epinions_random_sample" + inputSize(smpl) + "" + charactInput + ".txt"
      var inputTOTAL = sc.textFile(final_path_for_input)  // creation of RDD
      inputTOTAL = inputTOTAL.repartition(8)  //

      // Sample size
      var totalSize = inputTOTAL.count.toInt
      println(smpl + ") SAMPLE SIZE = " + totalSize)

      // Keeping only UserID, ItemID & rating
      var edgesTOTAL = inputTOTAL.map { input =>
        val fields = input.split(" ")
        Edge(fields(0).toLong * 2, fields(1).toLong * 2 + 1, fields(2).toDouble)
      }


      // From RDD to "easy to use" format - Creation of ArrayBuffers
      // One ArrayBuffer per field (UserID, ItemID & rating)
      var usersTOTAL0 = ArrayBuffer[org.apache.spark.graphx.VertexId]()
      var itemsTOTAL0 = ArrayBuffer[org.apache.spark.graphx.VertexId]()
      var ratingsTOTAL0 = ArrayBuffer[Double]()

      edgesTOTAL.map(x => (x.srcId)).collect.copyToBuffer(usersTOTAL0)
      edgesTOTAL.map(x => (x.dstId)).collect.copyToBuffer(itemsTOTAL0)
      edgesTOTAL.map(x => (x.attr)).collect.copyToBuffer(ratingsTOTAL0)

      // Distinct users & distinct items - Sparsity
      var dist_users0 = usersTOTAL0.distinct.size
      var dist_items0 = itemsTOTAL0.distinct.size
      sparsity0 = inputSize(smpl).toDouble / ((dist_users0 - 1) * (dist_items0 - 1)).toDouble
      println("\n\n\t\tSPARSITIES\n")
      println("distinct users (TOTAL) = " + dist_users0)
      println("distinct items (TOTAL) = " + dist_items0)



      // INPUT Part 2 - Input sample is splitted to 2 subsamples
      // The directory where subsamples are stored

      var path_for_output_subgr = path_for_output + "/subgraph"

      // If the "create_subsamples" parameter is 1, then create 2 subsamples
      if (create_subsamples == 1) {

        // Beginning of splitting procedure
        var startSplitTime = System.currentTimeMillis()
        println("input path: "+final_path_for_input)

        // Splitting in the half of previously defined ArrayBuffers
        var (usersTOTAL1_2, itemsTOTAL1_2, ratingsTOTAL1_2) = splitInput(usersTOTAL0, itemsTOTAL0, ratingsTOTAL0, totalSize)

        // Saving the ArrayBuffers of the two subsamples as two txt files
        for (sg <- 0 to 1) {
          path_for_output_subgr = path_for_output_subgr + (sg + 1) + "_" + totalSize + charactInput +".txt"
          println("path_for_output_subgr: "+path_for_output_subgr)

          var pw_graphs = new PrintWriter(new File(path_for_output_subgr))
          for (i <- 0 to usersTOTAL1_2(sg).size - 1) {
            pw_graphs.println(usersTOTAL1_2(sg)(i) + " " + itemsTOTAL1_2(sg)(i) + " " + ratingsTOTAL1_2(sg)(i))
          }

          path_for_output_subgr = path_for_output + "/subgraph"
          pw_graphs.close()
        }

        // End of splitting procedure
        splitTime = System.currentTimeMillis() - startSplitTime
      }


      // Paths for the created subsamples
      var path_for_subgraph1 = ""
      var path_for_subgraph2 = ""

      // ArrayBuffers that store the 2 subsamples
      var usersTOTAL1_2 = new Array[ArrayBuffer[org.apache.spark.graphx.VertexId]](2)
      usersTOTAL1_2(0) = new ArrayBuffer[org.apache.spark.graphx.VertexId]()
      usersTOTAL1_2(1) = new ArrayBuffer[org.apache.spark.graphx.VertexId]()
      var itemsTOTAL1_2 = new Array[ArrayBuffer[org.apache.spark.graphx.VertexId]](2)
      itemsTOTAL1_2(0) = new ArrayBuffer[org.apache.spark.graphx.VertexId]()
      itemsTOTAL1_2(1) = new ArrayBuffer[org.apache.spark.graphx.VertexId]()
      var ratingsTOTAL1_2 = new Array[ArrayBuffer[Double]](2)
      ratingsTOTAL1_2(0) = new ArrayBuffer[Double]()
      ratingsTOTAL1_2(1) = new ArrayBuffer[Double]()

      // List with modes - used to decide if odes 2, 4, 5, 6 or 7 will be executed
      var modeList = ArrayBuffer[Int]()
      for (mode <- first_mode to last_mode) {
        modeList += mode
      }
      println("modeList -> ",modeList)

      // Modes 2, 4, 5, 6 & 7 use subsamples
      if (modeList.contains(2) || modeList.contains(4) || modeList.contains(5) || modeList.contains(6) || modeList.contains(7)) {

        // Input from subsample 1 - from txt file to RDD
        path_for_subgraph1 = "C:\\Users\\grigo\\IdeaProjects\\MyProj_2_2_0\\input\\subgraphs\\subgraph1_" + totalSize + charactInput + ".txt"
        println("\n\npath_for_subgraph1 -> " + path_for_subgraph1)
        var input_sub1 = sc.textFile(path_for_subgraph1)

        // Keeping only UserID, ItemID & rating
        var edges_sub1 = input_sub1.map { input =>
          val fields = input.split(" ")
          Edge(fields(0).toLong * 2, fields(1).toLong * 2 + 1, fields(2).toDouble)
        }

        // From RDD to "easy to use" format - Creation of ArrayBuffers
        // One ArrayBuffer per field (UserID, ItemID & rating)
        edges_sub1.map(x => (x.srcId)).collect.copyToBuffer(usersTOTAL1_2(0))
        edges_sub1.map(x => (x.dstId)).collect.copyToBuffer(itemsTOTAL1_2(0))
        edges_sub1.map(x => (x.attr)).collect.copyToBuffer(ratingsTOTAL1_2(0))


        // Input from subsample 2 - from txt file to RDD
        path_for_subgraph2 = "C:\\Users\\grigo\\IdeaProjects\\MyProj_2_2_0\\input\\subgraphs\\subgraph2_" + totalSize + charactInput + ".txt"
        var input_sub2 = sc.textFile(path_for_subgraph2)
        println("path_for_subgraph2 -> " + path_for_subgraph2 + "\n\n")
        var edges_sub2 = input_sub2.map { input =>
          val fields = input.split(" ")
          Edge(fields(0).toLong * 2, fields(1).toLong * 2 + 1, fields(2).toDouble)
        }

        // Keeping only UserID, ItemID & rating
        edges_sub2.map(x => (x.srcId)).collect.copyToBuffer(usersTOTAL1_2(1))
        edges_sub2.map(x => (x.dstId)).collect.copyToBuffer(itemsTOTAL1_2(1))
        edges_sub2.map(x => (x.attr)).collect.copyToBuffer(ratingsTOTAL1_2(1))

        println("sub1 size -> " + input_sub1.count())
        println("sub2 size -> " + input_sub2.count())

        // From RDD to "easy to use" format - Creation of ArrayBuffers
        // One ArrayBuffer per field (UserID, ItemID & rating)
        usersTOTAL0 = usersTOTAL1_2(0) ++ usersTOTAL1_2(1)
        itemsTOTAL0 = itemsTOTAL1_2(0) ++ itemsTOTAL1_2(1)
        ratingsTOTAL0 = ratingsTOTAL1_2(0) ++ ratingsTOTAL1_2(1)
      }




      // RESULT FILES
      for (mode <- first_mode to last_mode) {
        print("\nCURRENT mode -> "+mode+"\n\n")

        // Constructing the final output path depending on the chosen mode
        var final_path_for_output = path_for_output
        if (mode == 1 || mode == 0) {
          final_path_for_output = final_path_for_output + "/results_" + dataset + "_" + inputSize(smpl) + "_randomInput_" + kFactors + "factors" + charactOutput + ".txt"
        }
        if (mode == 2) {
          final_path_for_output = final_path_for_output + "/results_" + dataset + "_" + inputSize(smpl) + "_randomInput_" + kFactors + "factors_SPLITTED_Input" + charactOutput + ".txt"
        }
        if (mode == 3) {
          final_path_for_output = final_path_for_output + "/results_" + dataset + "_" + inputSize(smpl) + "_randomInput_" + kFactors + "factors_MYWAY_Input" + charactOutput + ".txt"
        }
        if (mode == 4 || mode == 5 || mode == 6) {
          if (selection == 0) {
            final_path_for_output = final_path_for_output + "/results_" + dataset + "_" + inputSize(smpl) + "_randomInput_" + kFactors + "factors_Influencers" + mode + "_" + k_infl + "influencers" + charactOutput + ".txt"
          } else {
            final_path_for_output = final_path_for_output + "/results_" + dataset + "_" + inputSize(smpl) + "_randomInput_" + kFactors + "factors_Influencers" + mode + "_" + k_infl + "influencers_selected" + charactOutput + ".txt"
          }
        }
        if(mode == 7){
          final_path_for_output = final_path_for_output + "/results_" + dataset + "_" + inputSize(smpl) + "_randomInput_" + kFactors + "factors_LeastRatedItems" + mode + "_" + top_users + "top_users" + charactOutput + ".txt"
        }
        println("final_path_for_output = " + final_path_for_output)

        // Creating results file
        var pw = new PrintWriter(new File(final_path_for_output))

        pw.println("\ninput path = " + final_path_for_input + "\n")
        println("\ninput path = " + final_path_for_input)

        // If the chosen mode uses subsamples, record it to the results file
        if (first_mode >= 2 && first_mode != 3) {
          pw.println("path for subgraph1 = " + path_for_subgraph1)
          pw.println("path for subgraph2 = " + path_for_subgraph2)
          pw.println("\n\n")

          println("path for subgraph1 = " + path_for_subgraph1)
          println("path for subgraph2 = " + path_for_subgraph2)
        }

        // Extra 2 result files; one for recording exceptions and one for recording useful info about plots
        var excep_path = path_for_output
        var plot_info_path = path_for_output

        // Constructing paths for exception & plot info files depending on the chosen mode
        if (mode == 1 || mode == 0) {
          excep_path = path_for_output + "/exceptions_" + dataset + "_" + inputSize(smpl) + "_randomInput_" + kFactors + "factors" + charactOutput + ".txt"
          plot_info_path = plot_info_path + "/plotInfo_" + dataset + "_" + inputSize(smpl) + "_randomInput_" + kFactors + "factors" + charactOutput + ".txt"
        }
        if (mode == 2) {
          excep_path = path_for_output + "/exceptions_" + dataset + "_" + inputSize(smpl) + "_randomInput_" + kFactors + "factors_SPLITTED_Input" + charactOutput + ".txt"
          plot_info_path = plot_info_path + "/plotInfo_" + dataset + "_" + inputSize(smpl) + "_randomInput_" + kFactors + "factors_SPLITTED_Input" + charactOutput + ".txt"
        }
        if (mode == 3) {
          excep_path = path_for_output + "/exceptions_" + dataset + "_" + inputSize(smpl) + "_randomInput_" + kFactors + "factors_MYWAY_Input" + charactOutput + ".txt"
          plot_info_path = plot_info_path + "/plotInfo_" + dataset + "_" + inputSize(smpl) + "_randomInput_" + kFactors + "factors_MYWAY_Input" + charactOutput + ".txt"
        }
        if (mode == 4 || mode == 5 || mode == 6  ) {
          if (selection == 0) {
            excep_path = path_for_output + "/exceptions_" + dataset + "_" + inputSize(smpl) + "_randomInput_" + kFactors + "factors_Influencers" + mode + "_" + k_infl + "influencers" + charactOutput + ".txt"
            plot_info_path = plot_info_path + "/plotInfo_" + dataset + "_" + inputSize(smpl) + "_randomInput_" + kFactors + "factors_Influencers" + mode + "_" + k_infl + "influencers" + charactOutput + ".txt"
          } else {
            excep_path = path_for_output + "/exceptions_" + dataset + "_" + inputSize(smpl) + "_randomInput_" + kFactors + "factors_Influencers" + mode + "_" + k_infl + "influencers_selected" + charactOutput + ".txt"
            plot_info_path = plot_info_path + "/plotInfo_" + dataset + "_" + inputSize(smpl) + "_randomInput_" + kFactors + "factors_Influencers" + mode + "_" + k_infl + "influencers_selected" + charactOutput + ".txt"
          }
        }
        if(mode == 7){
          excep_path = path_for_output + "/exceptions_" + dataset + "_" + inputSize(smpl) + "_randomInput_" + kFactors + "factors_LeastRatedItems" + mode + "_" + top_users + "top_users" + charactOutput + ".txt"
          plot_info_path = plot_info_path + "/plotInfo_" + dataset + "_" + inputSize(smpl) + "_randomInput_" + kFactors + "factors_LeastRatedItems" + mode + "_" + top_users + "top_users" + charactOutput + ".txt"
        }
        println("final_path_for_output = " + excep_path)

        // Creating exceptions file
        var pw_excep = new PrintWriter(new File(excep_path))
        pw_excep.println("\ninput path = " + final_path_for_input + "\n")
        if(mode == 2 || mode == 4 || mode == 5 || mode == 6 || mode == 7) { // specific info about modes 2, 4, 5, 6 & 7
          pw_excep.println("path for subgraph1 = " + path_for_subgraph1)
          pw_excep.println("path for subgraph2 = " + path_for_subgraph2)
          pw_excep.println("\n")
        }

        pw_excep.println("Mode = " + mode + "\n\n\n")

        // Creating plot info file
        var pw_plot = new PrintWriter(new File(plot_info_path))
        pw_plot.println("\ninput path = " + final_path_for_input + "\n")
        if(mode == 1 || mode == 4 || mode == 5 || mode == 6 || mode == 7) { // specific info about modes 2, 4, 5, 6 & 7
          pw_plot.println("path for subgraph1 = " + path_for_subgraph1)
          pw_plot.println("path for subgraph2 = " + path_for_subgraph2)
          pw_plot.println("\n")
        }
        if(mode == 7){  // specific info about mode 7
          pw.println("number of top users who rated item with only one rating "+top_users)
        }

        pw_plot.println("Mode = " + mode + "\n\n\n")

        pw.println("Number of elements: " + totalSize + "\n")
        pw.println("Number of factors: " + kFactors + "\n")
        pw.println("Sample sparsity (TOTAL): " + sparsity0)
        pw.println("distinct users (TOTAL) = " + dist_users0)
        pw.println("distinct items (TOTAL) = " + dist_items0 + "\n\n")

        pw_excep.println("\n\n\t\tSPARSITIES (TOTAL)\n")
        pw_excep.println("Sample sparsity: " + sparsity0)
        pw_excep.println("distinct users (TOTAL) = " + dist_users0)
        pw_excep.println("distinct items (TOTAL) = " + dist_items0)

        pw_plot.println("\n\n\t\tSPARSITIES (TOTAL)\n")
        pw_plot.println("Sample sparsity: " + sparsity0)
        pw_plot.println("distinct users (TOTAL) = " + dist_users0)
        pw_plot.println("distinct items (TOTAL) = " + dist_items0)

        // Preliminaries time - Input split, results files creation
        var preliminariesTime = System.currentTimeMillis() - startTimeTOTAL //for both ways
        pw.println("Preliminaries Time in mins: " + preliminariesTime / 1000f / 60f + " \tin ms: " + preliminariesTime + "\n")




        //  MAIN PROGRAM

        // mode = 1 (not splitted input) - Train & Test set creation, Model Training & Model Evaluation
        if (mode == 1) {

          // Initial first & last index of the test set values
          var first_idx = 0
          var last_idx = (inputSize(smpl) * 0.1 - 1).toInt

          pw.println("\n\n")

          // K-fold cross validation
          for (fld <- 0 until folds) {

            pw.println("\n\n\t\t FOLD " + (fld + 1) + "\n")

            //--------- TRAIN & TEST CREATION
            var startTRAINandTESTtime = System.currentTimeMillis()

            // From ArrayBuffers to RDD - SVDPlusPlus.run() needs RDD file
            var (trainArray0, testArray0) = createTESTandTRAIN(usersTOTAL0, itemsTOTAL0, ratingsTOTAL0, first_idx, last_idx, fld, inputSize(smpl))
            val trainRDD = sc.parallelize(trainArray0).map { input => Edge(input(0).toLong, input(1).toLong, input(2)) }

            // First & last index of the test set values for the next fold
            first_idx += (inputSize(smpl) * 0.1).toInt
            last_idx += (inputSize(smpl) * 0.1).toInt

            createTRAINandTESTtime(fld) = System.currentTimeMillis() - startTRAINandTESTtime

            pw.println("Time for Training & Test set creation = " + createTRAINandTESTtime(fld) / 1000f / 60f + "mins\t" + createTRAINandTESTtime(fld) + "ms")
            println("Time for Training & Test set creation = " + createTRAINandTESTtime(fld) / 1000f / 60f + "mins\t" + createTRAINandTESTtime(fld) + "ms")



            //--------- MODEL TRAINING
            var startModelTrainingTime = System.currentTimeMillis()

            // Calling SVDPlusPlus.run()
            var (g, mean) = trainModel(trainRDD, conf)

            ModelTrainingTime(fld) = System.currentTimeMillis() - startModelTrainingTime

            pw.println("Time for Model Training = " + ModelTrainingTime(fld) / 1000f / 60f + "mins\t" + ModelTrainingTime(fld) + "ms")
            println("Time for Model Training = " + ModelTrainingTime(fld) / 1000f / 60f + "mins\t" + ModelTrainingTime(fld) + "ms")



            //--------- MODEL EVALUATION

            var startEvalauationTime = System.currentTimeMillis()

            // Making rating predictions & calculating RMSE
            var (x, excep_perc, exceptions, not_exceptions) = evaluateModel(testArray0, trainArray0, conf, g, mean, mode)
            RMSE = x
            println("RMSE ====" + RMSE)

            EvalauationTime(fld) = System.currentTimeMillis() - startEvalauationTime

            pw.println("Time for Evaluation = " + EvalauationTime(fld) / 1000f / 60f + "mins\t" + EvalauationTime(fld) + "ms")
            pw.println("\tPercentage of 'exceptions' predictions = " + excep_perc + " %")

            println("Time for Evaluation = " + EvalauationTime(fld) / 1000f / 60f + "mins\t" + EvalauationTime(fld) + "ms")



            //--------- RMSEs, time & exceptions info
            // Average RMSE for the k-folds
            avgRMSE = avgRMSE + RMSE

            //execution time of every fold
            foldDuration(fld) = System.currentTimeMillis() - startTRAINandTESTtime
            pw.println("Total fold execution time: " + foldDuration(fld) / 1000f / 60f + "mins\t" + foldDuration(fld) + "ms")

            println("Total fold execution time: " + foldDuration(fld) / 1000f / 60f + "mins\t" + foldDuration(fld) + "ms")

            pw.println("RMSE = " + RMSE)
            pw.println("\n\n\n")

            // Exceptions info for each fold
            exceps_table(0)(fld) = excep_perc
            exceps_table(1)(fld) = exceptions
            exceps_table(2)(fld) = not_exceptions

            println("Fold " + (fld + 1) + "\tmode" + mode + " is completed\n\n\n\n")
          }


          // Recording exceptions info
          pw_excep.println("\n\nexceptions percentage\texceptions\tnot exceptions\t\ttotal predictions")

          for (i <- 0 to folds - 1) {
            pw_excep.println(exceps_table(0)(i) + "\t\t\t\t\t  " + exceps_table(1)(i).toInt + "\t\t  " + exceps_table(2)(i).toInt + "\t\t\t\t\t  " + (exceps_table(1)(i) + exceps_table(2)(i)).toInt)
            exceps_avg += exceps_table(0)(i)
          }

          pw_excep.println("\n\n")
          exceps_avg = exceps_avg / folds
          pw_excep.println("Average fallback percentage: " + exceps_avg)
          pw_plot.println("Average fallback percentage: " + exceps_avg)
          plotFormat(2) = exceps_avg.toString

          println("exceps_avg as sum: " + exceps_avg + "   folds = " + folds)
          println("Average fallback percentage: " + exceps_avg)
        }




        // mode = 3 - Train & Test set creation, Model Training & Model Evaluation
        if (mode == 3){

          // K-fold cross validation
          for (fld <- 0 until folds) {

            pw.println("\n\n\t\t FOLD " + (fld + 1) + "\n")

            //--------- TRAIN & TEST CREATION
            var startTRAINandTESTtime = System.currentTimeMillis()

            // From ArrayBuffers to RDD - SVDPlusPlus.run() needs RDD file
            var (trainArray3, testArray3) = createTESTandTRAIN2(usersTOTAL0, itemsTOTAL0, ratingsTOTAL0)
            val trainRDD = sc.parallelize(trainArray3).map { input => Edge(input(0).toLong, input(1).toLong, input(2)) }

            createTRAINandTESTtime(fld) = System.currentTimeMillis() - startTRAINandTESTtime

            pw.println("Time for Training & Test set creation = " + createTRAINandTESTtime(fld) / 1000f / 60f + "mins\t" + createTRAINandTESTtime(fld) + "ms")
            println("Time for Training & Test set creation = " + createTRAINandTESTtime(fld) / 1000f / 60f + "mins\t" + createTRAINandTESTtime(fld) + "ms")



            //--------- MODEL TRAINING
            var startModelTrainingTime = System.currentTimeMillis()

            // Calling SVDPlusPlus.run()
            var (g, mean) = trainModel(trainRDD, conf)
            ModelTrainingTime(fld) = System.currentTimeMillis() - startModelTrainingTime

            pw.println("Time for Model Training = " + ModelTrainingTime(fld) / 1000f / 60f + "mins\t" + ModelTrainingTime(fld) + "ms")
            println("Time for Model Training = " + ModelTrainingTime(fld) / 1000f / 60f + "mins\t" + ModelTrainingTime(fld) + "ms")



            //--------- MODEL EVALUATION
            var startEvalauationTime = System.currentTimeMillis()

            // Making rating predictions & calculating RMSE
            var (x, excep_perc, exceptions, not_exceptions) = evaluateModel(testArray3, trainArray3, conf, g, mean, mode)
            RMSE = x

            EvalauationTime(fld) = System.currentTimeMillis() - startEvalauationTime

            pw.println("Time for Evaluation = " + EvalauationTime(fld) / 1000f / 60f + "mins\t" + EvalauationTime(fld) + "ms")
            pw.println("\tPercentage of 'exceptions' predictions = " + excep_perc + " %")

            println("Time for Evaluation = " + EvalauationTime(fld) / 1000f / 60f + "mins\t" + EvalauationTime(fld) + "ms")



            //--------- RMSEs, time & exceptions info
            // Average RMSE for the k-folds
            avgRMSE = avgRMSE + RMSE

            //execution time of every fold
            foldDuration(fld) = System.currentTimeMillis() - startTRAINandTESTtime
            pw.println("Total fold execution time: " + foldDuration(fld) / 1000f / 60f + "mins\t" + foldDuration(fld) + "ms")
            pw.println("RMSE = " + RMSE)

            println("Total fold execution time: " + foldDuration(fld) / 1000f / 60f + "mins\t" + foldDuration(fld) + "ms\n\n\n")



            // Reshuffle input RDD in order to create different train & test sets for the next fold
            inputTOTAL = inputTOTAL.repartition(2)

            println("Fold " + (fld + 1) + "\tmode" + mode + " is completed\n\n\n\n")
          }
        }




        // modes = 2, 4, 5, 6, 7 (use subsamples) - Adding extra ratings to subsamples (modes 4, 5, 6, 7)
        if (mode == 2 || mode == 4 || mode == 5 || mode == 6 || mode == 7) {

          pw.println("Time for sample splitting = " + splitTime / 1000f / 60f + "mins\t" + splitTime + "ms\n\n")

          // mode = 2 - Subsample sparsities
          if (mode == 2) {

            pw.println("\t\tSPARSITIES (SUB-samples)\n")
            pw.println("sparsity of sub-sample1: " + usersTOTAL1_2(0).size / (usersTOTAL1_2(0).distinct.size * itemsTOTAL1_2(0).distinct.size.toDouble))
            pw.println("sparsity of sub-sample2: " + usersTOTAL1_2(1).size / (usersTOTAL1_2(1).distinct.size * itemsTOTAL1_2(1).distinct.size).toDouble + "\n")

            pw.println("distinct users sub1 -> " + usersTOTAL1_2(0).distinct.size)
            pw.println("distinct items sub1 -> " + itemsTOTAL1_2(0).distinct.size)
            pw.println("distinct users sub2 -> " + usersTOTAL1_2(1).distinct.size)
            pw.println("distinct items sub2 -> " + itemsTOTAL1_2(1).distinct.size + "\n\n\n\n")



            pw_excep.println("\n\t\tSPARSITIES (SUB-samples)\n")
            pw_excep.println("sparsity of sub-sample1: " + usersTOTAL1_2(0).size / (usersTOTAL1_2(0).distinct.size * itemsTOTAL1_2(0).distinct.size.toDouble))
            pw_excep.println("sparsity of sub-sample2: " + usersTOTAL1_2(1).size / (usersTOTAL1_2(1).distinct.size * itemsTOTAL1_2(1).distinct.size).toDouble + "\n")

            pw_excep.println("distinct users sub1 -> " + usersTOTAL1_2(0).distinct.size)
            pw_excep.println("distinct items sub1 -> " + itemsTOTAL1_2(0).distinct.size)
            pw_excep.println("distinct users sub2 -> " + usersTOTAL1_2(1).distinct.size)
            pw_excep.println("distinct items sub2 -> " + itemsTOTAL1_2(1).distinct.size + "\n\n\n\n")



            pw_plot.println("\n\t\tSPARSITIES (SUB-samples)\n")
            pw_plot.println("sparsity of sub-sample1: " + usersTOTAL1_2(0).size / (usersTOTAL1_2(0).distinct.size * itemsTOTAL1_2(0).distinct.size.toDouble))
            pw_plot.println("sparsity of sub-sample2: " + usersTOTAL1_2(1).size / (usersTOTAL1_2(1).distinct.size * itemsTOTAL1_2(1).distinct.size).toDouble + "\n")

            pw_plot.println("distinct users sub1 -> " + usersTOTAL1_2(0).distinct.size)
            pw_plot.println("distinct items sub1 -> " + itemsTOTAL1_2(0).distinct.size)
            pw_plot.println("distinct users sub2 -> " + usersTOTAL1_2(1).distinct.size)
            pw_plot.println("distinct items sub2 -> " + itemsTOTAL1_2(1).distinct.size + "\n\n\n\n")

          } else { // mode = 4,5,6,7 - Subsample sparsities before adding influencers

            pw.println("\t\tSPARSITIES before adding influencers \n")
            pw.println("Size of sub-sample1, BEFORE: " + usersTOTAL1_2(0).size)
            pw.println("Size of sub-sample2, BEFORE: " + usersTOTAL1_2(1).size + "\n")

            pw.println("sparsity of sub-sample1 BEFORE: " + usersTOTAL1_2(0).size / (usersTOTAL1_2(0).distinct.size * itemsTOTAL1_2(0).distinct.size.toDouble))
            pw.println("sparsity of sub-sample2 BEFORE: " + usersTOTAL1_2(1).size / (usersTOTAL1_2(1).distinct.size * itemsTOTAL1_2(1).distinct.size).toDouble + "\n")

            pw.println("distinct users sub1 BEFORE -> " + usersTOTAL1_2(0).distinct.size)
            pw.println("distinct items sub1 BEFORE -> " + itemsTOTAL1_2(0).distinct.size)
            pw.println("distinct users sub2 BEFORE -> " + usersTOTAL1_2(1).distinct.size)
            pw.println("distinct items sub2 BEFORE -> " + itemsTOTAL1_2(1).distinct.size + "\n\n\n\n")



            pw_excep.println("\t\tSPARSITIES before adding influencers \n")
            pw_excep.println("Size of sub-sample1, BEFORE: " + usersTOTAL1_2(0).size)
            pw_excep.println("Size of sub-sample2, BEFORE: " + usersTOTAL1_2(1).size + "\n")

            pw_excep.println("distinct users sub1 BEFORE -> " + usersTOTAL1_2(0).distinct.size)
            pw_excep.println("distinct items sub1 BEFORE -> " + itemsTOTAL1_2(0).distinct.size)
            pw_excep.println("distinct users sub2 BEFORE -> " + usersTOTAL1_2(1).distinct.size)
            pw_excep.println("distinct items sub2 BEFORE -> " + itemsTOTAL1_2(1).distinct.size + "\n")

            pw_excep.println("sparsity of sub-sample1 BEFORE: " + usersTOTAL1_2(0).size / (usersTOTAL1_2(0).distinct.size * itemsTOTAL1_2(0).distinct.size.toDouble))
            pw_excep.println("sparsity of sub-sample2 BEFORE: " + usersTOTAL1_2(1).size / (usersTOTAL1_2(1).distinct.size * itemsTOTAL1_2(1).distinct.size).toDouble + "\n\n\n\n")



            pw_plot.println("\n\n\t\tSPARSITIES before adding influencers \n")
            pw_plot.println("Size of sub-sample1, BEFORE: " + usersTOTAL1_2(0).size)
            pw_plot.println("Size of sub-sample2, BEFORE: " + usersTOTAL1_2(1).size + "\n")

            pw_plot.println("distinct users sub1 BEFORE -> " + usersTOTAL1_2(0).distinct.size)
            pw_plot.println("distinct items sub1 BEFORE -> " + itemsTOTAL1_2(0).distinct.size)
            pw_plot.println("distinct users sub2 BEFORE -> " + usersTOTAL1_2(1).distinct.size)
            pw_plot.println("distinct items sub2 BEFORE -> " + itemsTOTAL1_2(1).distinct.size + "\n")

            pw_plot.println("sparsity of sub-sample1 BEFORE: " + usersTOTAL1_2(0).size / (usersTOTAL1_2(0).distinct.size * itemsTOTAL1_2(0).distinct.size.toDouble))
            pw_plot.println("sparsity of sub-sample2 BEFORE: " + usersTOTAL1_2(1).size / (usersTOTAL1_2(1).distinct.size * itemsTOTAL1_2(1).distinct.size).toDouble + "\n\n\n\n")
          }


          // Temporary ArrayBuffers
          var usersTOTAL1_2TMP = new ArrayBuffer[org.apache.spark.graphx.VertexId]
          var itemsTOTAL1_2TMP = new ArrayBuffer[org.apache.spark.graphx.VertexId]
          var ratingsTOTAL1_2TMP = new ArrayBuffer[Double]

          // mode = 4 - Finding influencers and copying their ratings from one subsample to other
          if (mode == 4) {

            // FIND INFLUENCERS
            var startfindInfluencerTime = System.currentTimeMillis()

            // ArrayBuffer with the IDs of k influencers in the total sample
            var influencerTOTAL = findKinfluencers(usersTOTAL0, k_infl)
            findInfluencerTime = System.currentTimeMillis() - startfindInfluencerTime

            pw.println("\nTime for finding influencer = " + findInfluencerTime / 1000f / 60f + "mins\t" + findInfluencerTime + "ms\n\n")
            pw.println("no of influencers = " + influencerTOTAL.size + "\n")

            for (infl <- 0 to influencerTOTAL.size - 1) {
              pw.println("influencer ID: " + influencerTOTAL(infl)._1 + "  number of ratings: " + influencerTOTAL(infl)._2)
            }
            pw.println()



            // COPY INFLUENCER RATINGS
            var startcopyInfluencersRatsTime = System.currentTimeMillis()

            usersTOTAL1_2(1).copyToBuffer(usersTOTAL1_2TMP)
            itemsTOTAL1_2(1).copyToBuffer(itemsTOTAL1_2TMP)
            ratingsTOTAL1_2(1).copyToBuffer(ratingsTOTAL1_2TMP)

            var previous_sz_sub = usersTOTAL1_2(1).size
            var current_sz_sub = 0
            var incr = 0
            var total_rats_added = 0

            // Copying influencers' ratings from subsample 1 to subsample 2
            for (i <- 0 to k_infl - 1) {
              copyInfluencersRats(usersTOTAL1_2(0), itemsTOTAL1_2(0), ratingsTOTAL1_2(0), usersTOTAL1_2(1), itemsTOTAL1_2(1), ratingsTOTAL1_2(1), influencerTOTAL(i), selection)
              current_sz_sub = usersTOTAL1_2(1).size
              incr = current_sz_sub - previous_sz_sub

              pw.println("ratings of influencer " + influencerTOTAL(i)._1 + " added to sub-sample2: " + incr)

              previous_sz_sub = current_sz_sub
              total_rats_added += incr
            }
            pw.println("total ratings added to sub-sample2: " + total_rats_added + "\n")

            previous_sz_sub = usersTOTAL1_2(0).size
            current_sz_sub = 0
            total_rats_added = 0

            // Copying influencers' ratings from subsample 2 to subsample 1
            for (i <- 0 to k_infl - 1) {
              copyInfluencersRats(usersTOTAL1_2TMP, itemsTOTAL1_2TMP, ratingsTOTAL1_2TMP, usersTOTAL1_2(0), itemsTOTAL1_2(0), ratingsTOTAL1_2(0), influencerTOTAL(i), selection)
              current_sz_sub = usersTOTAL1_2(0).size
              incr = current_sz_sub - previous_sz_sub

              pw.println("ratings of influencer " + influencerTOTAL(i)._1 + " added to sub-sample1: " + incr)

              previous_sz_sub = current_sz_sub
              total_rats_added += incr
            }

            pw.println("total ratings added to sub-sample1: " + total_rats_added + "\n")
            pw.println("\nSize of sub-sample1, after adding influencer's ratings: " + usersTOTAL1_2(0).size)
            pw.println("Size of sub-sample2, after adding influencer's ratings: " + usersTOTAL1_2(1).size + "\n")
            pw.println("sparsity of sub-sample1 after adding influencer's ratings: " + usersTOTAL1_2(0).size / (usersTOTAL1_2(0).distinct.size * itemsTOTAL1_2(0).distinct.size.toDouble))
            pw.println("sparsity of sub-sample1 after adding influencer's ratings: " + usersTOTAL1_2(1).size / (usersTOTAL1_2(1).distinct.size * itemsTOTAL1_2(1).distinct.size).toDouble + "\n")

            println("\n\nSize of sub-sample1, after adding influencer's ratings: " + usersTOTAL1_2(0).size)
            println("Size of sub-sample2, after adding influencer's ratings: " + usersTOTAL1_2(1).size + "\n\n")

            // Delete temporary ArrayBuffers
            usersTOTAL1_2TMP.clear()
            itemsTOTAL1_2TMP.clear()
            ratingsTOTAL1_2TMP.clear()

            copyInfluencersRatsTime = System.currentTimeMillis() - startcopyInfluencersRatsTime
            pw.println("Time for copying influencer ratings = " + copyInfluencersRatsTime / 1000f / 60f + "mins\t" + copyInfluencersRatsTime + "ms\n\n")
          }


          // mode = 5 - Finding influencers and copying their ratings from one subsample to other
          if (mode == 5) {

            // FIND INFLUENCERS
            var startfindInfluencerTime = System.currentTimeMillis()

            // ArrayBuffers with the IDs of k influencers in the 2 subsamples
            var influencer1 = findKinfluencers(usersTOTAL1_2(0), k_infl)
            var influencer2 = findKinfluencers(usersTOTAL1_2(1), k_infl)
            findInfluencerTime = System.currentTimeMillis() - startfindInfluencerTime

            pw.println("\n\t\tINFLUENCERS\n")
            pw.println("Time for finding influencers = " + findInfluencerTime / 1000f / 60f + "mins\t" + findInfluencerTime + "ms\n")
            pw.println("no of influencers of subsample1 = " + influencer1.size + "\n")

            pw_excep.println("\n\t\tINFLUENCERS\n")
            pw_excep.println("no of influencers of subsample1 = " + influencer1.size + "\n")

            pw_plot.println("no of influencers of subsample1 = " + influencer1.size + "")

            for (infl <- 0 to influencer1.size - 1) {
              pw.println("influencer ID: " + influencer1(infl)._1 + "  number of ratings: " + influencer1(infl)._2)
              pw_excep.println("influencer ID: " + influencer1(infl)._1 + "  number of ratings: " + influencer1(infl)._2)
            }

            pw.println("\n\nno of influencers of subsample2 = " + influencer2.size + "\n")
            pw_excep.println("\n\nno of influencers of subsample2 = " + influencer2.size + "\n")
            pw_plot.println("no of influencers of subsample2 = " + influencer2.size + "")

            for (infl <- 0 to influencer2.size - 1) {
              pw.println("influencer ID: " + influencer2(infl)._1 + "  number of ratings: " + influencer2(infl)._2)
              pw_excep.println("influencer ID: " + influencer2(infl)._1 + "  number of ratings: " + influencer2(infl)._2)
            }
            pw.println("\n\n")
            pw_excep.println("\n")



            // COPY INFLUENCER RATINGS
            var startcopyInfluencersRatsTime = System.currentTimeMillis()

            usersTOTAL1_2(1).copyToBuffer(usersTOTAL1_2TMP)
            itemsTOTAL1_2(1).copyToBuffer(itemsTOTAL1_2TMP)
            ratingsTOTAL1_2(1).copyToBuffer(ratingsTOTAL1_2TMP)

            var previous_sz_sub = usersTOTAL1_2(1).size
            var current_sz_sub = 0
            var incr = 0
            var total_rats_added = 0

            // Copying influencers' ratings found in subsample 1 from subsample 1 to subsample 2 - We use the influencers of subsample 1
            for (i <- 0 to k_infl - 1) {
              copyInfluencersRats(usersTOTAL1_2(0), itemsTOTAL1_2(0), ratingsTOTAL1_2(0), usersTOTAL1_2(1), itemsTOTAL1_2(1), ratingsTOTAL1_2(1), influencer1(i), selection)
              current_sz_sub = usersTOTAL1_2(1).size
              incr = current_sz_sub - previous_sz_sub

              pw.println("ratings of influencer " + influencer1(i)._1 + " added to sub-sample2: " + incr)

              previous_sz_sub = current_sz_sub
              total_rats_added += incr
            }

            pw.println("total ratings added to sub-sample2: " + total_rats_added + "\n")

            pw_excep.println("total ratings added to sub-sample2: " + total_rats_added)

            pw_plot.println("total ratings added to sub-sample2: " + total_rats_added + "")

            previous_sz_sub = usersTOTAL1_2(0).size
            current_sz_sub = 0
            total_rats_added = 0

            // Copying influencers' ratings found in subsample 2 from subsample 2 to subsample 1 - We use the influencers of subsample 2
            for (i <- 0 to k_infl - 1) {
              copyInfluencersRats(usersTOTAL1_2TMP, itemsTOTAL1_2TMP, ratingsTOTAL1_2TMP, usersTOTAL1_2(0), itemsTOTAL1_2(0), ratingsTOTAL1_2(0), influencer2(i), selection)
              current_sz_sub = usersTOTAL1_2(0).size
              incr = current_sz_sub - previous_sz_sub

              pw.println("ratings of influencer " + influencer2(i)._1 + " added to sub-sample1: " + incr)

              previous_sz_sub = current_sz_sub
              total_rats_added += incr
            }

            pw.println("total ratings added to sub-sample1: " + total_rats_added + "\n\n\n")
            pw.println("\n\n\t\tSPARSITIES after adding influencer \n")
            pw.println("Size of sub-sample1, AFTER: " + usersTOTAL1_2(0).size)
            pw.println("Size of sub-sample2, AFTER: " + usersTOTAL1_2(1).size + "\n")
            pw.println("sparsity of sub-sample1 AFTER: " + usersTOTAL1_2(0).size / (usersTOTAL1_2(0).distinct.size * itemsTOTAL1_2(0).distinct.size.toDouble))
            pw.println("sparsity of sub-sample2 AFTER: " + usersTOTAL1_2(1).size / (usersTOTAL1_2(1).distinct.size * itemsTOTAL1_2(1).distinct.size).toDouble + "\n")
            pw.println("distinct users sub1 AFTER -> " + usersTOTAL1_2(0).distinct.size)
            pw.println("distinct items sub1 AFTER -> " + itemsTOTAL1_2(0).distinct.size)
            pw.println("distinct users sub2 AFTER -> " + usersTOTAL1_2(1).distinct.size)
            pw.println("distinct items sub2 AFTER -> " + itemsTOTAL1_2(1).distinct.size + "\n\n\n\n\n")

            pw_excep.println("total ratings added to sub-sample1: " + total_rats_added + "\n\n\n\n\n")
            pw_excep.println("\n\t\tSPARSITIES after adding influencers \n")
            pw_excep.println("Size of sub-sample1, AFTER: " + usersTOTAL1_2(0).size)
            pw_excep.println("Size of sub-sample2, AFTER: " + usersTOTAL1_2(1).size + "\n")
            pw_excep.println("distinct users sub1 AFTER -> " + usersTOTAL1_2(0).distinct.size)
            pw_excep.println("distinct items sub1 AFTER -> " + itemsTOTAL1_2(0).distinct.size)
            pw_excep.println("distinct users sub2 AFTER -> " + usersTOTAL1_2(1).distinct.size)
            pw_excep.println("distinct items sub2 AFTER -> " + itemsTOTAL1_2(1).distinct.size + "\n")
            pw_excep.println("sparsity of sub-sample1 AFTER: " + usersTOTAL1_2(0).size / (usersTOTAL1_2(0).distinct.size * itemsTOTAL1_2(0).distinct.size.toDouble))
            pw_excep.println("sparsity of sub-sample2 AFTER: " + usersTOTAL1_2(1).size / (usersTOTAL1_2(1).distinct.size * itemsTOTAL1_2(1).distinct.size).toDouble + "\n\n\n\n\n")

            pw_plot.println("total ratings added to sub-sample1: " + total_rats_added + "\n\n\n\n\n")
            pw_plot.println("\n\t\tSPARSITIES after adding influencers \n")
            pw_plot.println("Size of sub-sample1, AFTER: " + usersTOTAL1_2(0).size)
            pw_plot.println("Size of sub-sample2, AFTER: " + usersTOTAL1_2(1).size + "\n")
            pw_plot.println("distinct users sub1 AFTER -> " + usersTOTAL1_2(0).distinct.size)
            pw_plot.println("distinct items sub1 AFTER -> " + itemsTOTAL1_2(0).distinct.size)
            pw_plot.println("distinct users sub2 AFTER -> " + usersTOTAL1_2(1).distinct.size)
            pw_plot.println("distinct items sub2 AFTER -> " + itemsTOTAL1_2(1).distinct.size + "\n")
            pw_plot.println("sparsity of sub-sample1 AFTER: " + usersTOTAL1_2(0).size / (usersTOTAL1_2(0).distinct.size * itemsTOTAL1_2(0).distinct.size.toDouble))
            pw_plot.println("sparsity of sub-sample2 AFTER: " + usersTOTAL1_2(1).size / (usersTOTAL1_2(1).distinct.size * itemsTOTAL1_2(1).distinct.size).toDouble + "\n\n\n\n\n")

            plotFormat(3) = (usersTOTAL1_2(0).size / (usersTOTAL1_2(0).distinct.size * itemsTOTAL1_2(0).distinct.size.toDouble)).toString
            plotFormat(4) = (usersTOTAL1_2(1).size / (usersTOTAL1_2(1).distinct.size * itemsTOTAL1_2(1).distinct.size).toDouble).toString

            // Delete temporary ArrayBuffers
            usersTOTAL1_2TMP.clear()
            itemsTOTAL1_2TMP.clear()
            ratingsTOTAL1_2TMP.clear()

            copyInfluencersRatsTime = System.currentTimeMillis() - startcopyInfluencersRatsTime
            pw.println("Time for copying influencer ratings = " + copyInfluencersRatsTime / 1000f / 60f + "mins\t" + copyInfluencersRatsTime + "ms\n\n")
          }


          // mode = 6 - Finding influencers and copying their ratings from one subsample to other
          if (mode == 6){

            // FIND INFLUENCERS
            var startfindInfluencerTime = System.currentTimeMillis()

            // ArrayBuffers with the IDs of k influencers in the 2 subsamples
            var influencer1 = findKinfluencers(usersTOTAL1_2(0), k_infl)
            var influencer2 = findKinfluencers(usersTOTAL1_2(1), k_infl)
            findInfluencerTime = System.currentTimeMillis() - startfindInfluencerTime

            pw.println("Time for finding influencer = " + findInfluencerTime / 1000f / 60f + "mins\t" + findInfluencerTime + "ms\n\n")
            pw.println("no of influencers of subsample1 = " + influencer1.size + "\n")

            for (infl <- 0 to influencer1.size - 1) {
              pw.println("influencer ID: " + influencer1(infl)._1 + "  number of ratings: " + influencer1(infl)._2)
            }
            pw.println("\n\nno of influencers of subsample2 = " + influencer2.size + "\n")
            for (infl <- 0 to influencer2.size - 1) {
              pw.println("influencer ID: " + influencer2(infl)._1 + "  number of ratings: " + influencer2(infl)._2)
            }
            pw.println("\n\n")



            // COPY INFLUENCER RATINGS
            var startcopyInfluencersRatsTime = System.currentTimeMillis()

            usersTOTAL1_2(1).copyToBuffer(usersTOTAL1_2TMP)
            itemsTOTAL1_2(1).copyToBuffer(itemsTOTAL1_2TMP)
            ratingsTOTAL1_2(1).copyToBuffer(ratingsTOTAL1_2TMP)

            var previous_sz_sub = usersTOTAL1_2(1).size
            var current_sz_sub = 0
            var incr = 0
            var total_rats_added = 0

            // Copying influencers' ratings found in subsample 1 from subsample 1 to subsample 2 - We use the influencers of subsample 2
            for (i <- 0 to k_infl - 1) {
              copyInfluencersRats(usersTOTAL1_2(0), itemsTOTAL1_2(0), ratingsTOTAL1_2(0), usersTOTAL1_2(1), itemsTOTAL1_2(1), ratingsTOTAL1_2(1), influencer2(i), selection)
              current_sz_sub = usersTOTAL1_2(1).size
              incr = current_sz_sub - previous_sz_sub

              pw.println("ratings of influencer " + influencer2(i)._1 + " added to sub-sample2: " + incr)

              previous_sz_sub = current_sz_sub
              total_rats_added += incr
            }
            pw.println("total ratings added to sub-sample2: " + total_rats_added + "\n")

            previous_sz_sub = usersTOTAL1_2(0).size
            current_sz_sub = 0
            total_rats_added = 0

            // Copying influencers' ratings found in subsample 1 from subsample 1 to subsample 2 - We use the influencers of subsample 1
            for (i <- 0 to k_infl - 1) {
              copyInfluencersRats(usersTOTAL1_2TMP, itemsTOTAL1_2TMP, ratingsTOTAL1_2TMP, usersTOTAL1_2(0), itemsTOTAL1_2(0), ratingsTOTAL1_2(0), influencer1(i), selection)
              current_sz_sub = usersTOTAL1_2(0).size
              incr = current_sz_sub - previous_sz_sub

              pw.println("ratings of influencer " + influencer1(i)._1 + " added to sub-sample1: " + incr)

              previous_sz_sub = current_sz_sub
              total_rats_added += incr
            }

            pw.println("total ratings added to sub-sample1: " + total_rats_added + "\n")
            pw.println("Size of sub-sample1, after adding influencer's ratings: " + usersTOTAL1_2(0).size)
            pw.println("Size of sub-sample2, after adding influencer's ratings: " + usersTOTAL1_2(1).size + "\n")
            pw.println("sparsity of sub-sample1 after adding influencer's ratings: " + usersTOTAL1_2(0).size / (usersTOTAL1_2(0).distinct.size * itemsTOTAL1_2(0).distinct.size.toDouble))
            pw.println("sparsity of sub-sample1 after adding influencer's ratings: " + usersTOTAL1_2(1).size / (usersTOTAL1_2(1).distinct.size * itemsTOTAL1_2(1).distinct.size).toDouble + "\n")

            println("\n\nSize of sub-sample1, after adding influencer's ratings: " + usersTOTAL1_2(0).size)
            println("Size of sub-sample2, after adding influencer's ratings: " + usersTOTAL1_2(1).size + "\n\n")

            // Delete temporary ArrayBuffers
            usersTOTAL1_2TMP.clear()
            itemsTOTAL1_2TMP.clear()
            ratingsTOTAL1_2TMP.clear()

            copyInfluencersRatsTime = System.currentTimeMillis() - startcopyInfluencersRatsTime

            pw.println("Time for copying influencer ratings = " + copyInfluencersRatsTime / 1000f / 60f + "mins\t" + copyInfluencersRatsTime + "ms\n\n")
          }



          // mode = 7 - Finding least rated items and copying their ratings from one subsample to other
          if (mode == 7){

            // FIND LEAST RATED ITEMS
            var startfindLeastRatedItemsTime = System.currentTimeMillis()

            // ArrayBuffers with the ItemIDs of the least rated items and their frequencies in each subsample
            var (itemList1,ratFreqs1) = findLeastRatedItems(itemsTOTAL1_2(0), top_least_rating)
            var (itemList2,ratFreqs2) = findLeastRatedItems(itemsTOTAL1_2(1), top_least_rating)
            findInfluencerTime = System.currentTimeMillis() - startfindLeastRatedItemsTime

            pw.println("\n\t\tLEAST RATED ITEMS\n")
            pw.println("Time for finding least rated items = " + findInfluencerTime / 1000f / 60f + "mins\t" + findInfluencerTime + "ms\n")
            pw.println("no of least rated items1 = " + itemList1.size + "\n")

            pw_excep.println("\n\t\tLEAST RATED ITEMS\n")
            pw_excep.println("no of least rated items1 = " + itemList1.size + "\n")

            pw_plot.println("no of least rated items1 = " + itemList1.size + "\n")

            for(i<-0 to ratFreqs1.size-1){
              pw.println("ratFreqs1 "+(i+1)+" = "+ ratFreqs1(i))
              pw_excep.println("ratFreqs1 "+(i+1)+" = "+ ratFreqs1(i))
              pw_plot.println("ratFreqs1 "+(i+1)+" = "+ ratFreqs1(i))
            }

            pw.println("")
            pw_excep.println("")
            pw_plot.println("")

            for (it <- 0 to itemList1.size - 1) {
              pw.println("item ID: " + itemList1(it))
              pw_excep.println("item ID: " + itemList1(it))
            }

            pw.println("\nno of least rated items2 = " + itemList2.size + "\n")
            pw_excep.println("\nno of least rated items2 = " + itemList2.size + "\n")
            pw_plot.println("\nno of least rated items2 = " + itemList2.size + "")

            for(i<-0 to ratFreqs2.size-1){
              pw.println("ratFreqs2 "+(i+1)+" = "+ ratFreqs2(i))
              pw_excep.println("ratFreqs2 "+(i+1)+" = "+ ratFreqs2(i))
              pw_plot.println("ratFreqs2 "+(i+1)+" = "+ ratFreqs2(i))
            }

            pw.println("")
            pw_excep.println("")
            pw_plot.println("")

            for (it <- 0 to itemList2.size - 1) {
              pw.println("item ID: " + itemList2(it))
              pw_excep.println("item ID: " + itemList2(it))
            }

            pw.println("\n\n")
            pw_excep.println("\n")



            // COPY least rated items RATINGS
            var startcopyInfluencersRatsTime = System.currentTimeMillis()

            usersTOTAL1_2(1).copyToBuffer(usersTOTAL1_2TMP)
            itemsTOTAL1_2(1).copyToBuffer(itemsTOTAL1_2TMP)
            ratingsTOTAL1_2(1).copyToBuffer(ratingsTOTAL1_2TMP)

            // Copying least rated items' ratings found in subsample 2 from subsample 1 to subsample 2
            var previous_sz_sub = usersTOTAL1_2(1).size
            var current_sz_sub = 0
            var incr = 0
            var total_items_added = 0

            copyLeastRatedItems(usersTOTAL1_2(0), itemsTOTAL1_2(0), ratingsTOTAL1_2(0), usersTOTAL1_2(1), itemsTOTAL1_2(1), ratingsTOTAL1_2(1), itemList2, top_users)

            current_sz_sub = usersTOTAL1_2(1).size
            incr = current_sz_sub - previous_sz_sub
            previous_sz_sub = current_sz_sub
            total_items_added += incr

            pw.println("total ratings added to sub-sample2: " + total_items_added + "\n")
            pw_excep.println("total ratings added to sub-sample2: " + total_items_added)
            pw_plot.println("total ratings added to sub-sample2: " + total_items_added + "")

            // Copying least rated items' ratings found in subsample 1 from subsample 2 to subsample 1
            previous_sz_sub = usersTOTAL1_2(0).size
            current_sz_sub = 0
            total_items_added = 0

            copyLeastRatedItems(usersTOTAL1_2(1), itemsTOTAL1_2(1), ratingsTOTAL1_2(1), usersTOTAL1_2(0), itemsTOTAL1_2(0), ratingsTOTAL1_2(0), itemList1, top_users)

            current_sz_sub = usersTOTAL1_2(0).size
            incr = current_sz_sub - previous_sz_sub
            previous_sz_sub = current_sz_sub
            total_items_added += incr


            pw.println("total ratings added to sub-sample1: " + total_items_added + "\n")
            pw.println("\n\n\t\tSPARSITIES after adding least rated items \n")
            pw.println("Size of sub-sample1, AFTER: " + usersTOTAL1_2(0).size)
            pw.println("Size of sub-sample2, AFTER: " + usersTOTAL1_2(1).size + "\n")
            pw.println("sparsity of sub-sample1 AFTER: " + usersTOTAL1_2(0).size / (usersTOTAL1_2(0).distinct.size * itemsTOTAL1_2(0).distinct.size.toDouble))
            pw.println("sparsity of sub-sample2 AFTER: " + usersTOTAL1_2(1).size / (usersTOTAL1_2(1).distinct.size * itemsTOTAL1_2(1).distinct.size).toDouble + "\n")
            pw.println("distinct users sub1 AFTER -> " + usersTOTAL1_2(0).distinct.size)
            pw.println("distinct items sub1 AFTER -> " + itemsTOTAL1_2(0).distinct.size)
            pw.println("distinct users sub2 AFTER -> " + usersTOTAL1_2(1).distinct.size)
            pw.println("distinct items sub2 AFTER -> " + itemsTOTAL1_2(1).distinct.size + "\n\n\n\n\n")


            pw_excep.println("total ratings added to sub-sample1: " + total_items_added)
            pw_excep.println("\n\t\tSPARSITIES after adding least rated items \n")
            pw_excep.println("Size of sub-sample1, AFTER: " + usersTOTAL1_2(0).size)
            pw_excep.println("Size of sub-sample2, AFTER: " + usersTOTAL1_2(1).size + "\n")
            pw_excep.println("distinct users sub1 AFTER -> " + usersTOTAL1_2(0).distinct.size)
            pw_excep.println("distinct items sub1 AFTER -> " + itemsTOTAL1_2(0).distinct.size)
            pw_excep.println("distinct users sub2 AFTER -> " + usersTOTAL1_2(1).distinct.size)
            pw_excep.println("distinct items sub2 AFTER -> " + itemsTOTAL1_2(1).distinct.size + "\n")
            pw_excep.println("sparsity of sub-sample1 AFTER: " + usersTOTAL1_2(0).size / (usersTOTAL1_2(0).distinct.size * itemsTOTAL1_2(0).distinct.size.toDouble))
            pw_excep.println("sparsity of sub-sample2 AFTER: " + usersTOTAL1_2(1).size / (usersTOTAL1_2(1).distinct.size * itemsTOTAL1_2(1).distinct.size).toDouble + "\n\n\n\n\n")


            pw_plot.println("total ratings added to sub-sample1: " + total_items_added + "")
            pw_plot.println("\n\t\tSPARSITIES after adding least rated items \n")
            pw_plot.println("Size of sub-sample1, AFTER: " + usersTOTAL1_2(0).size)
            pw_plot.println("Size of sub-sample2, AFTER: " + usersTOTAL1_2(1).size + "\n")
            pw_plot.println("distinct users sub1 AFTER -> " + usersTOTAL1_2(0).distinct.size)
            pw_plot.println("distinct items sub1 AFTER -> " + itemsTOTAL1_2(0).distinct.size)
            pw_plot.println("distinct users sub2 AFTER -> " + usersTOTAL1_2(1).distinct.size)
            pw_plot.println("distinct items sub2 AFTER -> " + itemsTOTAL1_2(1).distinct.size + "\n")
            pw_plot.println("sparsity of sub-sample1 AFTER: " + usersTOTAL1_2(0).size / (usersTOTAL1_2(0).distinct.size * itemsTOTAL1_2(0).distinct.size.toDouble))
            pw_plot.println("sparsity of sub-sample2 AFTER: " + usersTOTAL1_2(1).size / (usersTOTAL1_2(1).distinct.size * itemsTOTAL1_2(1).distinct.size).toDouble + "\n\n\n\n\n")

            plotFormat(3) = (usersTOTAL1_2(0).size / (usersTOTAL1_2(0).distinct.size * itemsTOTAL1_2(0).distinct.size.toDouble)).toString
            plotFormat(4) = (usersTOTAL1_2(1).size / (usersTOTAL1_2(1).distinct.size * itemsTOTAL1_2(1).distinct.size).toDouble).toString

            // Deleting temporary ArrayBuffers
            usersTOTAL1_2TMP.clear()
            itemsTOTAL1_2TMP.clear()
            ratingsTOTAL1_2TMP.clear()

            copyInfluencersRatsTime = System.currentTimeMillis() - startcopyInfluencersRatsTime
            pw.println("Time for copying influencer ratings = " + copyInfluencersRatsTime / 1000f / 60f + "mins\t" + copyInfluencersRatsTime + "ms\n\n")
          }



          // RANDOM SHUFFLE of subgraphs AFTER adding influencers
          var rand = new Random(System.currentTimeMillis())
          var rand_idx = 0

          // Lines of the subsample
          var lines: ArrayBuffer[Int] = ArrayBuffer()

          for (sub <- 0 to 1) {

            // Initialization of "lines"
            for (i <- 0 to usersTOTAL1_2(sub).size - 1) {
              lines += i
            }

            // While there are still lines to be chosen, repeat...
            while (lines.size != 0) {

              // Picking a random line
              rand_idx = rand.nextInt(lines.size)

              // Adding the chosen line to the temporary ArrayBuffers
              usersTOTAL1_2TMP += usersTOTAL1_2(sub)(rand_idx)
              itemsTOTAL1_2TMP += itemsTOTAL1_2(sub)(rand_idx)
              ratingsTOTAL1_2TMP += ratingsTOTAL1_2(sub)(rand_idx)

              // Removing the chosen line from "lines"
              lines.remove(rand_idx)
            }

            // Destroying the subsample ArrayBuffers of the old order of ratings
            usersTOTAL1_2(sub).clear()
            itemsTOTAL1_2(sub).clear()
            ratingsTOTAL1_2(sub).clear()

            // Creating the subsample ArrayBuffers using the new order
            usersTOTAL1_2TMP.copyToBuffer(usersTOTAL1_2(sub))
            itemsTOTAL1_2TMP.copyToBuffer(itemsTOTAL1_2(sub))
            ratingsTOTAL1_2TMP.copyToBuffer(ratingsTOTAL1_2(sub))

            // Deleting temporary ArrayBuffers
            usersTOTAL1_2TMP.clear()
            itemsTOTAL1_2TMP.clear()
            ratingsTOTAL1_2TMP.clear()
          }



          // modes = 2, 4, 5, 6, 7 (use subsamples) - Train & Test set creation, Model Training & Model Evaluation
          exceps_avg = 0.0

          // For each subsample
          for (arr <- 0 to 1) {

            var sparsity1_2 = 0.0
            var dist_users1_2 = usersTOTAL1_2(arr).distinct.size
            var dist_items1_2 = itemsTOTAL1_2(arr).distinct.size
            sparsity1_2 = usersTOTAL1_2(arr).size.toDouble / ((dist_users1_2 - 1) * (dist_items1_2 - 1)).toDouble

            pw.println("\n\t\tSUB-SAMPLE " + (arr + 1))
            pw.println("Sub-sample(" + (arr + 1) + ") sparsity: " + sparsity1_2 + "\n")

            var first_idx = 0
            var last_idx = (inputSize(smpl) * 0.1 / 2 - 1).toInt
            var fld = 0

            // K-fold cross validation
            for (fld <- 0 until folds) {

              pw.println("\n\n\t\t FOLD " + (fld + 1) + " subsample" + (arr + 1) + "\n")

              //--------- TRAIN & TEST CREATION
              var startTRAINandTESTtime = System.currentTimeMillis()

              var (trainArray1_2, testArray1_2) = createTESTandTRAIN(usersTOTAL1_2(arr), itemsTOTAL1_2(arr), ratingsTOTAL1_2(arr), first_idx, last_idx, fld, inputSize(smpl) / 2)
              first_idx += (inputSize(smpl) / 2 * 0.1).toInt
              last_idx += (inputSize(smpl) / 2 * 0.1).toInt
              val trainRDD = sc.parallelize(trainArray1_2).map { input => Edge(input(0).toLong, input(1).toLong, input(2)) }
              createTRAINandTESTtime1_2(arr)(fld) = System.currentTimeMillis() - startTRAINandTESTtime

              pw.println("Time for Training & Test set creation = " + createTRAINandTESTtime1_2(arr)(fld) / 1000f / 60f + "mins\t" + createTRAINandTESTtime1_2(arr)(fld) + "ms")
              println("Time for Training & Test set creation = " + createTRAINandTESTtime1_2(arr)(fld) / 1000f / 60f + "mins\t" + createTRAINandTESTtime1_2(arr)(fld) + "ms")



              //--------- MODEL TRAINING
              var startModelTrainingTime = System.currentTimeMillis()

              // Calling SVDPlusPlus.run()
              var (g, mean) = trainModel(trainRDD, conf)
              ModelTrainingTime1_2(arr)(fld) = System.currentTimeMillis() - startModelTrainingTime

              pw.println("Time for Model Training = " + ModelTrainingTime1_2(arr)(fld) / 1000f / 60f + "mins\t" + ModelTrainingTime1_2(arr)(fld) + "ms")
              println("Time for Model Training = " + ModelTrainingTime1_2(arr)(fld) / 1000f / 60f + "mins\t" + ModelTrainingTime1_2(arr)(fld) + "ms")



              //--------- MODEL EVALUATION
              var startEvalauationTime = System.currentTimeMillis()

              // Making rating predictions & calculating RMSE
              var (x, excep_perc, exceptions, not_exceptions) = evaluateModel(testArray1_2, trainArray1_2, conf, g, mean, mode)
              RMSE = x
              println("RMSE ====" + RMSE)

              EvalauationTime1_2(arr)(fld) = System.currentTimeMillis() - startEvalauationTime

              pw.println("Time for Evaluation = " + EvalauationTime1_2(arr)(fld) / 1000f / 60f + "mins\t" + EvalauationTime1_2(arr)(fld) + "ms")
              pw.println("\tPercentage of 'exceptions' predictions = " + excep_perc + " %")


              println("Time for Evaluation = " + EvalauationTime1_2(arr)(fld) / 1000f / 60f + "mins\t" + EvalauationTime1_2(arr)(fld) + "ms")


              // Execution time of every fold
              foldDuration1_2(arr)(fld) = System.currentTimeMillis() - startTRAINandTESTtime
              pw.println("Total fold execution time: " + foldDuration1_2(arr)(fld) / 1000f / 60f + "mins\t" + foldDuration1_2(arr)(fld) + "ms")

              println("Total fold execution time: " + foldDuration1_2(arr)(fld) / 1000f / 60f + "mins\t" + foldDuration1_2(arr)(fld) + "ms")



              //--------- RMSEs, time & exceptions info
              // Average RMSE for the k-folds for the total sample
              pw.println("RMSE = " + RMSE)
              pw.println("\n\n\n")
              println("Fold " + (fld + 1) + "\tmode" + mode + "\tsubsample " + (arr+1) + " is completed\n\n\n\n")

              // Average RMSEs for the k-folds for each subsample
              if (arr == 0) {
                avgRMSE1 = avgRMSE1 + RMSE
              } else {
                avgRMSE2 = avgRMSE2 + RMSE
              }

              // Exceptions info for each fold
              exceps_table(0)(fld) = excep_perc
              exceps_table(1)(fld) = exceptions
              exceps_table(2)(fld) = not_exceptions
            }

            pw_excep.println("SUB-SAMPLE " + (arr + 1) + "\n")
            pw_excep.println("exceptions percentage\texceptions\tnot exceptions\t\ttotal predictions")

            for (i <- 0 to folds - 1) {
              pw_excep.println(exceps_table(0)(i) + "\t\t\t\t\t  " + exceps_table(1)(i).toInt + "\t\t  " + exceps_table(2)(i).toInt + "\t\t\t\t\t  " + (exceps_table(1)(i) + exceps_table(2)(i)).toInt)
              exceps_avg += exceps_table(0)(i)
            }
            pw_excep.println("\n\n")
          }

          println("exceps_avg as sum: " + exceps_avg + "   folds = " + folds)

          exceps_avg = exceps_avg / (folds * 2)

          pw_excep.println("Average fallback percentage: " + exceps_avg)
          pw_plot.println("Average fallback percentage: " + exceps_avg)

          plotFormat(2) = exceps_avg.toString
        }




        //      GENERAL RESULTS

        // General results for modes = 1, 3 (don't use subsamples)
        if (mode == 1 || mode == 3) {

          // GENERAL RESULTS FILE
          pw.println("\n\tGENERAL RESULTS\n\n")

          // Durations
          var avgFOLDtime = 0.0
          var avgCreateTRAINandTESTtime = 0.0
          var avgModelTraintime = 0.0
          var avgEvaluationtime = 0.0

          for (i <- 0 to 9) {
            avgCreateTRAINandTESTtime = avgCreateTRAINandTESTtime + createTRAINandTESTtime(i)
            avgModelTraintime = avgModelTraintime + ModelTrainingTime(i)
            avgEvaluationtime = avgEvaluationtime + EvalauationTime(i)
            avgFOLDtime = avgFOLDtime + foldDuration(i)
          }

          avgCreateTRAINandTESTtime = avgCreateTRAINandTESTtime / folds
          pw.println("Average Time for Train and Test set creation in mins: " + avgCreateTRAINandTESTtime / 1000f / 60f + " \tin ms: " + avgCreateTRAINandTESTtime)
          avgModelTraintime = avgModelTraintime / folds
          pw.println("Average Time for Model Training in mins: " + avgModelTraintime / 1000f / 60f + " \tin ms: " + avgModelTraintime)
          avgEvaluationtime = avgEvaluationtime / folds
          pw.println("Average Time for Evaluation in mins: " + avgEvaluationtime / 1000f / 60f + " \tin ms: " + avgEvaluationtime)
          avgFOLDtime = avgFOLDtime / folds
          pw.println("Average Fold execution time in mins: " + avgFOLDtime / 1000f / 60f + " \tin ms: " + avgFOLDtime)
          pw.println("\n\n")


          // Standard deviations
          var stdCreateTRAINtime = 0.0
          var stdModelTraintime = 0.0
          var stdEvaluationtime = 0.0
          var stdFOLDtime = 0.0

          for (i <- 0 to 9) {
            stdCreateTRAINtime = stdCreateTRAINtime + scala.math.pow((createTRAINandTESTtime(i) - avgCreateTRAINandTESTtime), 2)
            stdModelTraintime = stdModelTraintime + scala.math.pow((ModelTrainingTime(i) - avgModelTraintime), 2)
            stdEvaluationtime = stdEvaluationtime + scala.math.pow((EvalauationTime(i) - avgEvaluationtime), 2)
            stdFOLDtime = stdFOLDtime + scala.math.pow((foldDuration(i) - avgFOLDtime), 2)
          }

          stdCreateTRAINtime = scala.math.sqrt(stdCreateTRAINtime / folds)
          pw.println("Standard Deviation for Train and Test set creation in mins: " + stdCreateTRAINtime / 1000f / 60f + " \tin ms: " + stdCreateTRAINtime)
          stdModelTraintime = scala.math.sqrt(stdModelTraintime / folds)
          pw.println("Standard Deviation for Model Training in mins: " + stdModelTraintime / 1000f / 60f + " \tin ms: " + stdModelTraintime)
          stdEvaluationtime = scala.math.sqrt(stdEvaluationtime / folds)
          pw.println("Standard Deviation for Evaluation in mins: " + stdEvaluationtime / 1000f / 60f + " \tin ms: " + stdEvaluationtime)
          stdFOLDtime = scala.math.sqrt(stdFOLDtime / folds)
          pw.println("Standard Deviation for Fold execution time in mins: " + stdFOLDtime / 1000f / 60f + " \tin ms: " + stdFOLDtime)
          pw.println("\n\n")


          // Average exceptions percentage
          pw.println("Average fallback percentage: " + exceps_avg)

          // Average RMSE
          avgRMSE = avgRMSE / folds
          pw.println("Average RMSE = " + avgRMSE)

          // Finish time of the experiment
          var finishTimeTOTAL = System.currentTimeMillis()

          // Total execution time of the experiment
          var totalTime = finishTimeTOTAL - startTimeTOTAL

          pw.println("Total Execution Time in ms: " + totalTime)
          pw.println("Total Execution Time in s: " + totalTime / 1000f)
          pw.println("Total Execution Time in mins: " + totalTime / 1000f / 60f)

          //close the result file
          pw.close

          println("Total Execution Time in ms: " + totalTime)
          println("Total Execution Time in s: " + totalTime / 1000f)
          println("Total Execution Time in mins: " + totalTime / 1000f / 60f)



          // EXCEPTIONS FILE
          pw_excep.println("\nAverage RMSE = " + avgRMSE)
          pw_excep.println("Total Execution Time in mins: " + totalTime / 1000f / 60f)
          pw_excep.close()



          // PLOT INFO FILE
          pw_plot.println("\nAverage RMSE = " + avgRMSE)
          pw_plot.println("Total Execution Time in mins: " + totalTime / 1000f / 60f)
          pw_plot.println("\n\n\t\tFORMAT FOR PLOTS   file -> " + final_path_for_output)
          plotFormat(0) = avgRMSE.toString
          plotFormat(1) = (totalTime / 1000f / 60f).toString
          pw_plot.println(plotFormat(0).replace(".", ","))
          pw_plot.println(plotFormat(1).replace(".", ","))
          if(mode == 1){
            pw_plot.println(plotFormat(2).replace(".", ","))
          }
          pw_plot.close()
        }



        // General results for modes = 2, 4, 5, 6, 7 (use subsamples)
        if (mode == 2 || mode == 4 || mode == 5 || mode == 6 || mode == 7) {

          // GENERAL RESULTS FILE
          pw.println("\n\n\tGENERAL RESULTS sub-sample 1\n")

          //  SUBSAMPLE 1

          // Durations subsample 1
          var avg_createTRAINandTESTtime1 = 0.0
          var avg_ModelTrainingTime1 = 0.0
          var avg_EvalauationTime1 = 0.0
          var avg_foldDuration1 = 0.0

          for (i <- 0 to folds - 1) {
            avg_createTRAINandTESTtime1 += createTRAINandTESTtime1_2(0)(i)
            avg_ModelTrainingTime1 += ModelTrainingTime1_2(0)(i)
            avg_EvalauationTime1 += EvalauationTime1_2(0)(i)
            avg_foldDuration1 += foldDuration1_2(0)(i)
          }

          var totalSubsampleTime1 = avg_createTRAINandTESTtime1 + avg_ModelTrainingTime1 + avg_EvalauationTime1

          avg_createTRAINandTESTtime1 = avg_createTRAINandTESTtime1 / folds.toFloat
          pw.println("Average Time for Train and Test set creation (1) in mins: " + avg_createTRAINandTESTtime1 / 1000f / 60f + " \tin ms: " + avg_createTRAINandTESTtime1)
          avg_ModelTrainingTime1 = avg_ModelTrainingTime1 / folds.toFloat
          pw.println("Average Time for Model Training (1) in mins: " + avg_ModelTrainingTime1 / 1000f / 60f + " \tin ms: " + avg_ModelTrainingTime1)
          avg_EvalauationTime1 = avg_EvalauationTime1 / folds.toFloat
          pw.println("Average Time for Evaluation (1) in mins: " + avg_EvalauationTime1 / 1000f / 60f + " \tin ms: " + avg_EvalauationTime1)
          avg_foldDuration1 = avg_foldDuration1 / folds.toFloat
          pw.println("Average Fold execution time (1) in mins: " + avg_foldDuration1 / 1000f / 60f + " \tin ms: " + avg_foldDuration1 + "\n")


          // Standard deviations subsample 1
          var stdCreateTRAINandTESTtime1 = 0.0
          var stdModelTraintime1 = 0.0
          var stdEvaluationtime1 = 0.0
          var stdFOLDtime1 = 0.0

          for (i <- 0 to folds - 1) {
            stdCreateTRAINandTESTtime1 = stdCreateTRAINandTESTtime1 + scala.math.pow((createTRAINandTESTtime1_2(0)(i) - avg_createTRAINandTESTtime1), 2)
            stdModelTraintime1 = stdModelTraintime1 + scala.math.pow((ModelTrainingTime1_2(0)(i) - avg_ModelTrainingTime1), 2)
            stdEvaluationtime1 = stdEvaluationtime1 + scala.math.pow((EvalauationTime1_2(0)(i) - avg_EvalauationTime1), 2)
            stdFOLDtime1 = stdFOLDtime1 + scala.math.pow((foldDuration1_2(0)(i) - avg_foldDuration1), 2)
          }

          stdCreateTRAINandTESTtime1 = scala.math.sqrt(stdCreateTRAINandTESTtime1 / folds)
          pw.println("Standard Deviation for Train and Test set creation (1) in mins: " + stdCreateTRAINandTESTtime1 / 1000f / 60f + " \tin ms: " + stdCreateTRAINandTESTtime1)
          stdModelTraintime1 = scala.math.sqrt(stdModelTraintime1 / folds)
          pw.println("Standard Deviation for Model Training (1) in mins: " + stdModelTraintime1 / 1000f / 60f + " \tin ms: " + stdModelTraintime1)
          stdEvaluationtime1 = scala.math.sqrt(stdEvaluationtime1 / folds)
          pw.println("Standard Deviation for Evaluation (1) in mins: " + stdEvaluationtime1 / 1000f / 60f + " \tin ms: " + stdEvaluationtime1)
          stdFOLDtime1 = scala.math.sqrt(stdFOLDtime1 / folds)
          pw.println("Standard Deviation for Fold execution (1) time in mins: " + stdFOLDtime1 / 1000f / 60f + " \tin ms: " + stdFOLDtime1)


          // Average RMSE subsample 1
          avgRMSEtotal += avgRMSE1
          avgRMSE1 = avgRMSE1 / folds.toFloat
          pw.println("\nAverage RMSE1 = " + avgRMSE1)



          // SUBSAMPLE 2

          pw.println("\n\n\n\n\tGENERAL RESULTS sub-sample 2\n")

          // Durations subsample 2
          var avg_createTRAINandTESTtime2 = 0.0
          var avg_ModelTrainingTime2 = 0.0
          var avg_EvalauationTime2 = 0.0
          var avg_foldDuration2 = 0.0

          for (i <- 0 to folds - 1) {
            avg_createTRAINandTESTtime2 += createTRAINandTESTtime1_2(1)(i)
            avg_ModelTrainingTime2 += ModelTrainingTime1_2(1)(i)
            avg_EvalauationTime2 += EvalauationTime1_2(1)(i)
            avg_foldDuration2 += foldDuration1_2(1)(i)
          }

          var totalSubsampleTime2 = avg_createTRAINandTESTtime2 + avg_ModelTrainingTime2 + avg_EvalauationTime2

          avg_createTRAINandTESTtime2 = avg_createTRAINandTESTtime2 / folds.toFloat
          pw.println("Average Time for Train and Test set creation (2) in mins: " + avg_createTRAINandTESTtime2 / 1000f / 60f + " \tin ms: " + avg_createTRAINandTESTtime2)
          avg_ModelTrainingTime2 = avg_ModelTrainingTime2 / folds.toFloat
          pw.println("Average Time for Model Training (2) in mins: " + avg_ModelTrainingTime2 / 1000f / 60f + " \tin ms: " + avg_ModelTrainingTime2)
          avg_EvalauationTime2 = avg_EvalauationTime2 / folds.toFloat
          pw.println("Average Time for Evaluation (2) in mins: " + avg_EvalauationTime2 / 1000f / 60f + " \tin ms: " + avg_EvalauationTime2)
          avg_foldDuration2 = avg_foldDuration2 / folds.toFloat
          pw.println("Average Fold execution time (2) in mins: " + avg_foldDuration2 / 1000f / 60f + " \tin ms: " + avg_foldDuration2 + "\n")


          // Standard deviation subsample 2
          var stdCreateTRAINandTESTtime2 = 0.0
          var stdModelTraintime2 = 0.0
          var stdEvaluationtime2 = 0.0
          var stdFOLDtime2 = 0.0

          for (i <- 0 to folds - 1) {
            stdCreateTRAINandTESTtime2 = stdCreateTRAINandTESTtime2 + scala.math.pow((createTRAINandTESTtime1_2(1)(i) - avg_createTRAINandTESTtime2), 2)
            stdModelTraintime2 = stdModelTraintime2 + scala.math.pow((ModelTrainingTime1_2(1)(i) - avg_ModelTrainingTime2), 2)
            stdEvaluationtime2 = stdEvaluationtime2 + scala.math.pow((EvalauationTime1_2(1)(i) - avg_EvalauationTime2), 2)
            stdFOLDtime2 = stdFOLDtime2 + scala.math.pow((foldDuration1_2(1)(i) - avg_foldDuration2), 2)
          }

          stdCreateTRAINandTESTtime2 = scala.math.sqrt(stdCreateTRAINandTESTtime2 / folds)
          pw.println("Standard Deviation for Train and Test set creation (2) in mins: " + stdCreateTRAINandTESTtime2 / 1000f / 60f + " \tin ms: " + stdCreateTRAINandTESTtime2)
          stdModelTraintime2 = scala.math.sqrt(stdModelTraintime2 / folds)
          pw.println("Standard Deviation for Model Training (2) in mins: " + stdModelTraintime2 / 1000f / 60f + " \tin ms: " + stdModelTraintime2)
          stdEvaluationtime2 = scala.math.sqrt(stdEvaluationtime2 / folds)
          pw.println("Standard Deviation for Evaluation (2) in mins: " + stdEvaluationtime2 / 1000f / 60f + " \tin ms: " + stdEvaluationtime2)
          stdFOLDtime2 = scala.math.sqrt(stdFOLDtime2 / folds)
          pw.println("Standard Deviation for Fold execution (2) time in mins: " + stdFOLDtime2 / 1000f / 60f + " \tin ms: " + stdFOLDtime2)


          // Average RMSE subsample 2
          avgRMSEtotal += avgRMSE2
          avgRMSE2 = avgRMSE2 / folds.toFloat
          pw.println("\nAverage RMSE2 = " + avgRMSE2)


          // Finish time of the experiment
          var finishTimeTOTAL = System.currentTimeMillis()


          // Total execution time of the experiment
          var totalTime = finishTimeTOTAL - startTimeTOTAL



          // BOTH SUBSAMPLES

          pw.println("\n\n\n\tGENERAL RESULTS for both sub-samples\n")

          // Durations
          avgRMSEtotal = avgRMSEtotal / (2 * folds).toFloat
          pw.println("Average RMSE = " + avgRMSEtotal + "\n")
          pw.println("Total sub-sample Time 1 in mins: " + totalSubsampleTime1 / 1000f / 60f + " \tin ms: " + totalSubsampleTime1)
          pw.println("Total sub-sample Time 2 in mins: " + totalSubsampleTime2 / 1000f / 60f + " \tin ms: " + totalSubsampleTime2)
          pw.println("Total sub-sample Time 1 + Total sub-sample Time 2 in mins: " + (totalSubsampleTime1 + totalSubsampleTime2) / 1000f / 60f + " \tin ms: " + (totalSubsampleTime1 + totalSubsampleTime2))

          if (mode == 2) {
            pw.println("\n\nTOTAL Execution time (preliminaries, sample split and 2 subsample executions)")
          } else { // modes 4,5,6,7 have also the finding influencer stage
            pw.println("\n\nTOTAL Execution time (preliminaries, sample split, finding influencer, copying ratings and 2 subsample executions)")
          }

          pw.println("Total Execution Time in ms: " + totalTime)
          pw.println("Total Execution Time in s: " + totalTime / 1000f)
          pw.println("Total Execution Time in mins: " + totalTime / 1000f / 60f)
          pw.close

          println("Total Execution Time in ms: " + totalTime)
          println("Total Execution Time in s: " + totalTime / 1000f)
          println("Total Execution Time in mins: " + totalTime / 1000f / 60f)



          // EXCEPTIONS FILE
          pw_excep.println("\nAverage RMSE = " + avgRMSEtotal)
          pw_excep.println("Total Execution Time in mins: " + totalTime / 1000f / 60f)
          pw_excep.close()



          // PLOT INFO FILE
          pw_plot.println("\nAverage RMSE = " + avgRMSEtotal)
          pw_plot.println("Total Execution Time in mins: " + totalTime / 1000f / 60f)
          pw_plot.println("\n\n\t\tFORMAT FOR PLOTS   file -> " + final_path_for_output)
          plotFormat(0) = avgRMSEtotal.toString
          plotFormat(1) = (totalTime / 1000f / 60f).toString
          pw_plot.println(plotFormat(0).replace(".", ","))
          pw_plot.println(plotFormat(1).replace(".", ","))
          pw_plot.println(plotFormat(2).replace(".", ","))
          pw_plot.close()
        }
      }
    }

  }




  // Prediction function defined according to Yehuda's Koren paper - is used to predict rating
  // (copied from Malak, M., & East, R. (2016). Spark GraphX in action. Manning Publications Co..)
  def pred(g: Graph[(Array[Double], Array[Double], Double, Double), Double], mean: Double, u: Long, i: Long) = {
    val user = g.vertices.filter(_._1 == u).collect()(0)._2
    val item = g.vertices.filter(_._1 == i).collect()(0)._2
    mean + user._3 + item._3 +
      item._1.zip(user._2).map(x => x._1 * x._2).reduce(_ + _)
  }



  // Creates train & test sets for modes 1,2,4,5,6,7 (all the modes except for 3)
  def createTESTandTRAIN(usersTOTAL: ArrayBuffer[org.apache.spark.graphx.VertexId], itemsTOTAL: ArrayBuffer[org.apache.spark.graphx.VertexId],
                         ratingsTOTAL: ArrayBuffer[Double], first_idx: Int, last_idx: Int, fld: Int, inputSample: Int)
  : (Array[Array[Double]], Array[Array[Double]]) = {

    // Parameters
    /* Input
       usersTOTAL, itemsTOTAL, ratingsTOTAL -> userIDs, itemIDs & ratings
       first_idx, last_idx -> first & last index of test set
       fld -> k-th fold of cross-validation
       inputSample -> size of the original input (in modes 4,5,6,7 usersTOTAL, itemsTOTAL, ratingsTOTAL
       are greater than original input because influencers' ratings are added

       Output
       trainArray, testArray -> 2D arrays that represent train & test sets
    */

    var first_index = first_idx
    var last_index = last_idx

    // Calculation of train & test set sizes - cross-validation 90-10
    var sizeofTrain = (0.9 * usersTOTAL.size).toInt
    var sizeofTest = (0.1 * usersTOTAL.size).toInt

    // For modes 4,5,6,7
    if (usersTOTAL.size > inputSample) {

      // You can't use 10% and 90% of the original size when influencers' ratings are added,
      // because the new size maybe is not multiple of 10, i.e. 1036
      sizeofTest = usersTOTAL.size / 10
      sizeofTrain = usersTOTAL.size - sizeofTest

      last_index = sizeofTest * (fld + 1) - 1
      first_index = last_index - (sizeofTest - 1)

      // Last fold
      if (fld == 9) {

        sizeofTrain = usersTOTAL.size / 10 * 9
        sizeofTest = usersTOTAL.size - sizeofTrain

        last_index = usersTOTAL.size - 1
        first_index = last_index - (sizeofTest - 1)
      }
    }


    var totalSize = usersTOTAL.size
    var testArray = Array.ofDim[Double](sizeofTest, 3)
    var trainArray = Array.ofDim[Double](sizeofTrain, 3)

    // Creating the test set Array
    var k = 0
    for (i <- first_index to last_index) {
      testArray(k)(0) = usersTOTAL(i)
      testArray(k)(1) = itemsTOTAL(i)
      testArray(k)(2) = ratingsTOTAL(i)
      k += 1
    }

    // Creating the train set Array
    for (i <- 0 to first_index - 1) {
      trainArray(i)(0) = usersTOTAL(i)
      trainArray(i)(1) = itemsTOTAL(i)
      trainArray(i)(2) = ratingsTOTAL(i)
    }

    k = 0
    for (i <- last_index + 1 to totalSize - 1) {

      // First fold
      if (fld == 0) {
        trainArray(k)(0) = usersTOTAL(i)
        trainArray(k)(1) = itemsTOTAL(i)
        trainArray(k)(2) = ratingsTOTAL(i)
        k += 1
      } else { // All the other folds
        trainArray(i - first_index)(0) = usersTOTAL(i)
        trainArray(i - first_index)(1) = itemsTOTAL(i)
        trainArray(i - first_index)(2) = ratingsTOTAL(i)
      }
    }

    return (trainArray, testArray)
  }



  // Creates train & test sets only for mode 3
  def createTESTandTRAIN2(usersTOTAL: ArrayBuffer[org.apache.spark.graphx.VertexId], itemsTOTAL: ArrayBuffer[org.apache.spark.graphx.VertexId],
                          ratingsTOTAL: ArrayBuffer[Double])
  : (Array[Array[Double]], Array[Array[Double]]) = {

    // Parameters
    /* Input
       usersTOTAL, itemsTOTAL, ratingsTOTAL -> userIDs, itemIDs & ratings
       first_idx, last_idx -> first & last index of test set

       Output
       trainArray, testArray -> 2D arrays that represent train & test sets
    */

    // number of distinct users, number of distinct items & sample size
    var dist_users = usersTOTAL.distinct.size
    var dist_items = itemsTOTAL.distinct.size
    var totalRatings = usersTOTAL.size


    // Maps every distinct user to a rating tuple - key:userID, value: ratingTuple (userID,itemID,rating)
    val hashMapByUsers: HashMap[org.apache.spark.graphx.VertexId, ratingTuple] = HashMap.empty[org.apache.spark.graphx.VertexId, ratingTuple]

    // Adding values to distinct user hashmap
    var i = 0
    while (hashMapByUsers.size < dist_users) {

      var rt = new ratingTuple()
      rt.u_=(usersTOTAL(i))
      rt.i_=(itemsTOTAL(i))
      rt.r_=(ratingsTOTAL(i))

      hashMapByUsers += ((rt.u, rt))
      i = i + 1
    }

    // Maps every distinct item to a rating tuple - key:itemID, value: ratingTuple (userID,itemID,rating)
    val hashMapByItems: HashMap[org.apache.spark.graphx.VertexId, ratingTuple] = HashMap()

    // Adding values to distinct item hashmap
    // Scanning all the sample in order to find a rating tuple that doesn't exist in the distinct user hashmap
    for (i <- 0 to (totalRatings - 1)) {

      var rt = new ratingTuple()
      rt.u_=(usersTOTAL(i))
      rt.i_=(itemsTOTAL(i))
      rt.r_=(ratingsTOTAL(i))

      var u1 = hashMapByUsers(usersTOTAL(i)).u
      var i1 = hashMapByUsers(usersTOTAL(i)).i
      var r1 = hashMapByUsers(usersTOTAL(i)).r

      if (u1 != usersTOTAL(i) || i1 != itemsTOTAL(i) || r1 != ratingsTOTAL(i)) {
        hashMapByItems += (itemsTOTAL(i) -> rt)
      }
    }



    // Hashmaps sizes
    var uHashSize = hashMapByUsers.size
    var iHashSize = hashMapByItems.size
    // Train & test sets sizes
    var sizeofTrain = (0.9 * totalRatings).toInt
    var sizeofTest = (0.1 * totalRatings).toInt

    // Train & test set related arrays
    var trainArray = Array.ofDim[Double](sizeofTrain, 3)
    var testArray = Array.ofDim[Double](sizeofTest, 3)
    var users_test = ArrayBuffer[org.apache.spark.graphx.VertexId]()
    var items_test = ArrayBuffer[org.apache.spark.graphx.VertexId]()
    var rat_test = ArrayBuffer[Double]()

    // Adding rating tuples to train set from the distinct user hashmap
    i = 0
    for (k <- hashMapByUsers.keys) {

      trainArray(i)(0) = hashMapByUsers(k.toInt).u
      trainArray(i)(1) = hashMapByUsers(k.toInt).i
      trainArray(i)(2) = hashMapByUsers(k.toInt).r
      i = i + 1
    }
    println("size of hashusers = " + hashMapByUsers.size)

    // Adding rating tuples to train set from the distinct item hashmap
    var pl = 0
    for (k <- hashMapByItems.keys) {

      trainArray(i)(0) = hashMapByItems(k.toInt).u
      trainArray(i)(1) = hashMapByItems(k.toInt).i
      trainArray(i)(2) = hashMapByItems(k.toInt).r
      i = i + 1
      pl = pl + 1
    }
    println("i=" + i + "  pl=" + pl)
    println("size of hashitems = " + hashMapByItems.size)


    // The number of the remaining rating tuples that must be added to train set
    var limit = sizeofTrain - uHashSize - iHashSize

    // Filling the train set
    var l = i
    var j = 0
    var test = 0
    for (k <- 0 to (totalRatings - 1).toInt) {

      var u1 = hashMapByUsers(usersTOTAL(k)).u
      var i1 = hashMapByUsers(usersTOTAL(k)).i
      var r1 = hashMapByUsers(usersTOTAL(k)).r


      if (hashMapByItems.contains(itemsTOTAL(k))) {
        var u2 = hashMapByItems(itemsTOTAL(k)).u
        var i2 = hashMapByItems(itemsTOTAL(k)).i
        var r2 = hashMapByItems(itemsTOTAL(k)).r

        // Finding not used rating tuples
        if ((u1 != usersTOTAL(k) || i1 != itemsTOTAL(k) || r1 != ratingsTOTAL(k)) && (u2 != usersTOTAL(k) || i2 != itemsTOTAL(k) || r2 != ratingsTOTAL(k))) {
          if (j < limit) { // The rest elements for train set
            trainArray(l)(0) = usersTOTAL(k)
            trainArray(l)(1) = itemsTOTAL(k)
            trainArray(l)(2) = ratingsTOTAL(k)

            l = l + 1
            j = j + 1

          } else { // If train set is full, add elements to test set
            test = test + 1

            users_test += usersTOTAL(k)
            items_test += itemsTOTAL(k)
            rat_test += ratingsTOTAL(k)
          }
        }
      }
    }


    // Creating test set
    for (i <- 0 to testArray.size - 1) {

      testArray(i)(0) = users_test(i)
      testArray(i)(1) = items_test(i)
      testArray(i)(2) = rat_test(i)
    }

    return (trainArray, testArray)
  }



  // Training a model based on SVDPlusPlus.run() function
  def trainModel(trainRDD: RDD[Edge[Double]], conf: lib.SVDPlusPlus.Conf): (Graph[(Array[Double], Array[Double], Double, Double), Double], Double) = {
    // Parameters
    /* Input
       trainRDD -> RDD with the userIDs, itemsIDs & ratings of the training set
       conf -> object that includes the configuration parameters for SVD++

       Output
       g -> graph created by SVD++ and used by pred() function to predict ratings
       mean -> a number used by pred() function to predict ratings
    */

    var (g, mean) = lib.SVDPlusPlus.run(trainRDD, conf)
    return (g, mean)
  }



  // Model evaluation - The predicted ratings made by SVD++ are compared to real ratings
  def evaluateModel(testArray: Array[Array[Double]], trainArray: Array[Array[Double]], conf: lib.SVDPlusPlus.Conf, g: Graph[(Array[Double], Array[Double], Double, Double), Double], mean: Double, mode: Int): (Double, Double, Int, Int) = {

    // Parameters
    /* Input
       testArray -> array that represents the test set
       trainArray -> array that represents the train set
       conf -> object that includes the SVD++ configuration parameters
       g -> graph produced by the SVD++ model
       mean -> mean value produced by the SVD++ model
       mode -> the functioning mode (1,2,3,4,5,6,7)

       Output
       RMSE -> RMSE of the predictions
       exceptions_perc -> exceptions percentage
       exceptions -> number of exceptions
       NOTexceptions -> number of not exceptions
    */

    var excepLines: ArrayBuffer[Int] = new ArrayBuffer[Int]()   // ArrayBuffer that stores in which line of the test array the prediction throws exception
    var prd = 0.0   // prediction made by pred() function
    var RMSEsum = 0.0
    var exceptions = 0
    var NOTexceptions = 0
    var not_exceptions_mean = 0.0   // mean of "not exceptions" predictions
    var exceptions_mean = 0.0   // mean of "exceptions" predictions


    // Trying to predict a rating for every line of the test array
    var sizeofTest = testArray.size
    for (i <- 0 to sizeofTest - 1) {

      try {
        // Making prediction using pred()
        prd = pred(g, mean, testArray(i)(0).toLong, testArray(i)(1).toLong)

        // Predicted rating must be from 1 to 5
        prd = math.max(prd, conf.minVal)
        prd = math.min(prd, conf.maxVal)

        NOTexceptions += 1
        not_exceptions_mean += prd
        RMSEsum = RMSEsum + scala.math.pow(prd - testArray(i)(2), 2)

      } catch { // Exception - UserID or ItemID or both weren't found in the test array
        case ex: ArrayIndexOutOfBoundsException => exceptions = exceptions + 1
        excepLines += i
      }
    }

    not_exceptions_mean = not_exceptions_mean / NOTexceptions
    println("\nRMSE ONLY not exceptions -> " + (RMSEsum / NOTexceptions))
    println("non_fallback_mean  = " + not_exceptions_mean)

    var exceptions_perc = 0.0
    exceptions_perc = 100 * exceptions / sizeofTest



    // EXCEPTIONS HANDLING

    // UserIDs and ItemIDs of the train set
    var trainArrayU: ArrayBuffer[Double] = new ArrayBuffer[Double]()
    var trainArrayI: ArrayBuffer[Double] = new ArrayBuffer[Double]()

    // Map for the calculation of mean rating of every user - key:userID, value: (sum of ratings, number of ratings)
    val mapU = scala.collection.mutable.HashMap.empty[Double, (Double, Int)]

    // Map for the calculation of mean rating of every item - key:itemID, value: (sum of ratings, number of ratings)
    val mapI = scala.collection.mutable.HashMap.empty[Double, (Double, Int)]

    // All the modes except for 3
    if (mode != 3) {

      // Initialization of the 2 maps with userIDs, itemIDS  & zeros
      for (i <- 0 to trainArray.size - 1) {
        trainArrayU += trainArray(i)(0)
        trainArrayI += trainArray(i)(1)

        mapU += ((trainArrayU(i), (0.0, 0)))
        mapI += ((trainArrayI(i), (0.0, 0)))
      }

      // Calculation of the 2 maps
      var ratU: Double = 0.0
      var rat_cntU: Int = 0

      var ratI: Double = 0.0
      var rat_cntI: Int = 0

      for (i <- 0 to trainArray.size - 1) {

        // For each user calculating (sum of ratings, number of ratings)
        ratU = trainArray(i)(2) + mapU(trainArray(i)(0))._1
        rat_cntU = mapU(trainArray(i)(0))._2 + 1
        mapU.update(trainArrayU(i), (ratU, rat_cntU))

        // For each item calculating (sum of ratings, number of ratings)
        ratI = trainArray(i)(2) + mapI(trainArray(i)(1))._1
        rat_cntI = mapI(trainArray(i)(1))._2 + 1
        mapI.update(trainArrayI(i), (ratI, rat_cntI))
      }

      // UserIDs & ItemIDs that are inclued in excepLines ArrayBuffer
      var u_exp: Double = 0.0
      var i_exp: Double = 0.0

      for (i <- excepLines) {
        u_exp = testArray(i)(0)
        i_exp = testArray(i)(1)

        // Fallback because user and item weren't found
        if (!trainArrayU.contains(u_exp) && !trainArrayI.contains(i_exp)) {
          RMSEsum = RMSEsum + scala.math.pow(not_exceptions_mean - testArray(i)(2), 2)
        }
        // Fallback because item wasn't found
        if (trainArrayU.contains(u_exp) && !trainArrayI.contains(i_exp)) {
          exceptions_mean = mapU(u_exp)._1 / mapU(u_exp)._2
          RMSEsum = RMSEsum + scala.math.pow(exceptions_mean - testArray(i)(2), 2)
        }
        // Fallback because user wasn't found
        if (!trainArrayU.contains(u_exp) && trainArrayI.contains(i_exp)) {
          exceptions_mean = mapI(i_exp)._1 / mapI(i_exp)._2
          RMSEsum = RMSEsum + scala.math.pow(exceptions_mean - testArray(i)(2), 2)
        }
      }
    }

    println("exceptions, NOTexceptions ->" + exceptions + "  " + NOTexceptions)
    var RMSE = 0.0
    RMSE = scala.math.sqrt(RMSEsum / sizeofTest)
    return (RMSE, exceptions_perc, exceptions, NOTexceptions)
  }



  // Splits the initial input sample into to 2 subsamples
  def splitInput(usersTOTAL0: ArrayBuffer[org.apache.spark.graphx.VertexId], itemsTOTAL0: ArrayBuffer[org.apache.spark.graphx.VertexId],
                 ratingsTOTAL0: ArrayBuffer[Double], totalSize: Int): (Array[ArrayBuffer[org.apache.spark.graphx.VertexId]],
    Array[ArrayBuffer[org.apache.spark.graphx.VertexId]], Array[ArrayBuffer[Double]]) = {

    // Parameters
    /* Input
       usersTOTAL0, itemsTOTAL0, ratingsTOTAL0 -> userIDs, itemIDs & ratings of the total sample

       Output
       usersTOTAL1_2, itemsTOTAL1_2, ratingsTOTAL1_2 -> userIDs, itemIDs & ratings of the 2 subsamples
    */

    var usersTOTAL1_2: Array[ArrayBuffer[org.apache.spark.graphx.VertexId]] = new Array[ArrayBuffer[org.apache.spark.graphx.VertexId]](2)
    usersTOTAL1_2(0) = new ArrayBuffer[org.apache.spark.graphx.VertexId]
    usersTOTAL1_2(1) = new ArrayBuffer[org.apache.spark.graphx.VertexId]

    var itemsTOTAL1_2: Array[ArrayBuffer[org.apache.spark.graphx.VertexId]] = new Array[ArrayBuffer[org.apache.spark.graphx.VertexId]](2)
    itemsTOTAL1_2(0) = new ArrayBuffer[org.apache.spark.graphx.VertexId]
    itemsTOTAL1_2(1) = new ArrayBuffer[org.apache.spark.graphx.VertexId]

    var ratingsTOTAL1_2: Array[ArrayBuffer[Double]] = new Array[ArrayBuffer[Double]](2)
    ratingsTOTAL1_2(0) = new ArrayBuffer[Double]
    ratingsTOTAL1_2(1) = new ArrayBuffer[Double]


    // Creating a et that contains randomly chosen numbers which represent index in usersTOTAL0, itemsTOTAL0, ratingsTOTAL0
    var lines: Set[Int] = Set()
    var r = scala.util.Random
    while (lines.size < totalSize / 2) {
      lines += r.nextInt(totalSize)
    }
    println("set size = " + lines.size)

    var k1 = 0
    var k2 = 0
    for (i <- 0 to totalSize - 1) {

      // If i is found in set "lines", then add usersTOTAL0(i), itemsTOTAL0(i), ratingsTOTAL0(i) to the ArrayBuffers of subsample 1
      if (lines.contains(i)) {
        usersTOTAL1_2(0).insert(k1, usersTOTAL0(i))
        itemsTOTAL1_2(0).insert(k1, itemsTOTAL0(i))
        ratingsTOTAL1_2(0).insert(k1, ratingsTOTAL0(i))
        k1 += 1

      } else { // If i is not found in set "lines", then add usersTOTAL0(i), itemsTOTAL0(i), ratingsTOTAL0(i) to the ArrayBuffers of subsample 2
        usersTOTAL1_2(1).insert(k2, usersTOTAL0(i))
        itemsTOTAL1_2(1).insert(k2, itemsTOTAL0(i))
        ratingsTOTAL1_2(1).insert(k2, ratingsTOTAL0(i))
        k2 += 1
      }
    }

    return (usersTOTAL1_2, itemsTOTAL1_2, ratingsTOTAL1_2)
  }



  // Finds k influencers
  def findKinfluencers(usersTOTAL: ArrayBuffer[org.apache.spark.graphx.VertexId], k: Int): ArrayBuffer[(org.apache.spark.graphx.VertexId, Int)] = {

    // Parameters
    /* Input
       usersTOTAL -> userIDs
       k -> number of influencers to be found

       Output
       influencers -> ArrayBuffer that contains the found influencers
    */

    // HashMap which calculates the frequency of every distinct user
    val hashMapUsersFreq: HashMap[org.apache.spark.graphx.VertexId, Int] = HashMap.empty[org.apache.spark.graphx.VertexId, Int]

    var influencer: (org.apache.spark.graphx.VertexId, Int) = (0, 0)
    var influencers = new ArrayBuffer[(org.apache.spark.graphx.VertexId, Int)]


    // Initialization of the HashMap
    for (uID <- usersTOTAL.distinct) {
      hashMapUsersFreq.put(uID, 0)
    }

    // Calculation of users' frequency
    for (i <- 0 to usersTOTAL.size - 1) {
      hashMapUsersFreq(usersTOTAL(i)) += 1
    }
    println("\n\tfindInfluencer ")

    // Keeping only the K most frequent users, i.e. those who have the most ratings
    for (i <- 1 to k) {
      influencer = hashMapUsersFreq.maxBy(_._2)
      influencers += influencer
      hashMapUsersFreq.remove(influencer._1)
    }

    return (influencers)

  }



  // Copies influencers' ratings from one subsample to the other
  def copyInfluencersRats(usersTOTALcopied: ArrayBuffer[org.apache.spark.graphx.VertexId], itemsTOTALcopied: ArrayBuffer[org.apache.spark.graphx.VertexId],
                          ratingsTOTALcopied: ArrayBuffer[Double],
                          usersTOTALupdated: ArrayBuffer[org.apache.spark.graphx.VertexId], itemsTOTALupdated: ArrayBuffer[org.apache.spark.graphx.VertexId],
                          ratingsTOTALupdated: ArrayBuffer[Double],
                          influencer: (org.apache.spark.graphx.VertexId, Int), selection: Int):
  (ArrayBuffer[org.apache.spark.graphx.VertexId], ArrayBuffer[org.apache.spark.graphx.VertexId], ArrayBuffer[Double]) = {

    // Parameters
    /* Input
       usersTOTALcopied, itemsTOTALcopied, ratingsTOTALcopied -> userIDs, itemIDs & ratings of the sample from which the ratings are copied
       usersTOTALupdated itemsTOTALupdated, ratingsTOTALupdated -> userIDs, itemIDs & ratings of the sample that is going to be updated

       Output
       usersTOTALupdated, itemsTOTALupdated, ratingsTOTALupdated -> userIDs, itemIDs & ratings of the sample that is going to be updated
    */

    // ArrayBuffer that contains the copied ratings
    var copiedRats = ArrayBuffer[ratingTuple]()

    // Finding influencers' ratings
    var cnt = 0
    for (k <- 0 to usersTOTALcopied.size - 1) {

      // Influencer found in "usersTOTALcopied" ArrayBuffer
      if (usersTOTALcopied(k) == influencer._1) {

        var rt = new ratingTuple()
        rt.u_=(usersTOTALcopied(k))
        rt.i_=(itemsTOTALcopied(k))
        rt.r_=(ratingsTOTALcopied(k))

        //
        if (selection == 0) {
          copiedRats.insert(cnt, rt)
          cnt += 1
        } else {
          if (usersTOTALcopied(k) == influencer._1 && itemsTOTALupdated.contains(itemsTOTALcopied(k))) {
            copiedRats.insert(cnt, rt)
            cnt += 1
          }
        }
      }
    }

    // Adding copied ratings to the subsample that is going to be updated
    for (k <- 0 to copiedRats.size - 1) {
      usersTOTALupdated += copiedRats(k).u
      itemsTOTALupdated += copiedRats(k).i
      ratingsTOTALupdated += copiedRats(k).r
    }

    return (usersTOTALupdated, itemsTOTALupdated, ratingsTOTALupdated)
  }



  // Finds the items that have from 1 rating to a given number of ratings (it is the parameter "tlr")
   def findLeastRatedItems(itemsTOTAL: ArrayBuffer[org.apache.spark.graphx.VertexId], tlr: Int ): (ArrayBuffer[org.apache.spark.graphx.VertexId], ArrayBuffer[Int]) = {

     // Parameters
     /* Input
        itemsTOTAL -> itemIDs
        tlr -> top least rating, i.e. if it's 4, then the function must find all the items that have 1,2,3 or 4 ratings

        Output
        ItemList -> ArrayBuffer that contains the least rated items
        ratFreqs -> ArrayBuffer that contains the ratings frequencies
     */


     // HashMap which calculates the frequency of every distinct item
     val hashMapItemsFreq: HashMap[org.apache.spark.graphx.VertexId, Int] = HashMap.empty[org.apache.spark.graphx.VertexId, Int]
     // Stores the current least rated item of the HashMap
     var leastRatedItem: (org.apache.spark.graphx.VertexId, Int) = (0, 0)
     // The returned ArrayBuffer by this function
     var ItemList = new ArrayBuffer[org.apache.spark.graphx.VertexId]
     // Initialization of the ratings frequencies
     var ratFreqs = new ArrayBuffer[Int](tlr)
     for(i<-0 to tlr-1){
       ratFreqs+=0
     }


     // Initialization of the HashMap
     for (iID <- itemsTOTAL.distinct) {
       hashMapItemsFreq.put(iID, 0)
     }
     // Calculation of items' frequency
     for (i <- 0 to itemsTOTAL.size - 1) {
       hashMapItemsFreq(itemsTOTAL(i)) += 1
     }
     println("\tfindKleastRatedItems ")


     // Finding the least rated items
     leastRatedItem = hashMapItemsFreq.minBy(_._2)
     println("leastRatedItem -> " + leastRatedItem)

     while (leastRatedItem._2 <= tlr) {   // While the current least rated item is rated less than or equal to "tlr", repeat...
       ItemList += leastRatedItem._1
       ratFreqs(leastRatedItem._2-1)+=1
       hashMapItemsFreq.remove(leastRatedItem._1)
       leastRatedItem = hashMapItemsFreq.minBy(_._2)
     }

     return (ItemList, ratFreqs)
   }



  // Copies least rated items from one subsample to the other
  def copyLeastRatedItems(usersTOTALcopied: ArrayBuffer[org.apache.spark.graphx.VertexId], itemsTOTALcopied: ArrayBuffer[org.apache.spark.graphx.VertexId],
                          ratingsTOTALcopied: ArrayBuffer[Double],
                          usersTOTALupdated: ArrayBuffer[org.apache.spark.graphx.VertexId], itemsTOTALupdated: ArrayBuffer[org.apache.spark.graphx.VertexId],
                          ratingsTOTALupdated: ArrayBuffer[Double],
                          ItemList: ArrayBuffer[org.apache.spark.graphx.VertexId], top_users: Int):
  (ArrayBuffer[org.apache.spark.graphx.VertexId], ArrayBuffer[org.apache.spark.graphx.VertexId], ArrayBuffer[Double])= {

    // Parameters
    /* Input
       usersTOTALcopied, itemsTOTALcopied, ratingsTOTALcopied -> userIDs, itemIDs & ratings of the sample from which the ratings are copied
       usersTOTALupdated itemsTOTALupdated, ratingsTOTALupdated -> userIDs, itemIDs & ratings of the sample that is going to be updated
       ItemList -> ArrayBuffer that contains the least rated items
       top_users ->   // number of the users who rated the least rated items - if we choose 6, then we'll take the user who has rated the most
       of the least rated items, the 2nd "top user" who rated these items, the 3rd "top user"... until the 6th "top user"

       Output
       usersTOTALupdated, itemsTOTALupdated, ratingsTOTALupdated -> userIDs, itemIDs & ratings of the sample that is going to be updated
    */

    // Map for the creation of rating list for every user - key:userID, value: ArrayBuffer(itemID, ratings)
    var hashMap1: HashMap[org.apache.spark.graphx.VertexId, ArrayBuffer[(org.apache.spark.graphx.VertexId, Double)]] = HashMap.empty[org.apache.spark.graphx.VertexId, ArrayBuffer[(org.apache.spark.graphx.VertexId, Double)]]
    // Map for the calculation of number of ratings for every user - key:userID, value: number of ratings
    var hashMap2: HashMap[org.apache.spark.graphx.VertexId, Int] = HashMap.empty[org.apache.spark.graphx.VertexId, Int]


    // Hashmap initialization
    for (i <- 0 to usersTOTALcopied.size - 1) {

      if (ItemList.contains(itemsTOTALcopied(i))) {
        hashMap1.put(usersTOTALcopied(i), new ArrayBuffer[(org.apache.spark.graphx.VertexId, Double)])
        hashMap2.put(usersTOTALcopied(i), 0)
      }
    }


    // Values insertion
    for (i <- 0 to usersTOTALcopied.size - 1) {

      if (ItemList.contains(itemsTOTALcopied(i))) {
        hashMap1(usersTOTALcopied(i)) += ((itemsTOTALcopied(i), ratingsTOTALcopied(i)))
        hashMap2(usersTOTALcopied(i)) += 1
      }
    }


    // The current user who has given the most ratings
    var maxUser: org.apache.spark.graphx.VertexId = 0
    // ArrayBuffer which contains all the ratings of a specific user
    var ratList: ArrayBuffer[(org.apache.spark.graphx.VertexId, Double)] = new ArrayBuffer[(org.apache.spark.graphx.VertexId, Double)]
    println("\n\n\tLIMIT -> MAX TOP USERS = "+hashMap1.size)


    // Adding copied ratings to the subsample that is going to be updated
    for (i <- 0 to top_users - 1) {

      maxUser = hashMap2.maxBy(_._2)._1
      ratList = hashMap1(maxUser)

      // Add the ratings of the "maxUser" to the "updated" ArrayBuffers
      while (ratList.size > 0) {
        usersTOTALupdated += maxUser
        itemsTOTALupdated += ratList(0)._1
        ratingsTOTALupdated += ratList(0)._2
        ratList.remove(0)
      }

      hashMap2.remove(maxUser)
      hashMap1.remove(maxUser)
    }

    return (usersTOTALupdated, itemsTOTALupdated, ratingsTOTALupdated)
  }

}





