package soccer_analysis

import jdk.incubator.http.internal.frame.DataFrame
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import utility.RuntimeUtility

/*
  Analisi di un database di calcio
 */
object main {

  def main(args: Array[String]): Unit = {

    import org.apache.log4j._

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val clusterNumber = 4

    //var conf = new SparkConf().setAppName("SoccerAnalysis").setMaster("local[*]")
    //val sc = new SparkContext(conf)
    //val sqlContext = new SQLContext(sc)

    val sqlContext = SparkSession.builder()
      .appName("SoccerAnalysis")
      .master("local[*]")
      .config("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
      .getOrCreate()

    import sqlContext.implicits._


    //val textRDD = sc.textFile("src\\main\\resources\\FIFA19PlayerDB.csv")
    //textRDD.foreach(println)

    val playerDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      // In Linux scommenta la riga successiva e commenta quella dopo
 //   .load("src/main/resources/FIFA19PlayerDB.csv")
 //   .load("src\\main\\resources\\FIFA19PlayerDB.csv").repartition(4)
      .load("s3://scpdati/FIFA19PlayerDB.csv")
      .repartition(4)

    //playerDF.show()

    val assembler = new VectorAssembler()
      //  .setInputCols(Array("weight"))
      .setInputCols(Array("Pace",
      "Acceleration",
      "Sprint Speed",
      "Dribbling",
      "Agility",
      "Balance",
      "Reactions",
      "Ball Control",
      "Composure",
      "Shooting",
      "Positioning",
      "Finishing",
      "Shot Power",
      "Long Shots",
      "Volleys",
      "Penalties",
      "Passing",
      "Vision",
      "Crossing",
      "Free Kick",
      "Short Pass",
      "Long Pass",
      "Pass Curve",
      "Defending",
      "Interceptions",
      "Heading",
      "Marking",
      "Standing Tackle",
      "Sliding Tackle",
      "Physicality",
      "Jumping",
      "Stamina",
      "Strength",
      "Aggression",
      "Diving",
      "Reflexes",
      "Handling",
      "Speed",
      "Kicking",
      "Positoning"))
      .setOutputCol("features")

    /* Viene eseguito il clustering dei dati*/
    /*
    val kmeans = new KMeans().setK(clusterNumber).setSeed(1)

    val model = RuntimeUtility.time(kmeans.fit(assembler.transform(playerDF)))

    val predictions = model.transform(assembler.transform(playerDF))
    //val predictions2 = model2.transform(assembler.transform(playerDF))
    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance = $silhouette")
    */

    val (clusterList,modelList,timeList,errorList) = RuntimeUtility.clusterGeneration(assembler.transform(playerDF),List(2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17))
    clusterList.foreach(i => println(i))

    val predictions = clusterList(1)
    val model = modelList(1)

    // Clustering dei cluster
    val (clusterList2,modelList2,timeList2,errorList2) = RuntimeUtility
      .clusterGeneration(predictions.filter("prediction == 1").withColumnRenamed("prediction","old_prediction"),List(7))
    //clusterList2(0).show()
    val evaluator = new ClusteringEvaluator()
    var silhouette = List[Double]()
    for(i <- clusterList.indices){
      silhouette = silhouette :+ evaluator.evaluate(clusterList(i))
    }

    // Tabella delle performance al variare di K
    var performanceCluster = Seq((2,silhouette(0),errorList(0),timeList(0)))

    for(i <- 1 to 15) {

      performanceCluster = performanceCluster :+ (i + 2, silhouette(i), errorList(i),timeList(i))
    }

    val performance = performanceCluster.toDF("K","Score","WSS","Time")
      /*
    Seq((2,silhouette(0),timeList(0)),
      (4,silhouette(1),timeList(1)),
      (17,silhouette(2),timeList(2)))
      .toDF("K","Score","Time")
    */
    //  println("Cluster Centers: ")
  //  model.clusterCenters.foreach(println)
  //  println("Number of partition: ")
  //  println(playerDF.rdd.partitions.size)

  //  predictions.toDF().show()

    //predictions.filter("prediction == 3").show()
    val ruoli = udf((position: String) => position match {
      case "GK" => "Portiere"                     // Portiere
      case "LB" => "Terzino Sinistro"             // Difensore
      case "CB" => "Difensore centrale"           // Difensore
      case "RB" => "Terzino destro"               // Difensore
      case "CDM"=> "Centrocampista difensivo"     // Centrocampo
      case "CM" => "Centrocampista"               // Centrocampo
      case "CAM"=> "Centrocampista offensivo"     // Centrocampo
      case "LM" => "Esterno sinistro"             // Centrocampo  //
      case "LW" => "Ala sinistra"                 // Attacco
      case "LF" => "Attaccante sinistro"          // Attacco
      case "RM" => "Esterno destro"               // Centrocampo  //
      case "RW" => "Ala destra"                   // Attacco
      case "RF" => "Attaczante destro"            // Attacco
      case "CF" => "Seconda punta"                // Attacco
      case "ST" => "Attaccante"                   // Attacco
      case "RWB" => "Mediano destro"              // Centrocampo //
      case "LWB" => "Mediano sinistro"            // Centrocampo //
      case _ => "NotDefined"
    })

    val control = RuntimeUtility.extractRole(playerDF)

    //for(i <- 0 to 3)
    //  control(i).show()

    val count_position = control(2).toDF().withColumn("Position",ruoli(col("Position")))
      .groupBy("Position").count().
      withColumn("percentage", col("count") /  sum("count").over() * 100)

    //count_position.orderBy(col("count").desc).show(17)

    /*
      Vengono analizzati i cluster
     */
    // Definisco la funzione per calcolare la distanza di ongni punto dal suo centroide
    val distanceFromCenters = udf((features: Vector, c: Int) => Vectors.sqdist(features, model.clusterCenters(c)))



    // Applico la funzione al dataframe
    val predictionsITA = predictions.withColumn("Position",ruoli(col("Position")))

    val distancesDF = predictionsITA.withColumn("distanceFromCenter", distanceFromCenters(col("features"), col("prediction")))
    // Stampo i primi 10
    //distancesDF.filter("prediction == 0").sort(col("distanceFromCenter").desc).show(10)

    // Conto i giocatori per ogni ruolo
    val postPerc = predictionsITA.toDF().groupBy("Position").count().
      withColumn("percentage", col("count") /  sum("count").over() * 100)

    //postPerc.sort(col("percentage").desc).show()

    //postPerc.sort(col("percentage").desc).withColumn("Position",col(ruoli("Position"))).show()


    // Conto in ogni cluster i giocatori per ogni ruolo
    val res = predictionsITA.toDF().groupBy("prediction","Position").count().
      withColumn("percentage", col("count") /  sum("count").over() * 100)

    res.filter("prediction == 3").sort(col("percentage").desc).show()


    /*
      Vengono salvati i dati dell'analisi
     */

    // Cluster
    for(i <- 0 to 3)
      distancesDF.filter(col("prediction") === i).sort(col("distanceFromCenter").desc).limit(10)
        .select("Player Name","Overall","Position","prediction","distanceFromCenter")
        .write.mode(SaveMode.Overwrite)
        .option("header","True")
        .format("csv")
        .save("s3://scpdati/result/cluster"+i+"_first10_.csv")

    // Statistiche sul dataset
    postPerc.write.mode(SaveMode.Overwrite)
      .option("header","True")
      .format("csv")
      .save("s3://scpdati/result/count.csv")

    // Statistiche sui cluster
    res.sort(col("prediction").desc).
      write.mode(SaveMode.Overwrite)
      .option("header","True")
      .format("csv")
      .save("s3://scpdati/result/cluster_count.csv")

    // Performance di calcolo al variare di K
    performance.coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .option("header","True")
      .format("csv")
      .save("s3://scpdati/result/performance.csv")

    // Dataset con colonna prediction
    predictionsITA.toDF().select("Player Name",
      "Pace",
      "Acceleration",
      "Sprint Speed",
      "Dribbling",
      "Agility",
      "Balance",
      "Reactions",
      "Ball Control",
      "Composure",
      "Shooting",
      "Positioning",
      "Finishing",
      "Shot Power",
      "Long Shots",
      "Volleys",
      "Penalties",
      "Passing",
      "Vision",
      "Crossing",
      "Free Kick",
      "Short Pass",
      "Long Pass",
      "Pass Curve",
      "Defending",
      "Interceptions",
      "Heading",
      "Marking",
      "Standing Tackle",
      "Sliding Tackle",
      "Physicality",
      "Jumping",
      "Stamina",
      "Strength",
      "Aggression",
      "Diving",
      "Reflexes",
      "Handling",
      "Speed",
      "Kicking",
      "Positoning",
      "prediction").coalesce(1).write.mode(SaveMode.Overwrite)
      .option("header","True")
      .format("csv")
      .save("s3://scpdati/result/cluster.csv")

  }


}
