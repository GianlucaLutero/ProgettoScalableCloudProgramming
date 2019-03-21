package soccer_analysis

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


/*
  Analisi di un database di calcio
 */
object main {

  def main(args: Array[String]): Unit = {

    import org.apache.log4j._

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val clusterNumber = 4

    val ruoli = Map("GK"->"portiere",
      "LB"-> "Terzino Sinistro",
      "CB"-> "Difensore centrale",
      "RB"-> "Terzino destro",
      "CDM"-> "Centrocampista difensivo",
      "CM"-> "Centrocampista",
      "CAM"-> "Centrocampista offensivo",
      "LM"-> "Esterno sinistro",
      "LW"-> "Ala sinistra",
      "LF"-> "Attaccante sinistro",
      "RM"-> "Esterno destro",
      "RW"-> "Ala destra",
      "RF"-> "Attaczante destro",
      "CF"-> "Seconda punta",
      "ST"-> "Attaccante" )

    //var conf = new SparkConf().setAppName("SoccerAnalysis").setMaster("local[*]")
    //val sc = new SparkContext(conf)
    //val sqlContext = new SQLContext(sc)

    val sqlContext = SparkSession.builder()
      .appName("SoccerAnalysis")
      .master("local[*]")
      .config("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
      .getOrCreate()


    //val textRDD = sc.textFile("src\\main\\resources\\FIFA19PlayerDB.csv")
    //textRDD.foreach(println)

    val playerDF= sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      // In Linux scommenta la riga successiva e commenta quella dopo
 //      .load("src/main/resources/FIFA19PlayerDB.csv")
//      .load("src\\main\\resources\\FIFA19PlayerDB.csv").repartition(4)
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
    val kmeans = new KMeans().setK(clusterNumber).setSeed(1)
    val model = kmeans.fit(assembler.transform(playerDF))

    val predictions = model.transform(assembler.transform(playerDF))

    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance = $silhouette")

  //  println("Cluster Centers: ")
  //  model.clusterCenters.foreach(println)
  //  println("Number of partition: ")
  //  println(playerDF.rdd.partitions.size)

  //  predictions.toDF().show()

    //predictions.filter("prediction == 3").show()

    /*
      Vengono analizzati i cluster
     */
    // Definisco la funzione per calcolare la distanza di ongni punto dal suo centroide
    val distanceFromCenters = udf((features: Vector, c: Int) => Vectors.sqdist(features, model.clusterCenters(c)))

    // Applico la funzione al dataframe
    val distancesDF = predictions.withColumn("distanceFromCenter", distanceFromCenters(col("features"), col("prediction")))
    // Stampo i primi 10
    //distancesDF.filter("prediction == 0 AND Overall > 80").sort(col("distanceFromCenter").desc).show(10)


    // Conto i giocatori per ogni ruolo
    val postPerc = predictions.toDF().groupBy("Position").count().
      withColumn("percentage", col("count") /  sum("count").over() * 100)

    //postPerc.sort(col("percentage").desc).show()

    //postPerc.sort(col("percentage").desc).withColumn("Position",col(ruoli("Position"))).show()


    // Conto in ogni cluster i giocatori per ogni ruolo
    val res = predictions.toDF().groupBy("prediction","Position").count().
      withColumn("percentage", col("count") /  sum("count").over() * 100)

    //res.filter("prediction == 3").sort(col("percentage").desc).show()


    /*
      Vengono salvati i dati dell'analisi
     */

    for(i <- 0 to 3)
      distancesDF.filter(col("prediction") === i).sort(col("distanceFromCenter").desc).limit(10).
        select("Player Name","Overall","Position","prediction","distanceFromCenter").
        write.mode(SaveMode.Overwrite).option("header","True").format("csv").save("s3://scpdati/result/cluster"+i+"_first10_.csv")

    postPerc.write.mode(SaveMode.Overwrite).option("header","True").format("csv").save("s3://scpdati/result/count.csv")

    res.sort(col("prediction").desc).
      write.mode(SaveMode.Overwrite).option("header","True").format("csv").save("s3://scpdati/result/cluster_count.csv")

    predictions.toDF().select("Player Name",
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
      "prediction").coalesce(1).write.mode(SaveMode.Overwrite).option("header","True").format("csv").save("s3://scpdati/result/cluster.csv")

  }


}
