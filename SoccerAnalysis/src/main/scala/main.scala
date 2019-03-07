import breeze.linalg.eigSym.justEigenvalues.EigSym_DM_Impl
import org.apache.commons.math3.util.MathArrays.Position
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{Vectors,Vector}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col

/*
  Analisi di un database di calcio
 */
object main {

  def main(args: Array[String]): Unit = {

    val clusterNumber = 4

    val ruoli = Map(col("GK")->col("portiere"),
      col("LB")-> col("Terzino Sinistro"),
      col("CB")-> col("Difensore centrale"),
      col("RB")-> col("Terzino destro"),
      col("CDM")-> col("Centrocampista difensivo"),
      col("CM")-> col("Centrocampista"),
      col("CAM")-> col("Centrocampista offensivo"),
      col("LM")-> col("Esterno sinistro"),
      col("LW")-> col("Ala sinistra"),
      col("LF")-> col("Attaccante sinistro"),
      col("RM")-> col("Esterno destro"),
      col("RW")-> col("Ala destra"),
      col("RF")-> col("Attaczante destro"),
      col("CF")-> col("Seconda punta"),
      col("ST")-> col("Attaccante") )

    var conf = new SparkConf().setAppName("SoccerAnalysis").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //val textRDD = sc.textFile("src\\main\\resources\\FIFA19PlayerDB.csv")

    //textRDD.foreach(println)

    val playerDF= sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      // In Linux scommenta la riga successiva e commenta quella dopo
//    .load("src/main/resources/FIFA19PlayerDB.csv")
      .load("src\\main\\resources\\FIFA19PlayerDB.csv").repartition(4)

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

    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)
    println("Number of partition: ")
    println(playerDF.rdd.partitions.size)

    predictions.toDF().show()

    //predictions.filter("prediction == 3").show()

    /*
      Vengono analizzati i cluster
     */
    // Definisco la funzione per calcolare la distanza di ongni punto dal suo centroide
    val distanceFromCenters = udf((features: Vector, c: Int) => Vectors.sqdist(features, model.clusterCenters(c)))

    // Applico la funzione al dataframe
    val distancesDF = predictions.withColumn("distanceFromCenter", distanceFromCenters(col("features"), col("prediction")))
    // Stampo i primi 10
    distancesDF.filter("prediction == 0 AND Overall > 80").sort(col("distanceFromCenter").desc).show(10)



    val res = predictions.toDF().groupBy("prediction","Position").count().
      withColumn("percentage", col("count") /  sum("count").over() * 100)

    res.filter("prediction == 3").sort(col("percentage").desc).show()

    /*
      Vengono salvati i dati dell'analisi
     */
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
      "prediction").write.mode(SaveMode.Overwrite).option("header","True").format("csv").save("src\\main\\resources\\test.csv")

  }


}
