import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/*
  Analisi di un database di calcio
 */
object main {

  def main(args: Array[String]): Unit = {

    val ruoli:Map[String,String] = Map("GK"->"portiere",
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

    val kmeans = new KMeans().setK(15).setSeed(1)
    val model = kmeans.fit(assembler.transform(playerDF))

    val predictions = model.transform(assembler.transform(playerDF))

    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance = $silhouette")

    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)
    println("Number of partition: ")
    println(playerDF.rdd.partitions.size)

    //predictions.toDF().show()

    predictions.filter("prediction == 3").show()

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
      "prediction").write.option("header","True").csv("src\\main\\resources\\test.csv")
  }


}
