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
      .load("src\\main\\resources\\FIFA19PlayerDB.csv")

    //playerDF.show()

    val assembler = new VectorAssembler()
      //  .setInputCols(Array("weight"))
      .setInputCols(Array("CB"))
      .setOutputCol("features")

    val kmeans = new KMeans().setK(5).setSeed(1)
    val model = kmeans.fit(assembler.transform(playerDF))

    val predictions = model.transform(assembler.transform(playerDF))

    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance = $silhouette")

    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)
    predictions.toDF().show()

  }


}
