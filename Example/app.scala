import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object app {

   def main(args: Array[String]): Unit = {

     Class.forName("com.mysql.cj.jdbc.Driver").newInstance

     var conf = new SparkConf().setAppName("TestProgetoSCP").setMaster("local[*]")
     val sc = new SparkContext(conf)
     val sqlContext = new SQLContext(sc)
     import sqlContext.implicits._
 //    val textRDD = sc.textFile("src\\main\\resources\\card_detail.csv")

   //  println(textRDD.foreach(println))
//    DatabaseConnection

/*     val empDF= sqlContext.read.format("csv")
       .option("header", "true")
       .option("inferSchema", "true")
       .load("src\\main\\resources\\card_detail.csv")

     empDF.show()*/

      val playerDF = sqlContext.read
        .format("jdbc")
        .option("url","jdbc:mysql://localhost/soccer?useLegacyDatetimeCode=false&serverTimezone=UTC")
        .option("user","root")
        .option("password","root")
        .option("dbtable","team_attributes")
        .load()


     val assembler = new VectorAssembler()
     //  .setInputCols(Array("weight"))
         .setInputCols(Array("buildUpPlaySpeed"))
       .setOutputCol("features")

     val kmeans = new KMeans().setK(5).setSeed(1)
     val model = kmeans.fit(assembler.transform(playerDF))

     val predictions = model.transform(assembler.transform(playerDF))

     val evaluator = new ClusteringEvaluator()

     val silhouette = evaluator.evaluate(predictions)
     println(s"Silhouette with squared euclidean distance = $silhouette")

     println("Cluster Centers: ")
     model.clusterCenters.foreach(println)
  }
}
