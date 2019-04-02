package utility

import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, _}

/*
  - Calcolare i tempi di esecuzione al variare di k (2 - 4 - 17) e numero di processori (1-3)
  - Calcolare l'errore per la cluster analysis
  - Grafici dei risultati -> in python
  - Aggregare posizioni simili (es: terzino destro e terzino sinistro)
 */



object RuntimeUtility {

  /*
    Calcolo dei tempi di un blocco di codice
   */
  def time[R](block: => R): (R,Long) = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    println("Elapsed time: " + (t1 - t0) + "ms")
    (result,t1-t0)
  }

  /*
    Divide i giocatori per ruoli: Portieri, Difensori, Centrocampisti e Attaccanti
   */
  def extractRole(df: DataFrame) : List[DataFrame] = {
    val roleList = List(
      df.filter(col("Position") === "GK"),
      df.filter(col("Position") === "LB" ||
        col("Position") === "RB" ||
        col("Position") === "CB"
      ),
      df.filter(col("Position") === "CDM" ||
        col("Position") === "LM" ||
        col("Position") === "RM" ||
        col("Position") === "CM" ||
        col("Position") === "CAM"||
        col("Position") === "LWB" ||
        col("Position") === "RWB"
      ),
      df.filter(col("Position") === "LW" ||
        col("Position") === "LF" ||
        col("Position") === "RW" ||
        col("Position") === "RF" ||
        col("Position") === "CF" ||
        col("Position") === "ST"
      )
    )
    roleList
  }

  def extractRole12(df: DataFrame) : List[DataFrame] = {
    val roleList12 = List(
      df.filter(col("Position") === "GK"),
      df.filter(col("Position") === "CB"),
      df.filter(col("Position") === "LB" ||
        col("Position") === "RB"),


      df.filter(col("Position") === "CDM"),
      df.filter(col("Position") === "CM"),
      df.filter(col("Position") === "CAM"),

      df.filter(col("Position") === "LM" ||
        col("Position") === "RM"),

      df.filter(col("Position") === "LWB" ||
        col("Position") === "RWB"),


      df.filter(col("Position") === "CF"),
      df.filter(col("Position") === "ST"),

      df.filter(col("Position") === "LW" ||
        col("Position") === "RW"),

      df.filter(col("Position") === "LF" ||
        col("Position") === "RF"
      )

    )
    roleList12
  }

  /*
    Funzione per eseguire kmeans in automatico con k crescente
   */

  def clusterGeneration(df : DataFrame,kList : List[Int]) = {

    var ll =  List[DataFrame]()
    var lm = List[KMeansModel]()
    var lt =List[Long]()

    for( i <- kList){
      val kmeans = new KMeans().setK(i).setSeed(1)


      println(s"Cluster con K:$i")
      val (model, time) = RuntimeUtility.time(kmeans.fit(df))

      lm = lm :+ model
      lt = lt :+ time

      val predictions = model.transform(df)

      ll = ll :+ predictions
    }

    (ll,lm,lt)
  }

}




