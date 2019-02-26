name := "SoccerAnalysis"

version := "0.1"

scalaVersion := "2.12.8"

val sparkVersion = "2.4.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies ++= Seq {
  "org.apache.spark" %% "spark-core" % sparkVersion
  "org.apache.spark" %% "spark-sql"% sparkVersion
  "org.apache.spark" %% "spark-hive" % sparkVersion
  "org.apache.spark" %% "spark-mllib" % sparkVersion
}

// https://mvnrepository.com/artifact/mysql/mysql-connector-java
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.14"
