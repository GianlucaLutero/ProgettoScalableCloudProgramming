
name := "SoccerAnalysis"

version := "0.1"

scalaVersion := "2.12.8"

val sparkVersion = "2.4.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core

/*
libraryDependencies ++= Seq {
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
  "org.apache.spark" %% "spark-sql"% sparkVersion % "provided"
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"
}
*/

libraryDependencies ++= Seq {
  "org.apache.spark" %% "spark-core" % sparkVersion
  "org.apache.spark" %% "spark-sql"% sparkVersion
  "org.apache.spark" %% "spark-hive" % sparkVersion
  "org.apache.spark" %% "spark-mllib" % sparkVersion
  
}

// https://mvnrepository.com/artifact/mysql/mysql-connector-java
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.14"

// https://mvnrepository.com/artifact/com.amazonaws/aws-android-sdk-s3
libraryDependencies += "com.amazonaws" % "aws-android-sdk-s3" % "2.12.5"


assemblyMergeStrategy in assembly := {
  case PathList("org","aopalliance", xs @ _*)       => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*)         => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*)        => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*)     => MergeStrategy.last
  case PathList("org", "apache", xs @ _*)           => MergeStrategy.last
  case PathList("com", "google", xs @ _*)           => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*)         => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*)           => MergeStrategy.last
  case "git.properties"                             => MergeStrategy.discard
  case "about.html"                                 => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA"                      => MergeStrategy.last
  case "META-INF/mailcap"                           => MergeStrategy.last
  case "META-INF/mimetypes.default"                 => MergeStrategy.last
  case "plugin.properties"                          => MergeStrategy.last
  case "log4j.properties"                           => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}