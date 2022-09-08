
name := "youtubeDataProject"

version := "0.1"

scalaVersion := "2.12.6"

resolvers ++= Seq(
  "Typesafe" at "https://repo.typesafe.com/typesafe/releases/",
  "Java.net Maven2 Repository" at "https://download.java.net/maven/2/",
  "IO Spring" at "https://repo.spring.io/plugins-release/"
)

libraryDependencies ++= {
  val sparkVer = "3.0.1"
  val hadoopVer = "3.2.1"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer % Provided,
    "org.apache.spark" %% "spark-sql" % sparkVer,
    "org.apache.spark" %% "spark-streaming" % sparkVer,
    "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVer,
    //"com.databricks" %% "spark-avro" % sparkVer,
    "com.google.apis" % "google-api-services-youtube" % "v3-rev174-1.22.0",
    "com.lihaoyi" %% "requests" % "0.7.0",
    "com.lihaoyi" %% "upickle" % "0.7.1",
    "org.json4s" %% "json4s-native" % "4.0.0",
    "org.postgresql" % "postgresql" % "42.2.5",
    "com.typesafe" % "config" % "1.4.1"
  )
}

assemblyJarName in assembly := "youtubeDataProject.jar"
mainClass in assembly := Some("com.example.App")

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.startsWith("META-INF") => MergeStrategy.discard
  case PathList("org", "apache", xs@_*) => MergeStrategy.first
  case _ => MergeStrategy.first
}

fork in run := true
javaOptions in run ++= Seq(
  "-Dlog4j.debug=true",
  "-Dlog4j.configuration=log4j.properties"
)
outputStrategy := Some(StdoutOutput)

fork in Test := true
parallelExecution in Test := false

logLevel in assembly := Level.Error
