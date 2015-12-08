name := "IICFiltering"
version := "1.0"
scalaVersion := "2.10.6"
mainClass in assembly := Some("com.sidesna.iicfiltering.Main")


assemblyOption in assembly ~= { _.copy(includeScala = false) }

unmanagedResourceDirectories in Compile += { baseDirectory.value / "src/main/config" }

resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.2" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.5.2" % "provided",
  "org.apache.spark" %% "spark-hive" % "1.5.2" % "provided",
  "org.apache.spark" %% "spark-mllib" % "1.5.2" % "provided",
  "com.typesafe.play" %% "play-json" % "2.3.4",
  "mysql" % "mysql-connector-java" % "5.1.37"
)