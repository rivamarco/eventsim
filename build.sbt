name := "eventsim"

version := "1.0"

libraryDependencies += "org.apache.commons" % "commons-math3" % "3.5"

libraryDependencies += "de.jollyday" % "jollyday" % "0.5.1"

libraryDependencies += "org.rogach" %% "scallop" % "3.3.0"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.6.1"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.1"

libraryDependencies += "org.apache.kafka" % "kafka_2.10" % "0.8.2.1"

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"

enablePlugins(JavaServerAppPackaging, SbtNativePackager)

lazy val packageDist   = taskKey[File]("create universal package and move it to package folder")
addCommandAlias("dist", "packageDist")

packageDist := {
  val distFile = (packageBin in Universal).value
  val output   = baseDirectory.value / "package" / distFile.getName
  IO.move(distFile, output)
  output
}