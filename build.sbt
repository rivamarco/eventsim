organization:= "io.radicalbit"

name := "eventsim"

version := "1.0.0-SNAPSHOT"

resolvers ++= Seq(
  Opts.resolver.mavenLocalFile,
  "Radicalbit Public Releases" at "https://tools.radicalbit.io/artifactory/public-release/",
  "Radicalbit Public Releases" at "https://tools.radicalbit.io/artifactory/public-snapshot/"
)

libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-math3" % "3.5",
  "de.jollyday" % "jollyday" % "0.5.1",
  "org.rogach" %% "scallop" % "3.3.0",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.6.1",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.1",
  "org.apache.kafka" % "kafka_2.10" % "0.8.2.1",
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2",
  "io.radicalbit.nsdb" %% "nsdb-scala-api" % "1.0.0-SNAPSHOT"
)

enablePlugins(JavaServerAppPackaging, SbtNativePackager)

lazy val packageDist   = taskKey[File]("create universal package and move it to package folder")
addCommandAlias("dist", "packageDist")

packageDist := {
  val distFile = (packageBin in Universal).value
  val output   = baseDirectory.value / "package" / distFile.getName
  IO.move(distFile, output)
  output
}