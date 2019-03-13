name := "HCDN"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.0.1" % "provided",
  "org.apache.spark" %% "spark-mllib" % "2.0.1" % "provided",
  "com.github.scopt" %% "scopt" % "3.5.0"
)

assemblyJarName in assembly := "HCDNPreprocessing.jar"

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))
runMain in Compile <<= Defaults.runMainTask(fullClasspath in Compile, runner in (Compile, run))

