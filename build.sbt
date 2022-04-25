name := "GraceQL"
version := "1.0"
scalaVersion := "3.1.1"

libraryDependencies ++= Seq(
    "org.scala-lang" %% "scala3-staging" % scalaVersion.value,
    "org.scalatest" %% "scalatest" % "3.2.9" % Test,
) 

scalacOptions ++= Seq(
    "-Yretain-trees"
)
