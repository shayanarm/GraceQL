name := "GraceQL"
version := "1.0"
scalaVersion := "3.1.1"

libraryDependencies ++= Seq(
    "org.scala-lang" %% "scala3-staging" % scalaVersion.value
) 

scalacOptions ++= Seq(
    "-Yretain-trees"
)
