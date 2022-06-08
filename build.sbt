name := "GraceQL"
version := "0.1.0"
scalaVersion := "3.1.1"

libraryDependencies ++= Seq(
    // "org.scala-lang" %% "scala3-staging" % scalaVersion.value,
    "org.scalatest" %% "scalatest" % "3.2.9" % Test,
    "mysql" % "mysql-connector-java" % "8.0.+" % Test,
    "org.postgresql" % "postgresql" % "42.2.+" %Test,
    "org.scala-lang.modules" %% "scala-parser-combinators" % "2.1.+"
) 

scalacOptions ++= Seq(
    // "-Yretain-trees"
)
