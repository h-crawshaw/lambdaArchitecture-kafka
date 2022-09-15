ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "app"
  )

val sparkVersion = "3.2.0"
val configVersion = "1.4.2"
val awscalaVersion = "0.9.+"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.github.seratch" %% "awscala-s3" % awscalaVersion)

libraryDependencies += "com.typesafe" % "config" % configVersion