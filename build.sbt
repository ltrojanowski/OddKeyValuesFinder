import sbt.Keys._
import sbtide.Keys.idePackagePrefix

lazy val root = (project in file(".")).settings(
  inThisBuild(
    List(
      organization := "com.ltrojanowski",
      scalaVersion := "2.12.13"
    )
  ),
  version := "0.1",
  name := "OddKeyValuesFinder",
  idePackagePrefix := Some("com.ltrojanowski.oddkeyvaluesfinder"),
  libraryDependencies := Seq(
    "org.apache.spark"  %% "spark-core"   % "3.1.0",
    "org.apache.spark"  %% "spark-sql"    % "3.1.0",
    "org.apache.hadoop" % "hadoop-common" % "3.3.0",
    "org.apache.hadoop" % "hadoop-client" % "3.3.0",
    "org.apache.hadoop" % "hadoop-aws"    % "3.3.0"
  )
)
