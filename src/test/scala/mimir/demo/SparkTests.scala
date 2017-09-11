package mimir.demo

import java.io.File

import org.specs2.matcher.FileMatchers
import mimir.test._
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

object SparkTests extends SQLTestSpecification("databases/debug",Map("jdbc" -> "spark","reset" -> "NO"))
  with FileMatchers
{

  // The demo spec uses cumulative tests --- Each stage depends on the stages that
  // precede it.  The 'sequential' keyword below is necessary to prevent Specs2 from
  // automatically parallelizing testing.
  sequential


  "The Basic Demo" should {
    "Be able to open the database" >> {
      db // force the DB to be loaded
      dbFile must beAFile
    }

    "Load Files" >> {
      //"test/data/ratings1.csv"
/*
      val logFile = "test/data/ratings1.csv" // Should be some file on your system
      val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
      val sc = new SparkContext(conf)
      val logData = sc.textFile(logFile, 2).cache()
      val numAs = logData.filter(line => line.contains("4")).count()
      val numBs = logData.filter(line => line.contains("b")).count()
      println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
*/
/*
      val logFile = "test/data/ratings1.csv" // Should be some file on your system
      val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
      val sc = new SparkContext(conf)
      val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
      
      spark.read.csv("test/data/ratings1.csv").show()
*/

      true
    }

  }
}
