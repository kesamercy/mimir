package mimir.demo

import mimir.exec.{DefaultOutputFormat, OutputFormat}
import org.specs2.matcher.FileMatchers
import mimir.test._
import mimir.util.TimeUtils


object SparkTests extends SQLTestSpecification("databases/debug",Map("jdbc" -> "spark","reset" -> "NO", "inline" -> "YES"))
  with FileMatchers
{

  // The demo spec uses cumulative tests --- Each stage depends on the stages that
  // precede it.  The 'sequential' keyword below is necessary to prevent Specs2 from
  // automatically parallelizing testing.
  sequential

  var output: OutputFormat = DefaultOutputFormat

  def time[A](description: String, op: () => A): A = {
    val t:StringBuilder = new StringBuilder()
    TimeUtils.monitor(description, op, println(_))
  }


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

      //      val res1: ResultIterator = query("SELECT A FROM R")
      //      val res2: ResultIterator = query("SELECT * FROM R")
      /*
            time("AVERAGE 2M rows",() => {
              val res3: ResultIterator = query("SELECT AVG(bearing) FROM MTA_RAW")
            })
      */

//      update("CREATE LENS TEST21 as SELECT * FROM R WITH MISSING_VALUE('C')")
//      query("SELECT * FROM R ORDER BY RANDOM()"){output.print(_)}

//      time("AVERAGE 2M rows",() => {
//        val res3 = query("SELECT AVG(bearing) FROM MTA_RAW"){output.print(_)}
//      })

//      time("Simple UDF Test",() => {
//        val res3 = query("SELECT SUM(SIMPLETEST(bearing)) FROM MTA_RAW"){output.print(_)}
//      })
      //      val res3: ResultIterator = query("SELECT * FROM R , CITYRAW")
      //      val res4: ResultIterator = query("SELECT * FROM CITYRAW")
      query("SELECT SUM(C) FROM TEST21"){output.print(_)}
//      while(true){}

      println("done")
      true
    }

  }

}