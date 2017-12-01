package mimir.demo

import mimir.algebra.Operator
import mimir.exec.{DefaultOutputFormat, OutputFormat}
import mimir.parser.MimirJSqlParser
import mimir.sql.SparkSQLBackend
import org.specs2.matcher.FileMatchers
import mimir.test._
import mimir.util.TimeUtils
import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.select.Select
import org.apache.spark.sql.DataFrame


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

  def convert(sql: String): Operator = {
    val parser = new MimirJSqlParser(new java.io.StringReader(sql))
    val stmt: Statement = parser.Statement()
    db.sql.convert(stmt.asInstanceOf[Select])
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

      /*
      val oper1 = convert("SELECT * FROM R")
      val df1: DataFrame = db.backend.asInstanceOf[SparkSQLBackend].OperatorToDF(oper1)
      df1.show()

      val oper2 = convert("SELECT A FROM R")
      val df2: DataFrame = db.backend.asInstanceOf[SparkSQLBackend].OperatorToDF(oper2)
      df2.show()

      val oper3 = convert("SELECT A, B FROM R")
      val df3: DataFrame = db.backend.asInstanceOf[SparkSQLBackend].OperatorToDF(oper3)
      df3.show()

      val oper4 = convert("SELECT A, B FROM R WHERE B < 2")
      val df4: DataFrame = db.backend.asInstanceOf[SparkSQLBackend].OperatorToDF(oper4)
      df4.show()

      val oper41 = convert("SELECT A, B FROM R WHERE A < 3 AND B < 3")
      val df41: DataFrame = db.backend.asInstanceOf[SparkSQLBackend].OperatorToDF(oper41)
      df41.show()

      val oper42 = convert("SELECT A, B FROM R WHERE A < B AND B < 3")
      val df42: DataFrame = db.backend.asInstanceOf[SparkSQLBackend].OperatorToDF(oper42)
      df42.show()

      val oper5 = convert("SELECT SUM(A) FROM R")
      val df5: DataFrame = db.backend.asInstanceOf[SparkSQLBackend].OperatorToDF(oper5)
      df5.show()

      val oper6 = convert("SELECT A + 5 FROM R")
      val df6: DataFrame = db.backend.asInstanceOf[SparkSQLBackend].OperatorToDF(oper6)
      df6.show()

      val oper7 = convert("SELECT SUM(CASE WHEN SUBQ_A.C IS NULL THEN BEST_GUESS_VGTERM('TEST23:SPARK:C', 0, SUBQ_A.MIMIR_ROWID, SUBQ_A.A, SUBQ_A.B, SUBQ_A.C, SUBQ_A.ROWID) ELSE SUBQ_A.C END) AS SUM, GROUP_AND(SUBQ_A.C) AS MIMIR_COL_DET_SUM, GROUP_OR(1) AS MIMIR_ROW_DET FROM (SELECT R.A AS A, R.B AS B, R.C AS C, R.ROWID AS ROWID, R.ROWID AS MIMIR_ROWID FROM R AS R) SUBQ_A")
      val df7: DataFrame = db.backend.asInstanceOf[SparkSQLBackend].OperatorToDF(oper7)
      df7.show()

      val oper71 = convert("SELECT CASE WHEN SUBQ_A.C IS NULL THEN BEST_GUESS_VGTERM('TEST23:SPARK:C', 0, SUBQ_A.MIMIR_ROWID, SUBQ_A.A, SUBQ_A.B, SUBQ_A.C, SUBQ_A.ROWID) ELSE SUBQ_A.C END AS SUM FROM (SELECT R.A AS A, R.B AS B, R.C AS C, R.ROWID AS ROWID, R.ROWID AS MIMIR_ROWID FROM R AS R) SUBQ_A")
      val df71: DataFrame = db.backend.asInstanceOf[SparkSQLBackend].OperatorToDF(oper71)
      df71.show()
*/

//      query("SELECT A + 5 FROM R"){output.print(_)}
      query("SELECT * FROM R"){output.print(_)}

      // create lens mv1 as select * from hundred with missing_value('C');
      // select * from mv1;

      val lensName = "TEST40"
      update(s"CREATE LENS $lensName as SELECT * FROM R WITH MISSING_VALUE('C')")
      query(s"SELECT sum(c) FROM $lensName"){output.print(_)}
//      query(s"SELECT C FROM $lensName"){output.print(_)}

/*
      val oper6 = convert("SELECT BEST_GUESS(A) FROM R")
      val df6: DataFrame = db.backend.asInstanceOf[SparkSQLBackend].OperatorToDF(db.backend.specializeQuery(oper6,db))
      df6.show()
*/

//      query("SELECT SUM(C) FROM TEST21"){output.print(_)}
//      while(true){}

      println("done")
      true
    }

  }

}