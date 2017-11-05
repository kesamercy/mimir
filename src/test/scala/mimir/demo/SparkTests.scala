package mimir.demo

import mimir.exec.{OutputFormat, DefaultOutputFormat}
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

      loadTable("R")
      update("CREATE LENS TEST AS SELECT * FROM R WITH MISSING_VALUE('C')")

      query("SELECT * FROM TEST"){output.print(_)}
      query("SELECT SUM(C) FROM R"){output.print(_)}

      true
    }

  }
}
