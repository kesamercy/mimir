package mimir.sql;

import java.sql._

import mimir.Database
import mimir.Methods
import mimir.algebra._
import mimir.util.JDBCUtils
import mimir.sql.sparksql._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import mimir.sql.sparksql.SparkResultSet

class SparkSQLBackend() 
  extends Backend
{
 
 var spark: org.apache.spark.sql.SparkSession = null
 var inliningAvailable = false;

  
  val tableSchemas: scala.collection.mutable.Map[String, Seq[(String, Type)]] = mutable.Map()

  def open() = {
    this.synchronized({
      spark = org.apache.spark.sql.SparkSession.builder
        .master("local")
        .appName("MimirSparkSQLBackend")
        .enableHiveSupport()
        .getOrCreate;

      assert(spark != null)
      
    })
  }

  def enableInlining(db: Database): Unit =
  {
      sparksql.VGTermFunctions.register(db, spark)
      inliningAvailable = true
  }

  def close(): Unit = {
    this.synchronized({
      spark.close()
    })
  }

  def execute(sel: String): ResultSet = 
  {
    this.synchronized({
      try {
        if(spark == null) {
          throw new SQLException("Trying to use unopened connection!")
        }
        val df = spark.sql(sel)
        new SparkResultSet(df)
      } catch { 
        case e: SQLException => println(e.toString+"during\n"+sel)
          throw new SQLException("Error in "+sel, e)
      }
    })
  }
  def execute(sel: String, args: Seq[PrimitiveValue]): ResultSet = 
  {
    this.synchronized({
      try {
        if(spark == null) {
          throw new SQLException("Trying to use unopened connection!")
        }
        var sqlStr = sel
        args.map(arg => {
          sqlStr = sqlStr.replaceFirst("?",getArg(arg))
          ""
        })
        val df = spark.sql(sqlStr)
        new SparkResultSet(df)
      } catch { 
        case e: SQLException => println(e.toString+"during\n"+sel+" <- "+args)
          throw new SQLException("Error", e)
      }
    })
  }
  
  def fixUpdateSqlForSpark(upd: String) : String = {
    upd.replaceAll(",\\s*PRIMARY\\s+KEY\\s*[()a-zA-Z0-9]+", "").replaceAll("\\s+text\\s*(,|[\\s)]+)", " string$1")
  }
  
  def update(upd: String): Unit =
  {
    this.synchronized({
      if(spark == null) {
        throw new SQLException("Trying to use unopened connection!")
      }
      spark.sql(fixUpdateSqlForSpark(upd))
    })
  }

  def update(upd: TraversableOnce[String]): Unit =
  {
    this.synchronized({
      if(spark == null) {
        throw new SQLException("Trying to use unopened connection!")
      }
      upd.foreach( updSql => {
        spark.sql(fixUpdateSqlForSpark(updSql))
      })
    })
  }

  def update(upd: String, args: Seq[PrimitiveValue]): Unit =
  {
    this.synchronized({
      if(spark == null) {
        throw new SQLException("Trying to use unopened connection!")
      }
      var sqlStr = upd
        args.map(arg => {
          sqlStr = sqlStr.replaceFirst("?",getArg(arg))
          ""
        })
       spark.sql(fixUpdateSqlForSpark(sqlStr))
    })
  }

  def fastUpdateBatch(upd: String, argsList: Iterable[Seq[PrimitiveValue]]): Unit =
  {
    this.synchronized({
      if(spark == null) {
        throw new SQLException("Trying to use unopened connection!")
      }
      argsList.foreach( args => {
        var sqlStr = upd
        args.map(arg => {
          sqlStr = sqlStr.replaceFirst("?",getArg(arg))
          ""
        })
       spark.sql(fixUpdateSqlForSpark(sqlStr))
      })
    })
  }
  
  def getTableSchema(table: String): Option[Seq[(String, Type)]] =
  {
    this.synchronized({
      if(spark == null) {
        throw new SQLException("Trying to use unopened connection!")
      }

      tableSchemas.get(table) match {
        case x: Some[_] => x
        case None =>
          val tables = this.getAllTables().map{(x) => x.toUpperCase}
          if(!tables.contains(table.toUpperCase)) return None

          val cols: Option[List[(String, Type)]] = SparkSQLCompat.getTableSchema(spark, table)
            
          cols match { case None => (); case Some(s) => tableSchemas += table -> s }
          cols
      }
    })
  }

  def getArg(arg: PrimitiveValue) : String = {
    arg match {
            case IntPrimitive(i)      => i.toString()
            case FloatPrimitive(f)    => f.toString()
            case StringPrimitive(s)   => s"'$s'"
            case d:DatePrimitive      => s"'$d.asString'"
            case BoolPrimitive(true)  => 1.toString()
            case BoolPrimitive(false) => 0.toString()
            case RowIdPrimitive(r)    => r.toString()
            case NullPrimitive()      => "NULL"
          }
  }
  
  def getAllTables(): Seq[String] = {
    this.synchronized({
      if(spark == null) {
        throw new SQLException("Trying to use unopened connection!")
      }

      val tables = spark.catalog.listTables().collect()
        

      val tableNames = new ListBuffer[String]()

      for(table <- tables) {
        tableNames.append(table.name)
      }

      tableNames.toList
    })
  }

  def canHandleVGTerms(): Boolean = inliningAvailable

  def specializeQuery(q: Operator): Operator = {
    if( inliningAvailable ) 
        VGTermFunctions.specialize(mimir.sql.sqlite.SpecializeForSQLite(q))
     else
        q
  }

  

  def listTablesQuery: Operator = 
  {
    ???
  }
  def listAttrsQuery: Operator = 
  {
    ???
  }
  
}