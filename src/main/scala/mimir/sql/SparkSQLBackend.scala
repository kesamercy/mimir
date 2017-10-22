package mimir.sql

import java.sql._

import mimir.Database
import mimir.Methods
import mimir.algebra._
import mimir.util.{JDBCUtils, TimeUtils}
import mimir.sql.sparksql._
import mimir.sql.sqlite.SpecializeForSQLite

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import mimir.sql.sparksql.SparkResultSet
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StructField}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime
import com.github.nscala_time.time.Imports._

class SparkSQLBackend(sparkConnection: SparkConnection, val metaDataStore: JDBCBackend = new JDBCBackend("sqlite", "databases/testing.db"))
  extends Backend
{
  //  - sparkConnection is the connection to a database, might be extended in the future to an array to support multiple databases or files.
  //    - Either way it's just a source connection
  //  - metaDataStore is the place where Mimir's meta-data for lenses is stored
  //    - On lens query, the meta-data tables will be pushed into spark and then the query will be performed and a result returned
  //    -

  var spark: org.apache.spark.sql.SparkSession = null
  var inliningAvailable = false
  var db: mimir.Database = null

  val tableSchemas: scala.collection.mutable.Map[String, Seq[StructField]] = mutable.Map()

  def open() = {
    this.synchronized({
      val conf = new SparkConf().set("spark.driver.allowMultipleContexts", "true").setAppName("MimirSparkSQLBackend").setMaster("local[*]")
      spark = SparkSession
        .builder()
        .config(conf)
        .getOrCreate()

      sparkConnection.open()
      metaDataStore.open()

      assert(spark != null)
      assert(metaDataStore != null)

      // register udf's for spark
      SparkSQLCompat.registerFunctions(spark)

    })
  }

  def enableInlining(db: Database): Unit =
  {
      sparksql.VGTermFunctions.register(db, spark)

      inliningAvailable = true
  }

  def close(): Unit = {
    this.synchronized({
      sparkConnection.close()
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

        loadTableIfNotExists("R")

        // will need to detect non-deterministic queries

//        val tableList: Seq[(String,String)] = JDBCUtils.getTablesFromOperator(sel,this)
//        tableList.foreach((x) => {
//          loadTableIfNotExists(x._1.toUpperCase())
//        })

      } catch {
        case e: SQLException => println(e.toString+"during\n"+sel)
          throw new SQLException("Error in "+sel, e)
      }
    })

    try {
      val start = DateTime.now
      val df = spark.sql(sel)
      df.show()
      val end = DateTime.now
      println(s"SPARK SQL took: ${(start to end).millis} ms")
      val ret = new SparkResultSet(df)
      ret
    } catch {
      case e: SQLException => println(e.toString+"during\n"+sel)
        throw new SQLException("Error in "+sel, e)
    }
  }
  def execute(sel: String, args: Seq[PrimitiveValue]): ResultSet =
  {
    this.synchronized({
      var sqlStr = sel
      args.map(arg => {
        sqlStr = sqlStr.replaceFirst("\\?", getArg(arg))
        ""
      })
      execute(sqlStr)
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

  def fastUpdateBatch(stmt: String, argArray: TraversableOnce[Seq[PrimitiveValue]]): Unit =
  {
    ???
  }

  def refreshTableSchema(): Unit = {
    this.synchronized({
      if (spark == null) {
        throw new SQLException("Trying to use unopened connection!")
      }
      val tables = getAllTables()
      tables.foreach((table) => {
        tableSchemas.get(table.toUpperCase()) match {
          case Some(_) => // do nothing, the table is there, might need to change this to support updates
          case None =>   // need to add the table to main schema
            tableSchemas += table.toUpperCase -> spark.table(table.toUpperCase()).schema.fields.toSeq
        }
      })
    })
  }

  def getTableSchema(table: String): Option[Seq[(String, Type)]] =
  {
    this.synchronized({
      if(spark == null) {
        throw new SQLException("Trying to use unopened connection!")
      }

      sparkConnection.asInstanceOf[sqliteSparkConnection].sqliteBackend.getTableSchema(table)
//      loadTableIfNotExists(table.toUpperCase())
//
//      tableSchemas.get(table.toUpperCase) match {
//        case Some(x: Seq[StructField]) => Some(convertToSchema(x))
//        case None => None
//        }
    })
  }

  override def getView(name: String, table: String): Option[Seq[Seq[PrimitiveValue]]] = {
    loadTableIfNotExists(table.toUpperCase()) // this will probably go away
    val n = name.toUpperCase()
    // should change to use the metaDataBackend
    // will need a result set to df function possibly
    val df = spark.sql(s"SELECT query FROM $table WHERE name = '$n'")
    if(df.count() == 0)
      None
    else
      Some(JDBCUtils.extractAllRows(new SparkResultSet(df)).flush)
  }

  def convertToSchema(sparkSchema: Seq[StructField]): Seq[(String, Type)] = {
    sparkSchema.map((s:StructField) => Tuple2(s.name.toUpperCase(),sparkTypesToMimirTypes(s.dataType)))
  }

  def sparkTypesToMimirTypes(dataType: DataType): Type = {
    dataType match {
      case IntegerType => TInt()
      case LongType => TFloat()
      case _ => TString()
    }
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

  def loadTable(table: String): Boolean = {
    sparkConnection.loadTable(spark,table.toUpperCase())
    tableSchemas.contains(table.toUpperCase())
  }

  def loadTableIfNotExists(table: String): Boolean = {
    val tableInSpark = tableSchemas.contains(table.toUpperCase())
    var b = false
    if(!tableInSpark){
      // table isn't in spark so try and load table
      b = loadTable(table.toUpperCase())
    }
    refreshTableSchema()
    b
  }

  def canHandleVGTerms(): Boolean = inliningAvailable

  def specializeQuery(q: Operator): Operator = {
    if( inliningAvailable )
        VGTermFunctions.specialize(SpecializeForSQLite(q, db))
     else
        q
  }

  def specializeQuery(q: Operator,d: Database): Operator = {
    if( inliningAvailable ) {
      VGTermFunctions.specialize(mimir.sql.sqlite.SpecializeForSQLite(q, d))
    }
    else
      q
  }

  def setDB(newDB: Database): Unit = {
    db = newDB
  }

  def listTablesQuery: Operator =
  {
    ???
  }
  def listAttrsQuery: Operator =
  {
    ???
  }

  override def selectInto(table: String, query: String): Unit = ???

  override def invalidateCache(): Unit = ???

  override def rowIdType: Type = TRowId()

  override def dateType: Type = TDate()
}