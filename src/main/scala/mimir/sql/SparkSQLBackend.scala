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
import org.apache.spark.sql.types.{DataType, LongType, StructField, IntegerType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

class SparkSQLBackend(sparkConnection: SparkConnection)
  extends Backend
{

  var spark: org.apache.spark.sql.SparkSession = null
  var inliningAvailable = false
  var db: mimir.Database = null

  val tableSchemas: scala.collection.mutable.Map[String, Seq[StructField]] = mutable.Map()

  def open() = {
    this.synchronized({
      val conf = new SparkConf().setAppName("MimirSparkSQLBackend").setMaster("local[2]")
      spark = SparkSession
        .builder()
        .config(conf)
        .getOrCreate()

      sparkConnection.open()
      assert(spark != null)

      // register udf's
      SparkSQLCompat.registerFunctions(spark)

      // check the backend for view tables and try to load them
//      val viewTableList = ListBuffer("MIMIR_VIEWS")
//      viewTableList.foreach(loadTable(_))
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

        val tableList: Seq[(String,String)] = JDBCUtils.getTablesFromOperator(sel,this)
        tableList.foreach((x) => {
          tableSchemas.get(x._1.toUpperCase) match {
            case Some(t) => // do nothing since the table is already loaded, maybe add dirty bit for updates here
            case None =>
              sparkConnection.loadTable(spark,x._1,x._2)
              tableSchemas += x._1.toUpperCase -> spark.table(x._1).schema.fields.toSeq
          }
        })

      } catch {
        case e: SQLException => println(e.toString+"during\n"+sel)
          throw new SQLException("Error in "+sel, e)
      }
    })

    try {
      // convert to operator
      //        val oper = sql.convert(sel)
      // pull operator apart so that it is split into plain select (no agg) and agg (to be done in spark)
      //        val plainSelect: Seq[String] = optimize(oper,spark)
      //        val sparkSelect: Seq[String] = optimize(oper,sparkAgg)
      // pass plain to jdbc to get Data Frames
      //        val plainDFSet: Seq[DataFrame] =

      val df = spark.sql(sel)
//      df.show()
      new SparkResultSet(df)
    } catch {
      case e: SQLException => println(e.toString+"during\n"+sel)
        throw new SQLException("Error in "+sel, e)
    }
  }
  def execute(sel: String, args: Seq[PrimitiveValue]): ResultSet =
  {
    var sqlStr = sel
    args.map(arg => {
      sqlStr = sqlStr.replaceFirst("\\?",getArg(arg))
      ""
    })
    execute(sqlStr)
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

      tableSchemas.get(table.toUpperCase) match {
        case Some(x: Seq[StructField]) => Some(convertToSchema(x))
        case None =>
          var tables: Seq[String] = this.getAllTables().map { (x) => x.toUpperCase }

          if (!tables.contains(table.toUpperCase)) {
            sparkConnection.loadTable(spark, table, table) // attempt to load the table
            tables = this.getAllTables().map { (x) => x.toUpperCase }
          }

          if (!tables.contains(table.toUpperCase))
            return None


          tableSchemas += table.toUpperCase -> spark.table(table).schema.fields.toSeq
          // add the new table and schema to tableSchema list

          tableSchemas.get(table) match {
            case Some(sch) =>
              return Some(convertToSchema(sch))
            case None => return None // redundant
          }
        }
    })
  }

  override def getView(name: String, table: String): Option[Seq[Seq[PrimitiveValue]]] = {
    tableSchemas.get(table) match {
      case Some(s) =>
      case None =>
        loadTable(table)
    }
    val n = name.toUpperCase()
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
    sparkConnection.loadTable(spark,table)
    spark.catalog.tableExists(table)
  }

  def checkForTable(table: String): Boolean = {
    val tableInSpark = spark.catalog.tableExists(table)
    if(!tableInSpark){
      // table isn't in spark so try and load table
      loadTable(table)
    }
    spark.catalog.tableExists(table)
  }

  def canHandleVGTerms(): Boolean = inliningAvailable

  def specializeQuery(q: Operator): Operator = {
    if( inliningAvailable )
        VGTermFunctions.specialize(mimir.sql.sqlite.SpecializeForSQLite(q))
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

}