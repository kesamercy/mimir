package mimir.sql

import java.sql._

import mimir.Database
import mimir.Methods
import mimir.algebra._
import mimir.ml.spark.SparkML
import mimir.util.JDBCUtils
import mimir.sql.sparksql.{SparkResultSet, _}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StructField}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

class SparkSQLBackend(sparkConnection: SparkConnection, metaDataStore: JDBCBackend = new JDBCBackend("sqlite", "databases/mimirLensDB.db"))
  extends Backend
{
  //  - sparkConnection is the connection to a database, might be extended in the future to an array to support multiple databases or files.
  //    - Either way it's just a source connection
  //  - metaDataStore is the place where Mimir's meta-data for lenses is stored
  //    - On lens query, the meta-data tables will be pushed into spark and then the query will be performed and a result returned
  //    -
  //  - maintain list of tables that are used as views, this is important for loading tables to spark

  // this is a black list of tables that spark will not perform queries over, this includes updates and queries, they will instead be directed to
  // the metaDataStore

  val blackListTables: List[String] = List("MIMIR_VIEWS","MIMIR_MODELS","MIMIR_MODEL_OWNERS","MIMIR_LENSES","MIMIR_ADAPTIVE_SCHEMAS")

  var spark: org.apache.spark.sql.SparkSession = null
  var inliningAvailable = false
  var db: mimir.Database = null

  val tableSchemas: scala.collection.mutable.Map[String, Seq[StructField]] = mutable.Map()

  def open() = {
    this.synchronized({
      val conf = new SparkConf().setAppName("MimirSparkSQLBackend").setMaster("local[*]")
      spark = SparkSession
        .builder()
        .config(conf)
        .getOrCreate()

      sparkConnection.open()
      metaDataStore.open()

      assert(spark != null)
      assert(metaDataStore != null)

      SparkML.sc = Some(spark.sparkContext)

      // register udf's for spark
      SparkSQLCompat.registerFunctions(spark)

    })
  }

  def enableInlining(db: Database): Unit =
  {
      //sparksql.VGTermFunctions.register(db, spark)
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
    var cantLoad:Boolean = false // if for some reason one or more table can't be loaded
    var isMetaDataQuery:Boolean = false // to check if it's a query on some meta-data table in the backend
    val lensList: ListBuffer[String] = ListBuffer[String]() // list of lenses in the query

    this.synchronized({
      try {
        if(spark == null) {
          throw new SQLException("Trying to use unopened connection!")
        }

        val tableList: Seq[(String,String)] = JDBCUtils.getTablesFromOperator(sel,this)
        val tList:Seq[String] = tableList.map((t)=> t._1)

        isMetaDataQuery = !tList.intersect(blackListTables).isEmpty

        tableList.foreach((x) => {
          if(isView(x._1,"MIMIR_VIEWS"))
            lensList += x._1
          if(!loadTableIfNotExists(x._1.toUpperCase()))
            cantLoad = true // set to true because one or more table can't be loaded, this might be a backend query then
        })

      } catch {
        case e: SQLException => println(e.toString+"during\n"+sel)
          throw new SQLException("Error in "+sel, e)
      }
    })

    try {
      if(lensList.nonEmpty){
        // is a non-deterministic query
        // First need to get and load all the tables for
        metaDataStore.execute(sel)
      }
      else if(isMetaDataQuery){
        // is a query on the backend
        metaDataStore.execute(sel)
      }
      else if(cantLoad){
        // can't load one or more table so attempt to perform backend query
        metaDataStore.execute(sel)
      }
      else {
        // regular spark query with tables loaded
        val df = spark.sql(sel)
        val test: ((Any,Any) => Int) = {(i,j) => j.asInstanceOf[Int]+i.asInstanceOf[Int]}
        val testUDF = org.apache.spark.sql.functions.udf(test)
        df.withColumn("D",testUDF(org.apache.spark.sql.functions.lit(100),df("C"))).show()
        // df.show()
        new SparkResultSet(df)
      }
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

  // For right now update can only be called on the metaDataBackend
  def update(upd: String): Unit =
  {
    this.synchronized({
      if(spark == null) {
        throw new SQLException("Trying to use unopened connection!")
      }
      // update the metaDataBackend instead of spark, could check here to decided which one to update in the future
      metaDataStore.update(upd)
    })
  }

  def update(upd: TraversableOnce[String]): Unit =
  {
    this.synchronized({
      if(spark == null) {
        throw new SQLException("Trying to use unopened connection!")
      }
      metaDataStore.update(upd)
    })
  }

  def update(upd: String, args: Seq[PrimitiveValue]): Unit =
  {
    this.synchronized({
      if(spark == null) {
        throw new SQLException("Trying to use unopened connection!")
      }
      metaDataStore.update(upd,args)
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

      if(blackListTables.contains(table.toUpperCase()))
        return metaDataStore.getTableSchema(table.toUpperCase)

      val tableLoaded: Boolean = loadTableIfNotExists(table.toUpperCase())

      if(tableLoaded) {
        // The table was found in one of the spark connections
        tableSchemas.get(table.toUpperCase) match {
          case Some(x: Seq[StructField]) => Some(convertToSchema(x))
          case None => None
        }
      }
      else {
        // Spark is not aware of the table, so it's either a metaData table or the table does not exist
        metaDataStore.getTableSchema(table.toUpperCase)
      }
    })
  }

  override def getView(name: String, table: String): Option[Seq[Seq[PrimitiveValue]]] = {
    metaDataStore.getView(name,table)
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

  def isView(name: String, table: String): Boolean = {
    metaDataStore.getView(name,table) match {
      case Some(res) => true
      case None => false
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
    try {
      sparkConnection.loadTable(spark, table.toUpperCase())
      true
    }
    catch{
      case _ => false
    }
  }

  def loadTableIfNotExists(table: String): Boolean = {
    val tableInSpark = tableSchemas.contains(table.toUpperCase())
    var tableLoaded = false
    if(!tableInSpark){
      // table isn't in spark so try and load table
      tableLoaded = loadTable(table.toUpperCase())
      refreshTableSchema()
    }
    else
      tableLoaded = true
    tableLoaded
  }

  def canHandleVGTerms(): Boolean = inliningAvailable

  def specializeQuery(q: Operator): Operator = {
//    if( inliningAvailable )
     if( true)
      VGTermFunctions.specialize(mimir.sql.sqlite.SpecializeForSQLite(q,db))
     else
      q
  }

  def specializeQuery(q: Operator,d: Database): Operator = {
    if( inliningAvailable )
      VGTermFunctions.specialize(mimir.sql.sqlite.SpecializeForSQLite(q,d))
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

  def fastUpdateBatch(stmt: String, argArray: TraversableOnce[Seq[PrimitiveValue]]): Unit =
  {
    ???
  }

  override def selectInto(table: String, query: String): Unit = ???

  override def invalidateCache(): Unit = ???

  override def rowIdType: Type = TRowId()

  override def dateType: Type = TDate()
}