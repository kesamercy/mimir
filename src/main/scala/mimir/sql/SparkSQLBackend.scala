package mimir.sql

import java.sql._

import mimir.Database
import mimir.algebra.{Cmp, _}
import mimir.ctables.vgterm.BestGuess
import mimir.ml.spark.SparkML
import mimir.util.JDBCUtils
import mimir.sql.sparksql.{SparkResultSet, _}
import org.apache.spark.ml.Model

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import mimir.models.{Model, SimpleSparkClassifierModel}
import mimir.parser.MimirJSqlParser
import org.apache.spark.ml.linalg.Vectors

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
      val conf = new SparkConf().setAppName("MimirSparkSQLBackend")
      spark = SparkSession
        .builder()
        .config(conf)
        .getOrCreate()

      sparkConnection.open()
      metaDataStore.open()

      assert(spark != null)
      assert(metaDataStore != null)

      SparkML.sc = Some(spark.sparkContext)
      SparkML.sparkSession = Some(spark)

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

  def execute(oper: Operator): ResultSet = {
    var cantLoad:Boolean = false // if for some reason one or more table can't be loaded
    var isMetaDataQuery:Boolean = false // to check if it's a query on some meta-data table in the backend
    val lensList: ListBuffer[String] = ListBuffer[String]() // list of lenses in the query

    this.synchronized({
      try {
        if(spark == null) {
          throw new SQLException("Trying to use unopened connection!")
        }

        val tableList: Seq[(String,String)] = JDBCUtils.getTablesFromOperator(oper)
        val tList:Seq[String] = tableList.map((t)=> t._1)

        isMetaDataQuery = !tList.intersect(blackListTables).isEmpty

        tableList.foreach((x) => {
          if(isView(x._1,"MIMIR_VIEWS"))
            lensList += x._1
          if(!loadTableIfNotExists(x._1.toUpperCase()))
            cantLoad = true // set to true because one or more table can't be loaded, this might be a backend query then
        })

      } catch {
        case e: SQLException => println(e.toString+"during\n"+oper.toString)
          throw new SQLException("Error in "+oper.toString, e)
      }
    })

    try {
      if(lensList.nonEmpty){
        // is a non-deterministic query
        // First need to get and load all the tables for
        metaDataStore.execute(db.ra.convert(oper))
      }
      else if(isMetaDataQuery){
        // is a query on the backend
        metaDataStore.execute(db.ra.convert(oper))
      }
      else if(cantLoad){
        // can't load one or more table so attempt to perform backend query
        metaDataStore.execute(db.ra.convert(oper))
      }
      else {
        // regular spark query with tables loaded
        val df: DataFrame = OperatorToDF(oper)
//        df.show()
        new SparkResultSet(df)
      }
    } catch {
      case e: SQLException => println(e.toString+"during\n"+oper.toString)
        throw new SQLException("Error in "+oper.toString, e)
    }
  }

  def execute(sel: String): ResultSet =
  {
    var cantLoad:Boolean = false // if for some reason one or more table can't be loaded
  var isMetaDataQuery:Boolean = false // to check if it's a query on some meta-data table in the backend
  val lensList: ListBuffer[String] = ListBuffer[String]() // list of lenses in the query
    //SELECT SUM(CASE WHEN SUBQ_A.C IS NULL THEN BEST_GUESS_VGTERM('TEST19:SPARK:C', 0, SUBQ_A.MIMIR_ROWID, SUBQ_A.A, SUBQ_A.B, SUBQ_A.C, SUBQ_A.ROWID) ELSE SUBQ_A.C END) AS SUM, GROUP_AND(SUBQ_A.C IS NOT NULL) AS MIMIR_COL_DET_SUM, GROUP_OR((1 = 1)) AS MIMIR_ROW_DET FROM (SELECT R.A AS A, R.B AS B, R.C AS C, R.ROWID AS ROWID, R.ROWID AS MIMIR_ROWID FROM R AS R) SUBQ_A

    this.synchronized({

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
          /*
                  def rowUDF(model: mimir.models.Model) = udf((r: Row) => {
                    val m: SimpleSparkClassifierModel = model.asInstanceOf[SimpleSparkClassifierModel]
                    val A: Int = r.get(0).asInstanceOf[Int]
                    val B: Int = r.get(1).asInstanceOf[Int]
                    val C: Int = r.get(2).asInstanceOf[Int]
                    val rowID = r.get(3)
                    val res = m.classify(RowIdPrimitive(rowID.toString),Seq[PrimitiveValue](IntPrimitive(A),IntPrimitive(B),NullPrimitive()))

                    r.get(0).asInstanceOf[Int] + r.get(1).asInstanceOf[Int]
                  })
                  val mod = db.models.get("TEST21:SPARK:C")

                  val df1 = spark.sqlContext.table("R").select(
                    col("A"),
                    col("B"),
                    //when(col("C").isNull, myUDF(col("B"))).otherwise(col("C")),
                    when(col("C").isNull, rowUDF(mod)(struct(col("A"), col("B"), col("C"), col("ROWID")))).otherwise(col("C")).alias("C"),
                    col("ROWID")).agg(sum("C"))

                  df1.show()
          */

          // regular spark query with tables loaded
          /*
          val test: ((Any,Any) => Int) = {(i,j) => j.asInstanceOf[Int]+i.asInstanceOf[Int]}
          val testUDF = org.apache.spark.sql.functions.udf(test)
          df.withColumn("D",testUDF(org.apache.spark.sql.functions.lit(100),df("C"))).show()
          */
          //.replace(", GROUP_AND(SUBQ_A.C IS NOT NULL) AS MIMIR_COL_DET_SUM, GROUP_OR((1 = 1)) AS MIMIR_ROW_DET FROM","")
          var stmt: net.sf.jsqlparser.statement.Statement = null
          try {
            val parser = new MimirJSqlParser(new java.io.StringReader(sel))
            stmt = parser.Statement()
          }
          catch {
            case _ =>
              val newsel = sel.replace(" IS NOT NULL","").replace("(1 = 1)","1").replace("(0 = 0)","0")
              val parser = new MimirJSqlParser(new java.io.StringReader(newsel))
              stmt = parser.Statement()
          }
          stmt match {
            case s:  net.sf.jsqlparser.statement.select.Select  =>
              execute(db.sql.convert(s))
            //      case expl: Explain    => handleExplain(expl)
            //      case pragma: Pragma   => handlePragma(pragma)
            //      case analyze: Analyze => handleAnalyze(analyze)
            case _                => throw new SQLException("This can not currently be processed by JDBCUtils.getTablesFromOperator")
          }
        }
      } catch {
        case e: SQLException => println(e.toString+"during\n"+sel)
          throw new SQLException("Error in "+sel, e)
      }

    })
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
      case DoubleType => TFloat()
      case FloatType => TFloat()
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
    if( true) {
      val ret = VGTermFunctions.specialize(mimir.sql.sqlite.SpecializeForSQLite(q, db))
//      val test = OperatorToDF(ret)
      ret
    }
    else
      q
  }

  def specializeQuery(q: Operator,d: Database): Operator = {
    if( inliningAvailable ) {
      val ret = VGTermFunctions.specialize(mimir.sql.sqlite.SpecializeForSQLite(q, d))
//      val test = OperatorToDF(ret)
//      test.show()
      ret
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

  def fastUpdateBatch(stmt: String, argArray: TraversableOnce[Seq[PrimitiveValue]]): Unit =
  {
    ???
  }

  override def selectInto(table: String, query: String): Unit = ???

  override def invalidateCache(): Unit = ???

  override def rowIdType: Type = TRowId()

  override def dateType: Type = TDate()

  /*
  * Converts a mimir operator to a set of spark operators that return a dataframe
  * This will assume the operator is already optimized for spark and just preforms the translation
  * The main purpose of this instead of using SparkSQL is that this will give us the ability to pass models(objects) directly to the workers for lens queries
  * Additionally this will allow us to use the struct type for multiple column returns as well as varying column input sizes
  */

  def OperatorToDF(oper: Operator): DataFrame = {

    // Ok so here's the plan, only From Tables are the base case, we're going to recursively call this function until we finally get to union/ plain select...
    // you may ask, "how is this efficent, won't it be trying to preform the operations at every return call???" To this I say, I hope not. Luckily DataFrames are evaluated lazily. So nothing should happen until you call show or collect on it.
    // If this is not the case this will have been deleted and you will not have read my rhetoric, so I got that going for me

    oper match { // oh boy this is going to be a large pattern match, glhf
      case u: Union => {
        val dfList: Seq[DataFrame] = OperatorUtils.extractUnionClauses(u).map(OperatorToDF(_))
        ???
      }
      case Join(lhs: Operator, rhs: Operator) => {
        OperatorToDF(lhs).join(OperatorToDF(rhs))
      }
      case Project(cols: Seq[ProjectArg], src) => {
        val df = OperatorToDF(src)
        val retC: Seq[Column] = cols.map((c) => {
          ExpressionToSparkColumn(c.expression).alias(c.name)

        })
        df.select(retC: _*) // Needs to handle functions
      }
      case Aggregate(gbCols: Seq[Var], aggCols: Seq[AggFunction], src: Operator) => {
        val aggList: Seq[Column] = aggCols.map((a) => {
          val e: Expression = a.args(0) // only supports one arg for now
          val c: Column = ExpressionToSparkColumn(e)
          a.function match {
            case "SUM" => sum(c) as a.alias
            case "GROUP_AND" => min(c) as a.alias   // hack
            case "GROUP_OR" => max(c) as a.alias  // hack
          }
        })
        if(gbCols.isEmpty) { // no groupBY
          //          OperatorToDF(src).agg(aggList.head,aggList: _*)
          OperatorToDF(src).agg(aggList.head,aggList.tail: _*)
        }
        else
          ???
        //OperatorToDF(src).groupBy().agg(sum("A") as "A")
      }
      /*      case AllTarget() => { // I assume SELECT *
              ???
            }
            case ProjectTarget(cols) => {
              ???
            }
      */
      case Select(cond: Expression, src: Operator) => { // WHERE clause
        val c: Column = ExpressionToSparkColumn(cond)
        OperatorToDF(src).where(c)
      }
      case Limit(offset: Long, maybeCount: Option[Long], src: Operator) => {
        // offset: Igore first _ rows, maybeCount is the limit number, src is the query
        if(offset > 0)
          throw new Exception("SPARK CURRENTLY DOESN'T SUPPORT OFFSET")
        maybeCount match {
          case Some(count) => OperatorToDF(src).limit(count.toInt)
          case None => OperatorToDF(src)
        }
      }
      // Just convert the query and add the alias
      case View(name: String, query: Operator, annotations) => {
        OperatorToDF(query).alias(name)
      }
      case AdaptiveView(schema, name: String, query: Operator, annotations) => {
        ???
      }
      // Base case, maybe wrap a selection around this to tidy things up
      case Table(name: String, alias: String, tgtSch: Seq[(String,Type)], metadata: Seq[(String,Expression,Type)]) => {
        spark.sqlContext.table(name).alias(alias) // can select out the target schema too if needed
      }
      case _ => {
        ??? // whelp you encountered a case I didn't get to or didn't think of, good luck
      }
    }
  }

  // For selection a column is expected, this will be the boolean result
  def ExpressionToSparkColumn(e: Expression): Column = {
    e match {
      case Arithmetic(op: Arith.Op, lhs: Expression, rhs: Expression) =>
        CombineEnum(op, ExpressionToSparkColumn(lhs), ExpressionToSparkColumn(rhs))

      case Comparison(op: Cmp.Op, lhs, rhs) =>
        CombineEnum(op, ExpressionToSparkColumn(lhs), ExpressionToSparkColumn(rhs))

      case Conditional(condition: Expression, thenClause: Expression, elseClause: Expression) =>
        when(ExpressionToSparkColumn(condition), ExpressionToSparkColumn(thenClause)).otherwise(ExpressionToSparkColumn(elseClause))

      case Function("BEST_GUESS_VGTERM", params: Seq[Expression]) => // best guess vgterm for spark models
        val model: mimir.models.Model = db.models.get(params(0).toString.replace("\'", ""))
        val idx: Int = (params(1).asInstanceOf[IntPrimitive]).asInt
        val cols: Seq[Expression] = params.slice(3, params.size) // 2 is mimir_rowid
      val columns: Seq[Column] = cols.map((c) => {
        ExpressionToSparkColumn(c)
      })
        mimir.sql.sparksql.VGTermFunctions.rowUDF(model)(struct(columns: _*))

      case IsNullExpression(child: Expression) => ExpressionToSparkColumn(child).isNull
      case IsNotNullExpression(child: Expression) =>
        ExpressionToSparkColumn(child).isNotNull
      case Not(e: Expression) => {
        e match {
          case IsNullExpression(c) => ExpressionToSparkColumn(IsNotNullExpression(c))
          case _ => ??? // just call on the opposite
        }
      }
      // base case items
      case Vector(cols) =>
        struct(cols.map(col(_)): _*)
      case Var(c) =>
        if(c.toString.contains("ROWID")) // c.toString.equals("MIMIR_ROWID") ||
          //lit(0)
          monotonically_increasing_id()
        else
          col(c)
      case IntPrimitive(i) => lit(i)
      case FloatPrimitive(i) => lit(i)
      case StringPrimitive(s) => lit(s)
      case BoolPrimitive(b) => lit(b)
      // unknown
      case _ => ???
    }
  }

  // for determining which operator to use, there is not direct conversion I know of so this is the hack for that
  def CombineEnum(operation: Any, lhs: Column, rhs: Column): Column = {
    operation match {
      // arithmetic
      case Arith.And => lhs.and(rhs)
      case Arith.Or => lhs.or(rhs)
      case Arith.BitAnd => lhs.bitwiseAND(rhs)
      case Arith.BitOr => lhs.bitwiseOR(rhs)
      case Arith.Div => lhs.divide(rhs)
      case Arith.Mult => lhs.multiply(rhs)
      case Arith.Sub => lhs.minus(rhs)
      case Arith.Add => lhs.+(rhs)
      // logical
      case Cmp.Eq => lhs.<=>(rhs)
      case Cmp.Gt => lhs.>(rhs)
      case Cmp.Lt => lhs.<(rhs)
      case Cmp.Gte => lhs.>=(rhs)
      case Cmp.Lte => lhs.<=(rhs)
      case Cmp.Neq => lhs.notEqual(rhs)
      case Cmp.Like => lhs.like(rhs.toString())

      case _ => ??? // not implimented
    }
  }
}