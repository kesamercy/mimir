package mimir.ml.spark

import mimir.algebra._
import mimir.Database
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.ml.feature.Imputer
import org.apache.spark.rdd.RDD

object SparkML {
  type SparkModel = PipelineModel
  case class SparkModelGeneratorParams(query:Operator, db:Database, predictionCol:String)
  type SparkModelGenerator = SparkModelGeneratorParams => PipelineModel
  var sc: Option[SparkContext] = None
  var sqlCtx : Option[SQLContext] = None
}

abstract class SparkML {
  def getSparkSession() : SparkContext = {
      val conf = new SparkConf().setMaster("local[*]").setAppName("MultiClassClassification")
      SparkML.sc match {
        case None => {
          val sparkCtx = new SparkContext(conf)
          SparkML.sc = Some(sparkCtx)
          sparkCtx
        }
        case Some(session) => session
      }
  }
  
  def getSparkSqlContext() : SQLContext = {
    SparkML.sqlCtx match {
      case None => {
        SparkML.sqlCtx = Some(new SQLContext(getSparkSession()))
        SparkML.sqlCtx.get
      }
      case Some(ctx) => ctx
    }
  }
  
  type ValuePreparer = (PrimitiveValue, Type) => Any
  
  protected def prepareValueTrain(value:PrimitiveValue, t:Type): Any 
  
  protected def prepareValueApply(value:PrimitiveValue, t:Type): Any 
  
  protected def getSparkType(t:Type) : DataType 
  
  type DataFrameTransformer = (DataFrame) => DataFrame
  
  protected def nullValueReplacement(df:DataFrame): DataFrame = {
    import org.apache.spark.sql.functions.mean
    val imputerCols = df.schema.fields.flatMap(col => {
      if(df.filter(df(col.name).isNull).count > 0)
        col.dataType match {
          case IntegerType | LongType | DoubleType | FloatType => Some(col.name)
          case StringType => None
          case _ => None
        }
      else None
    }).toArray
    new Imputer().setInputCols(imputerCols) .setOutputCols(imputerCols).fit(df).transform(df)
  }
  
  def prepareData(query : Operator, db:Database, valuePreparer: ValuePreparer = prepareValueTrain, sparkTyper:Type => DataType = getSparkType) : DataFrame = {

    val schema = db.bestGuessSchema(query).toList
    val sqlContext = getSparkSqlContext()
    sqlContext.createDataFrame(
      getSparkSession().parallelize(
        db.query(query, mimir.exec.mode.BestGuess)(results => {
          results.toList.map(row =>{
            Row((valuePreparer(row.provenance, TString()) +: row.tuple.zip(schema).map(value => valuePreparer(value._1, value._2._2))): _*)
          })
      })), StructType(StructField("rowid", StringType, false) :: schema.map(col => StructField(col._1, sparkTyper(col._2), true))))
  }
  
  def applyModelDB(model : PipelineModel, query : Operator, db:Database, valuePreparer:ValuePreparer = prepareValueApply, sparkTyper:Type => DataType = getSparkType, dfTransformer:Option[DataFrameTransformer] = Some(nullValueReplacement)) : DataFrame = {
    val data = db.query(query, mimir.exec.mode.BestGuess)(results => {
      results.toList.map(row => row.provenance +: row.tuple)
    })
    applyModel(model, ("rowid", TString()) +:db.bestGuessSchema(query), data, valuePreparer, sparkTyper, dfTransformer)
  }
  
  def applyModel( model : PipelineModel, cols:Seq[(String, Type)], testData : List[Seq[PrimitiveValue]], valuePreparer:ValuePreparer = prepareValueApply, sparkTyper:Type => DataType = getSparkType, dfTransformer:Option[DataFrameTransformer] = Some(nullValueReplacement)): DataFrame = {
    val sqlContext = getSparkSqlContext()
    val modDF = dfTransformer.getOrElse((df:DataFrame) => df)
    val str: StructType = StructType(cols.toList.map(col => StructField(col._1, sparkTyper(col._2), true)))
    val t: List[Row] = testData.map( row => {
      Row(row.zip(cols).map(value => valuePreparer(value._1, value._2._2)):_*)
    })
    val d: RDD[Row] = getSparkSession().parallelize(t)
    val df: DataFrame = sqlContext.createDataFrame(d, str)
    df.show()
    val mdf: DataFrame = modDF(df)
    mdf.show()
    mdf.printSchema()
    model.transform(df)
  }
  
  def extractPredictions(model : PipelineModel, predictions:DataFrame, maxPredictions:Int = 5) : Seq[(String, (String, Double))]  
  
  def extractPredictionsForRow(model : PipelineModel, predictions:DataFrame, rowid:String, maxPredictions:Int = 5) : Seq[(String, Double)]
    
}