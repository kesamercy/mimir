package mimir.sql.sparksql

import mimir.algebra._
import mimir.util.JDBCUtils
import org.geotools.referencing.datum.DefaultEllipsoid
import org.joda.time.DateTime
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object SparkSQLCompat {

  def registerFunctions(spark:org.apache.spark.sql.SparkSession):Unit = {
    spark.udf.register("SIMPLETEST", SpeedTest.SimpleTest _)
/*    spark.udf.register("MIMIRCAST", MimirCast.mimircast _)
    spark.udf.register("AGGTEST", new AggTest)
    spark.udf.register( "SQRT", Sqrt.sqrt _)
    spark.udf.register( "DST", Distance.distance _)
    spark.udf.register( "SPEED", Speed.speed _)
    spark.udf.register( "MINUS", Minus.minus _)
    spark.udf.register( "GROUP_AND", new GroupAnd)
    spark.udf.register( "GROUP_OR", new GroupOr)
    spark.udf.register( "FIRST", new First)
    spark.udf.register( "FIRST_INT", new First)
    spark.udf.register( "FIRST_FLOAT", new First)
*/
  }
  
  def getTableSchema(spark:org.apache.spark.sql.SparkSession, table: String): Option[List[(String, Type)]] =
  {
    val df = spark.table(table)
    val schema = df.schema
    val result = schema.fields.map( x => { 
      val name = x.name.toUpperCase.trim
      val inferredType = x.dataType match {
        case BinaryType => TString()
        case BooleanType => TString()
        case ByteType => TString()
        case CalendarIntervalType => TString()
        case DateType => TString()
        case DecimalType() => TFloat()
        case DoubleType => TFloat()
        case FloatType => TFloat()
        case IntegerType => TInt()
        case LongType => TInt()
        case NullType => TAny()
        case ShortType => TInt()
        case StringType => TString()
        case TimestampType => TTimeStamp()
      }
      
      // println(s"$name -> $rawType -> $baseType -> $inferredType"); 

      (name, inferredType)
    })

    if(!result.isEmpty){ Some(result.toList) } else { None }
  }
}

object Minus {
  def minus(arg1:Double, arg2:Double): Double = {
    arg2-arg1
  }
  }

object Distance {
  def distance(lat1:Double, lon1:Double, lat2:Double, lon2:Double): Unit = {
    DefaultEllipsoid.WGS84.orthodromicDistance(lon1, lat1, lon2, lat2)
  }
}

object Speed {
  def speed(distance:Double, startingDateText: String, endingDateText: String): Double = {
    val startingDate: DateTime = DateTime.parse(startingDateText)
    var endingDate: DateTime = new DateTime
    if(endingDateText != null) {
      endingDate = DateTime.parse(endingDateText)
    }

    val numberOfHours: Long = Math.abs(endingDate.getMillis - startingDate.getMillis) / 1000 / 60 / 60

    (distance / 1000 / numberOfHours) // kmph
  }
}

object Sqrt {
  def sqrt(arg1:Double): Double = {
    Math.sqrt(arg1)
  }
}

object MimirCast {
    def mimircast(arg1: Any, arg2:Int): Any = { // 1 is int, double is 2, 3 is string, 5 is null
      try {
//        println("Input: " + value_text(0) + " : " + value_text(1))
        val t = Type.toSQLiteType(arg2)
//        println("TYPE CASTED: "+t)
        val va = arg1
        t match {
          case TInt() =>
            va match {
              case intVal : Int => intVal.toLong
              case doubleVal : Double   => doubleVal.toLong
              case strVal : String => java.lang.Long.parseLong(strVal)
              case null    => null
            }
          case TFloat() =>
            va match {
              case intVal : Int => intVal.toDouble
              case doubleVal : Double   => doubleVal.toDouble
              case strVal : String => java.lang.Double.parseDouble(strVal)
              case null    => null
            }
          case TString() | TRowId() | TDate() | TTimeStamp() =>
            va.toString()

          case TUser(name) =>
            if(va != null) {
              val v:String = va.toString()
              Type.rootType(t) match {
                case TRowId() =>
                  v
                case TString() | TDate() | TTimeStamp() =>
                  val txt = v
                  if(TypeRegistry.matches(name, txt)){
                    v
                  } else {
                    null
                  }
                case TInt() | TBool() =>
                  if(TypeRegistry.matches(name, v)){
                    java.lang.Integer.parseInt(v)
                  } else {
                    null
                  }
                case TFloat() =>
                  if(TypeRegistry.matches(name, v)){
                    java.lang.Double.parseDouble(v)
                  } else {
                    null
                  }
                case TAny() =>
                  if(TypeRegistry.matches(name, v)){
                    v
                  } else {
                    null
                  }
                case TUser(_) | TType() =>
                  throw new Exception("In SQLiteCompat expected natural type but got: " + Type.rootType(t).toString())
              }
            }
            else{
              null
            }

          case _ =>
            "I assume that you put something other than a number in, this functions works like, MIMIRCAST(column,type), the types are int values, 1 is int, 2 is double, 3 is string, and 5 is null, so MIMIRCAST(COL,1) is casting column 1 to int"
            // throw new java.sql.SQLDataException("Well here we are, I'm not sure really what went wrong but it happened in MIMIRCAST, maybe it was a type, good luck")
        }

      } catch {
        case _:TypeException => null
        case _:NumberFormatException => null
      }
    }
}

class GroupAnd extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", IntegerType) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("agg", BooleanType) :: Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = IntegerType

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = true
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Boolean](0)  && (input.getAs[Integer](0) != 0)
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Boolean](0) && buffer2.getAs[Boolean](0)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    if(buffer.getAs[Boolean](0)){ 1 } else { 0 }
  }
}

class GroupOr extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", IntegerType) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("agg", BooleanType) :: Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = IntegerType

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = true
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Boolean](0)  || (input.getAs[Integer](0) != 0)
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Boolean](0) || buffer2.getAs[Boolean](0)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    if(buffer.getAs[Boolean](0)){ 1 } else { 0 }
  }
}

object SpeedTest {
  def SimpleTest(i:Int): Int = {
      9001
  }
}

class First extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", StringType) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("agg", StringType) :: Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = null
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(buffer.getAs[String](0) == null ){ buffer(0) = input.getAs[String](0) }
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if(buffer1.getAs[String](0) == null ){ buffer1(0) = buffer2.getAs[String](0) }
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[String](0)
  }
}

class FirstInt extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", IntegerType) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("agg", IntegerType) ::
    StructField("agg", BooleanType) :: Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = IntegerType

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
    buffer(1) = true
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(buffer.getAs[Boolean](1)){ buffer(0) = input.getAs[Integer](0); buffer(1) = false }
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if(buffer1.getAs[Boolean](1)){ buffer1(0) = buffer2.getAs[Integer](0) }
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    if(buffer.getAs[Boolean](1)){ null } else{ buffer.getAs[Integer](0) }
  }
}

class FirstFloat extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", DoubleType) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("agg", DoubleType) ::
    StructField("agg", BooleanType) :: Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0.0
    buffer(1) = false
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(buffer.getAs[Boolean](1)){ buffer(0) = input.getAs[Double](0); buffer(1) = true }
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if(buffer1.getAs[Boolean](1)){ buffer1(0) = buffer2.getAs[Double](0) }
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    if(buffer.getAs[Boolean](1)){ null } else{ buffer.getAs[Double](0) }
  }
}

class AggTest extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", IntegerType) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("agg", IntegerType) :: Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = IntegerType

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) =  buffer.getAs[Integer](0) + input.getAs[Integer](0)
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) =  buffer1.getAs[Integer](0) + buffer2.getAs[Integer](0)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Integer](0)
  }
}

