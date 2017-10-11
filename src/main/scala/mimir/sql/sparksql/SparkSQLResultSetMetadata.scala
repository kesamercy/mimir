package mimir.sql.sparksql

import java.sql.SQLException
import java.sql.Types
import java.util.ArrayList
import java.util.List
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import scala.collection.JavaConversions._

class SparkResultSetMetaData (dataFrame : DataFrame)
    extends java.sql.ResultSetMetaData {

  val schema = dataFrame.schema
  

  override def getColumnCount(): Int = {
   schema.length
  }

  override def getColumnLabel(column: Int): String = this.getColumnName(column)

  override def getColumnName(column: Int): String = {
    if (schema == null || column > dataFrame.schema.length || column <= 0) {
      throw new SQLException("Column out of range")
    }
    schema.fieldNames(column - 1)
  }

  override def getCatalogName(column: Int): String = 
    ""

  override def getColumnDisplaySize(column: Int): Int = {
    val `type`: Int = this.getColumnType(column)
    var value: Int = 0
    if (`type` == Types.VARCHAR) {
      value = 40
    } else if (`type` == Types.INTEGER) {
      value = 10
    } else if (`type` == Types.BOOLEAN) {
      value = 5
    } else if (`type` == Types.FLOAT) {
      value = 15
    } else if (`type` == Types.JAVA_OBJECT) {
      value = 60
    }
    value
  }

  override def isAutoIncrement(column: Int): Boolean = false

  override def isSearchable(column: Int): Boolean = {
    if (column <= 0 || column > this.getColumnCount) {
      false
    }
    true
  }

  override def isCurrency(column: Int): Boolean = false

  override def isNullable(column: Int): Int = 0

  override def isSigned(column: Int): Boolean = false

  override def getPrecision(column: Int): Int = 0

  override def getScale(column: Int): Int = 0

  override def unwrap[T](iface: Class[T]): T = schema.asInstanceOf[T]

  override def isWrapperFor(iface: Class[_]): Boolean = schema.getClass == iface

  /**
    * By default, every field are string ...
    */
  override def getColumnType(column: Int): Int = {
    schema.fields(column-1).dataType  match {
        case BinaryType => Types.BINARY
        case BooleanType => Types.BOOLEAN
        case ByteType => Types.INTEGER
        case CalendarIntervalType => Types.OTHER
        case DateType => Types.DATE
        case DecimalType() => Types.DECIMAL
        case DoubleType => Types.DOUBLE
        case FloatType => Types.FLOAT
        case IntegerType => Types.INTEGER
        case LongType => Types.BIGINT
        case NullType => Types.NULL
        case ShortType => Types.SMALLINT
        case StringType => Types.VARCHAR
        case TimestampType => Types.TIMESTAMP
      }
  }

  override def getColumnTypeName(column: Int): String = {
    (schema.fields(column-1).dataType  match {
        case BinaryType => Types.BINARY
        case BooleanType => Types.BOOLEAN
        case ByteType => Types.INTEGER
        case CalendarIntervalType => Types.OTHER
        case DateType => Types.DATE
        case DecimalType() => Types.DECIMAL
        case DoubleType => Types.DOUBLE
        case FloatType => Types.FLOAT
        case IntegerType => Types.INTEGER
        case LongType => Types.BIGINT
        case NullType => Types.NULL
        case ShortType => Types.SMALLINT
        case StringType => Types.VARCHAR
        case TimestampType => Types.TIMESTAMP
      }).toString()
  }

  override def getColumnClassName(column: Int): String =
    classOf[String].getName

  override def getTableName(column: Int): String = schema.catalogString

  override def getSchemaName(column: Int): String = //not applicable
    ""

  override def isCaseSensitive(column: Int): Boolean = true

  override def isReadOnly(column: Int): Boolean = true

  override def isWritable(column: Int): Boolean = false

  override def isDefinitelyWritable(column: Int): Boolean = false

}