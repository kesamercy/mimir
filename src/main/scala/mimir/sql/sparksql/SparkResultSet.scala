package mimir.sql.sparksql


import java.io.InputStream
import java.io.Reader
import java.math.BigDecimal
import java.net.MalformedURLException
import java.net.URL
//import java.sql.Array
import java.sql.Blob
import java.sql.Clob
import java.sql.Date
import java.sql.NClob
import java.sql.Ref
import java.sql.ResultSet
import java.sql.RowId
import java.sql.SQLException
import java.sql.SQLWarning
import java.sql.SQLXML
import java.sql.Time
import java.sql.Timestamp
import java.util.Calendar
import java.util.Map
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConversions._

class SparkResultSet(sparkDataFrame: DataFrame) extends ResultSet {

  val cols = sparkDataFrame.columns.toSeq.zipWithIndex.toMap
  val rows = sparkDataFrame.collect()
  var row = -1L
  var wasLastGetNull = false
  
  def unwrap[T](iface: Class[T]): T = sparkDataFrame.asInstanceOf[T]

  def isWrapperFor(wrappedType: Class[_]): Boolean = wrappedType == sparkDataFrame.getClass 
  
  def wasNull(): Boolean = wasLastGetNull

  def getObject[T](colName: String,getAs: Class[T]): T = getObject[T](findColumn(colName),getAs)
  def getObject[T](columnIndex: Int,getAs: Class[T]): T = {
    if(row >= 0 && row < sparkDataFrame.count()){
      val ret = rows(row.toInt).get(columnIndex)
      if(ret == null)
        wasLastGetNull = true
      ret.asInstanceOf[T]
    }
    else
      null.asInstanceOf[T]
      //throw new IndexOutOfBoundsException(s"Row: $row is out of the range of 0 - ${sparkDataFrame.count()}")
  }
  def getObject(x$1: String,x$2: java.util.Map[String,Class[_]]): Object = throw new UnsupportedOperationException(
      "Sorry, the driver does not implement getDate")

  def getObject(x$1: Int,x$2: java.util.Map[String,Class[_]]): Object = throw new UnsupportedOperationException(
      "Sorry, the driver does not implement getDate")

  def getObject(columnIndex: Int): Object = getObject[Object](columnIndex, classOf[Object])
  
  def findColumn(columnName: String): Int = {
    cols(columnName)
  }
  
  def getString(columnIndex: Int): String = {
    val o: AnyRef = getObject(columnIndex)
    if (o == null) "" else o.toString
  }

  def getBoolean(columnIndex: Int): Boolean =
    (getObject(columnIndex) != null) && "" != getObject(columnIndex)

  def getByte(columnIndex: Int): Byte =
    java.lang.Byte.parseByte(getString(columnIndex))

  def getShort(columnIndex: Int): Short =
    java.lang.Short.parseShort(getString(columnIndex))

  def getInt(columnIndex: Int): Int =
    java.lang.Integer.parseInt(getString(columnIndex))

  def getLong(columnIndex: Int): Long =
    java.lang.Long.parseLong(getString(columnIndex))

  def getFloat(columnIndex: Int): Float =
    java.lang.Float.parseFloat(getString(columnIndex))

  def getDouble(columnIndex: Int): Double =
    java.lang.Double.parseDouble(getString(columnIndex))

  def getBigDecimal(columnIndex: Int, scale: Int): BigDecimal =
    new BigDecimal(getString(columnIndex))

  def getBytes(columnIndex: Int): Array[Byte] = getString(columnIndex).getBytes

  def getDate(columnIndex: Int): Date =
    throw new UnsupportedOperationException(
      "Sorry, the driver does not implement getDate")

  def getTime(columnIndex: Int): Time =
    throw new UnsupportedOperationException(
      "Sorry, the driver does not implement getTime")

  def getTimestamp(columnIndex: Int): Timestamp =
    throw new UnsupportedOperationException(
      "Sorry, the driver does not implement getTimestamp")

  def getAsciiStream(columnIndex: Int): InputStream =
    throw new UnsupportedOperationException(
      "Sorry, the driver does not implement getAsciiStream")

  def getUnicodeStream(columnIndex: Int): InputStream =
    throw new UnsupportedOperationException(
      "Sorry, the driver does not implement getUnicodeStream")

  def getBinaryStream(columnIndex: Int): InputStream =
    throw new UnsupportedOperationException(
      "Sorry, does not implement getBinaryStream")

  def getString(columnLabel: String): String = getObject(columnLabel).toString

  def getBoolean(columnLabel: String): Boolean =
    (getObject(columnLabel) != null) && "" != getObject(columnLabel)

  def getByte(columnLabel: String): Byte =
    java.lang.Byte.parseByte(getString(columnLabel))

  def getShort(columnLabel: String): Short =
    java.lang.Short.parseShort(getString(columnLabel))

  def getInt(columnLabel: String): Int =
    java.lang.Integer.parseInt(getString(columnLabel))

  def getLong(columnLabel: String): Long =
    java.lang.Long.parseLong(getString(columnLabel))

  def getFloat(columnLabel: String): Float =
    java.lang.Float.parseFloat(getString(columnLabel))

  def getDouble(columnLabel: String): Double =
    java.lang.Double.parseDouble(getString(columnLabel))

  def getBigDecimal(columnLabel: String, scale: Int): BigDecimal =
    new BigDecimal(getString(columnLabel))

  def getBytes(columnLabel: String): Array[Byte] =
    getString(columnLabel).getBytes

  def getDate(columnLabel: String): Date =
    throw new UnsupportedOperationException(
      "Sorry, the driver does not implement getDate/getTime/getTimestamp")

  def getTime(columnLabel: String): Time =
    throw new UnsupportedOperationException(
      "Sorry, the driver does not implement getDate/getTime/getTimestamp")

  def getTimestamp(columnLabel: String): Timestamp =
    throw new UnsupportedOperationException(
      "Sorry, the driver does not implement getDate/getTime/getTimestamp")

  def getAsciiStream(columnLabel: String): InputStream =
    throw new UnsupportedOperationException(
      "Sorry, the driver does not implement get*Stream")

  def getUnicodeStream(columnLabel: String): InputStream =
    throw new UnsupportedOperationException(
      "Sorry, the driver does not implement get*Stream")

  def getBinaryStream(columnLabel: String): InputStream =
    throw new UnsupportedOperationException(
      "Sorry, the driver does not implement get*Stream")

  def getWarnings(): SQLWarning = null

  def clearWarnings(): Unit = {}

  def getCursorName(): String = null

  def getObject(columnLabel: String): AnyRef =
    getObject(findColumn(columnLabel))

  def getCharacterStream(columnIndex: Int): Reader =
    throw new UnsupportedOperationException(
      "Sorry, the driver does not implement get*Stream")

  def getCharacterStream(columnLabel: String): Reader =
    throw new UnsupportedOperationException(
      "Sorry, the driver does not implement get*Stream")

  def getBigDecimal(columnIndex: Int): BigDecimal =
    new BigDecimal(getString(columnIndex))

  def getBigDecimal(columnLabel: String): BigDecimal =
    new BigDecimal(getString(columnLabel))

  def isBeforeFirst(): Boolean =
    row < 0

  def isAfterLast(): Boolean =
    row >= sparkDataFrame.count()

  def isFirst(): Boolean =
    row == 0

  def isLast(): Boolean =
    row == sparkDataFrame.count()-1

  def beforeFirst(): Unit = {
   row = -1
  }

  def afterLast(): Unit = {
   row = sparkDataFrame.count()
  }

  def first(): Boolean = {
    row = 0  
    sparkDataFrame.count() > 0 
  }
  
  def last(): Boolean = {
    row = sparkDataFrame.count() -1
    sparkDataFrame.count() > 0 
  }
  
  def absolute(row: Int): Boolean = {
    this.row = row
    row < sparkDataFrame.count() && row >= 0
  }
  
  def relative(rows: Int): Boolean = {
    row = row+rows
    row < sparkDataFrame.count() && row >= 0
  }

  def previous(): Boolean ={
    row = row-1
    row >= 0   
  }
  
  def next(): Boolean= {
    println(s"SparkRS: next: $row -> ${row+1}")
    row = row+1
    row < sparkDataFrame.count()
  }
  
  def close(): Unit = {
    row = -1
  }
  def getMetaData(): java.sql.ResultSetMetaData = {
    new SparkResultSetMetaData(sparkDataFrame)
  }
  def getRow(): Int = {
    row.toInt
  }
  def getStatement(): java.sql.Statement = {
    throw new SQLException("not implemented")
  }
  def isClosed(): Boolean = {
    throw new SQLException("not implemented")
  }
  
  def setFetchDirection(direction: Int): Unit = {
    if (direction != ResultSet.FETCH_FORWARD) {
      throw new UnsupportedOperationException(
        "Only FETCH_FORWARD is supported")
    }
  }

  def getFetchDirection(): Int = ResultSet.FETCH_FORWARD

  def setFetchSize(rows: Int): Unit = {}

  def getFetchSize(): Int = 0

  def getType(): Int = ResultSet.TYPE_FORWARD_ONLY

  def getConcurrency(): Int = ResultSet.CONCUR_READ_ONLY

  def rowUpdated(): Boolean = false

  def rowInserted(): Boolean = false

  def rowDeleted(): Boolean = false

  def updateNull(columnIndex: Int): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateBoolean(columnIndex: Int, x: Boolean): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateByte(columnIndex: Int, x: Byte): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateShort(columnIndex: Int, x: Short): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateInt(columnIndex: Int, x: Int): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateLong(columnIndex: Int, x: Long): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateFloat(columnIndex: Int, x: Float): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateDouble(columnIndex: Int, x: Double): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateBigDecimal(columnIndex: Int, x: BigDecimal): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateString(columnIndex: Int, x: String): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateBytes(columnIndex: Int, x: Array[Byte]): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateDate(columnIndex: Int, x: Date): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateTime(columnIndex: Int, x: Time): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateTimestamp(columnIndex: Int, x: Timestamp): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateAsciiStream(columnIndex: Int, x: InputStream, length: Int): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateBinaryStream(columnIndex: Int, x: InputStream, length: Int): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateCharacterStream(columnIndex: Int, x: Reader, length: Int): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateObject(columnIndex: Int, x: AnyRef, scaleOrLength: Int): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateObject(columnIndex: Int, x: AnyRef): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateNull(columnLabel: String): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateBoolean(columnLabel: String, x: Boolean): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateByte(columnLabel: String, x: Byte): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateShort(columnLabel: String, x: Short): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateInt(columnLabel: String, x: Int): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateLong(columnLabel: String, x: Long): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateFloat(columnLabel: String, x: Float): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateDouble(columnLabel: String, x: Double): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateBigDecimal(columnLabel: String, x: BigDecimal): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateString(columnLabel: String, x: String): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateBytes(columnLabel: String, x: Array[Byte]): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateDate(columnLabel: String, x: Date): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateTime(columnLabel: String, x: Time): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateTimestamp(columnLabel: String, x: Timestamp): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateAsciiStream(columnLabel: String,
                        x: InputStream,
                        length: Int): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateBinaryStream(columnLabel: String,
                         x: InputStream,
                         length: Int): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateCharacterStream(columnLabel: String,
                            reader: Reader,
                            length: Int): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateObject(columnLabel: String, x: AnyRef, scaleOrLength: Int): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateObject(columnLabel: String, x: AnyRef): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def insertRow(): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateRow(): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def deleteRow(): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def refreshRow(): Unit = {}

  def cancelRowUpdates(): Unit = {}

  def moveToInsertRow(): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def moveToCurrentRow(): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def getRef(columnIndex: Int): Ref =
    throw new UnsupportedOperationException("Refs are not supported")

  def getBlob(columnIndex: Int): Blob =
    throw new UnsupportedOperationException("[BC]lobs are not supported")

  def getClob(columnIndex: Int): Clob =
    throw new UnsupportedOperationException("[BC]lobs are not supported")

  def getArray(columnIndex: Int): java.sql.Array =
    throw new UnsupportedOperationException("Arrays are not supported")

  def getRef(columnLabel: String): Ref =
    throw new UnsupportedOperationException("Refs are not supported")

  def getBlob(columnLabel: String): Blob =
    throw new UnsupportedOperationException("[BC]lobs are not supported")

  def getClob(columnLabel: String): Clob =
    throw new UnsupportedOperationException("[BC]lobs are not supported")

  def getArray(columnLabel: String): java.sql.Array =
    throw new UnsupportedOperationException("Arrays are not supported")

  def getDate(columnIndex: Int, cal: Calendar): Date =
    throw new UnsupportedOperationException(
      "Sorry, the driver does not implement getDate/getTime/getTimestamp")

  def getDate(columnLabel: String, cal: Calendar): Date =
    throw new UnsupportedOperationException(
      "Sorry, the driver does not implement getDate/getTime/getTimestamp")

  def getTime(columnIndex: Int, cal: Calendar): Time =
    throw new UnsupportedOperationException(
      "Sorry, the driver does not implement getDate/getTime/getTimestamp")

  def getTime(columnLabel: String, cal: Calendar): Time =
    throw new UnsupportedOperationException(
      "Sorry, the driver does not implement getDate/getTime/getTimestamp")

  def getTimestamp(columnIndex: Int, cal: Calendar): Timestamp =
    throw new UnsupportedOperationException(
      "Sorry, the driver does not implement getDate/getTime/getTimestamp")

  def getTimestamp(columnLabel: String, cal: Calendar): Timestamp =
    throw new UnsupportedOperationException(
      "Sorry, the driver does not implement getDate/getTime/getTimestamp")

  def getURL(columnIndex: Int): URL = new URL(getString(columnIndex))

  def getURL(columnLabel: String): URL = new URL(getString(columnLabel))

  def updateRef(columnIndex: Int, x: Ref): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateRef(columnLabel: String, x: Ref): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateBlob(columnIndex: Int, x: Blob): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateBlob(columnLabel: String, x: Blob): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateClob(columnIndex: Int, x: Clob): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateClob(columnLabel: String, x: Clob): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateArray(columnIndex: Int, x: java.sql.Array): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateArray(columnLabel: String, x: java.sql.Array): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def getRowId(columnIndex: Int): RowId =
    throw new UnsupportedOperationException("RowId not supported")

  def getRowId(columnLabel: String): RowId =
    throw new UnsupportedOperationException("RowId not supported")

  def updateRowId(columnIndex: Int, x: RowId): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateRowId(columnLabel: String, x: RowId): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def getHoldability(): Int = ResultSet.CLOSE_CURSORS_AT_COMMIT

  def updateNString(columnIndex: Int, nString: String): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateNString(columnLabel: String, nString: String): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateNClob(columnIndex: Int, nClob: NClob): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateNClob(columnLabel: String, nClob: NClob): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def getNClob(columnIndex: Int): NClob =
    throw new UnsupportedOperationException("[BC]lobs are not supported")

  def getNClob(columnLabel: String): NClob =
    throw new UnsupportedOperationException("[BC]lobs are not supported")

  def getSQLXML(columnIndex: Int): SQLXML =
    throw new UnsupportedOperationException("XML fields are not supported")

  def getSQLXML(columnLabel: String): SQLXML =
    throw new UnsupportedOperationException("XML fields are not supported")

  def updateSQLXML(columnIndex: Int, xmlObject: SQLXML): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def updateSQLXML(columnLabel: String, xmlObject: SQLXML): Unit = {
    throw new UnsupportedOperationException(
      "result modification methods are not supported")
  }

  def getNString(columnIndex: Int): String = getString(columnIndex)

  def getNString(columnLabel: String): String = getString(columnLabel)

  def getNCharacterStream(columnIndex: Int): Reader = // TODO Auto-generated method stub
    null

  def getNCharacterStream(columnLabel: String): Reader = // TODO Auto-generated method stub
    null

  def updateNCharacterStream(columnIndex: Int, x: Reader, length: Long): Unit = {}
// TODO Auto-generated method stub

  def updateNCharacterStream(columnLabel: String,
                             reader: Reader,
                             length: Long): Unit = {}
// TODO Auto-generated method stub

  def updateAsciiStream(columnIndex: Int, x: InputStream, length: Long): Unit = {}
// TODO Auto-generated method stub

  def updateBinaryStream(columnIndex: Int,
                         x: InputStream,
                         length: Long): Unit = {}
// TODO Auto-generated method stub

  def updateCharacterStream(columnIndex: Int, x: Reader, length: Long): Unit = {}
// TODO Auto-generated method stub

  def updateAsciiStream(columnLabel: String,
                        x: InputStream,
                        length: Long): Unit = {}
// TODO Auto-generated method stub

  def updateBinaryStream(columnLabel: String,
                         x: InputStream,
                         length: Long): Unit = {}
// TODO Auto-generated method stub

  def updateCharacterStream(columnLabel: String,
                            reader: Reader,
                            length: Long): Unit = {}
// TODO Auto-generated method stub

  def updateBlob(columnIndex: Int,
                 inputStream: InputStream,
                 length: Long): Unit = {}
// TODO Auto-generated method stub

  def updateBlob(columnLabel: String,
                 inputStream: InputStream,
                 length: Long): Unit = {}
// TODO Auto-generated method stub

  def updateClob(columnIndex: Int, reader: Reader, length: Long): Unit = {}
// TODO Auto-generated method stub

  def updateClob(columnLabel: String, reader: Reader, length: Long): Unit = {}
// TODO Auto-generated method stub

  def updateNClob(columnIndex: Int, reader: Reader, length: Long): Unit = {}
// TODO Auto-generated method stub

  def updateNClob(columnLabel: String, reader: Reader, length: Long): Unit = {}
// TODO Auto-generated method stub

  def updateNCharacterStream(columnIndex: Int, x: Reader): Unit = {}
// TODO Auto-generated method stub

  def updateNCharacterStream(columnLabel: String, reader: Reader): Unit = {}
// TODO Auto-generated method stub

  def updateAsciiStream(columnIndex: Int, x: InputStream): Unit = {}
// TODO Auto-generated method stub

  def updateBinaryStream(columnIndex: Int, x: InputStream): Unit = {}
// TODO Auto-generated method stub

  def updateCharacterStream(columnIndex: Int, x: Reader): Unit = {}
// TODO Auto-generated method stub

  def updateAsciiStream(columnLabel: String, x: InputStream): Unit = {}
// TODO Auto-generated method stub

  def updateBinaryStream(columnLabel: String, x: InputStream): Unit = {}
// TODO Auto-generated method stub

  def updateCharacterStream(columnLabel: String, reader: Reader): Unit = {}
// TODO Auto-generated method stub

  def updateBlob(columnIndex: Int, inputStream: InputStream): Unit = {}
// TODO Auto-generated method stub

  def updateBlob(columnLabel: String, inputStream: InputStream): Unit = {}
// TODO Auto-generated method stub

  def updateClob(columnIndex: Int, reader: Reader): Unit = {}
// TODO Auto-generated method stub

  def updateClob(columnLabel: String, reader: Reader): Unit = {}
// TODO Auto-generated method stub

  def updateNClob(columnIndex: Int, reader: Reader): Unit = {}
// TODO Auto-generated method stub

  def updateNClob(columnLabel: String, reader: Reader): Unit = {}
// TODO Auto-generated method stub

}

