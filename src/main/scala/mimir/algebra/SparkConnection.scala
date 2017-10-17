package mimir.algebra

import java.io.File

import mimir.sql.JDBCBackend
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

sealed trait SparkConnection
{
  override def toString(): String = {
    SparkConnection.toString(this)
  }
  def open(): Unit = {
    SparkConnection.open(this)
  }
  def close(): Unit = {
    SparkConnection.close(this)
  }
  def loadTable(spark: SparkSession, tableName: String, tableAlias: String = null): Unit = {
    SparkConnection.loadTable(spark, this, tableName)
  }
}

object SparkConnection {

  def open(sparkConnection: SparkConnection) = sparkConnection match {
    case sqliteSparkConnection(sqliteBackend) => sqliteBackend.open()
    case _ => // do nothing
  }

  def close(sparkConnection: SparkConnection) = sparkConnection match {
    case sqliteSparkConnection(sqliteBackend) => sqliteBackend.close()
    case _ => // do nothing
  }

  def loadTable(spark: SparkSession, sparkConnection: SparkConnection, tableName: String, tableAlias: String = null): Unit = {
    /*
      Used to load a table into the spark session, extend for
     */
    sparkConnection match {
      case sqliteSparkConnection(sqliteBackend) =>
        val sparkConnectionUrl = sqliteBackend.conn.getMetaData.getURL
        val sparkConnectionProperties = new java.util.Properties()
        val numberPartitions: Int = 4
        val tempTableName = tableName + "_TEMP"

        val df = spark.read.jdbc(sparkConnectionUrl,s"(SELECT *, ROWID FROM $tableName)",sparkConnectionProperties)//.repartition(numberPartitions)
//        val level = StorageLevel.MEMORY_ONLY
//        df.persist(level)

        df.createOrReplaceTempView(tableName) // alias has already been checked
        //spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName as SELECT * FROM $tempTableName")

      case dataframeSparkConnection(dataFrame) => "dataframe"
      case csvSparkConnection(csv) => "csv"
//      case rddSparkConnection() => "rdd"
    }
  }

  def connectionType(sparkConnection: SparkConnection) = sparkConnection.toString()

  def toString(sparkConnection: SparkConnection) = sparkConnection match {
    case sqliteSparkConnection(sqliteBackend) => "sqlite"
    case dataframeSparkConnection(dataFrame) => "dataframe"
    case csvSparkConnection(csv) => "csv"
//    case rddSparkConnection() => "rdd"
    }
}

case class sqliteSparkConnection(sqliteBackend: JDBCBackend) extends SparkConnection
case class dataframeSparkConnection(dataFrame: DataFrame) extends SparkConnection
case class csvSparkConnection(csv: File) extends SparkConnection
//case class rddSparkConnection(RDD) extends SparkConnection