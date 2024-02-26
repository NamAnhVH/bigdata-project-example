package com.vcc.adopt.training.bigdata.spark

import com.vcc.adopt.config.ConfigPropertiesLoader
import com.vcc.adopt.utils.hbase.HBaseConnectionFactory
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Get, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.{Connection, DriverManager, ResultSet}
import java.util



object SparkHBase {

  val spark: SparkSession = SparkSession.builder().getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  spark.conf.set("spark.sql.debug.maxToStringFields", 10000)

  private val url = ConfigPropertiesLoader.getYamlConfig.getProperty("url")
  private val username = ConfigPropertiesLoader.getYamlConfig.getProperty("username")
  private val password = ConfigPropertiesLoader.getYamlConfig.getProperty("password")
  var connection: Connection = null
  var resultSet: ResultSet = null


  def resultSetToDataFrame(resultSet: ResultSet): DataFrame = {
    import spark.implicits._
    val rows = Iterator.continually(resultSet).takeWhile(_.next()).map { row =>
      (row.getString("dept_no"), row.getInt("emp_no"))
    }
    val df = rows.toSeq.toDF("dept_no", "emp_no")
    df
  }
  private def readMySqlThenPutToHBase(): Unit = {
    println("----- Read employees on mySql then put to table bai5:deptemp ----")

    try {
      // Load driver
      Class.forName("com.mysql.cj.jdbc.Driver")

      // Tạo kết nối
      connection = DriverManager.getConnection(url, username, password)

      // Thực hiện truy vấn
      val statement = connection.createStatement()
      val query = "SELECT * FROM dept_emp"
      resultSet = statement.executeQuery(query)

      val data = resultSetToDataFrame(resultSet)
      data.show()

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      // Đóng kết nối
      if (resultSet != null) resultSet.close()
      if (connection != null) connection.close()
    }
  }


  def main(args: Array[String]): Unit = {
    readMySqlThenPutToHBase()
  }
}
