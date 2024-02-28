package com.vcc.adopt.training.bigdata.spark

import com.vcc.adopt.config.ConfigPropertiesLoader
import com.vcc.adopt.utils.hbase.HBaseConnectionFactory
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.sql.{Connection, DriverManager, ResultSet}



object SparkHBase {

  val spark: SparkSession = SparkSession.builder().getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  spark.conf.set("spark.sql.debug.maxToStringFields", 10000)

  private val url = ConfigPropertiesLoader.getYamlConfig.getProperty("url")
  private val username = ConfigPropertiesLoader.getYamlConfig.getProperty("username")
  private val password = ConfigPropertiesLoader.getYamlConfig.getProperty("password")
  var connection: Connection = null
  var resultSet: ResultSet = null

  private def readMySqlThenPutToHBase(): Unit = {
    println("----- Read employees on mySql then put to table bai5:deptemp ----")

    var deptEmp : DataFrame = null

    try {
      // Load driver
      Class.forName("com.mysql.cj.jdbc.Driver")

      // Tạo kết nối
      connection = DriverManager.getConnection(url, username, password)

      // Thực hiện truy vấn
      val statement = connection.createStatement()
      val query = "SELECT concat(de.dept_no,\"_\", de.emp_no) as dept_emp, de.from_date as de_from_date, de.to_date as de_to_date, e.emp_no, e.birth_date, e.first_name, e.last_name, e.gender, e.hire_date, d.dept_no, d.dept_name, dm.from_date as dm_from_date, dm.to_date as dm_to_date FROM dept_emp de\nleft join employees e on de.emp_no = e.emp_no\nleft join departments d on de.dept_no = d.dept_no\nleft join dept_manager dm on de.dept_no = dm.dept_no and de.emp_no = dm.emp_no;"
      resultSet = statement.executeQuery(query)

      deptEmp = {
        import spark.implicits._
        val rows = Iterator.continually(resultSet).takeWhile(_.next()).map { row =>
          (row.getString("dept_emp"),
            row.getString("de_from_date"),
            row.getString("de_to_date"),
            row.getInt("emp_no"),
            row.getString("birth_date"),
            row.getString("first_name"),
            row.getString("last_name"),
            row.getString("gender"),
            row.getString("hire_date"),
            row.getString("dept_no"),
            row.getString("dept_name"),
            row.getString("dm_from_date"),
            row.getString("dm_to_date")
          )
        }
        val df = rows.toSeq.toDF("dept_emp", "de_from_date", "de_to_date","emp_no","birth_date","first_name","last_name","gender","hire_date","dept_no","dept_name","dm_from_date","dm_to_date")
        df
      }

      deptEmp.show()
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
