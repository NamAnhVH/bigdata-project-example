package com.vcc.adopt.training.bigdata.spark

import com.vcc.adopt.config.ConfigPropertiesLoader
import com.vcc.adopt.utils.hbase.HBaseConnectionFactory
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Get, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.util
import java.sql.{Connection, DriverManager, ResultSet}



object SparkHBase {

  val spark: SparkSession = SparkSession.builder().getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  spark.conf.set("spark.sql.debug.maxToStringFields", 10000)

  private val url = ConfigPropertiesLoader.getYamlConfig.getProperty("url")
  private val username = ConfigPropertiesLoader.getYamlConfig.getProperty("username")
  private val password = ConfigPropertiesLoader.getYamlConfig.getProperty("password")
  var connection: Connection = null
  var connection1: Connection = null
  var resultSet: ResultSet = null

  private def readMySqlThenPutToHBaseDeptEmp(): Unit = {
    println("----- Read employees on mySql then put to table bai5:dept_emp ----")

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

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      // Đóng kết nối
      if (resultSet != null) resultSet.close()
      if (connection != null) connection.close()
    }

    deptEmp = deptEmp
      .withColumn("country", lit("US"))
      .repartition(5)

    val batchPutSize = 100

    deptEmp.foreachPartition((rows: Iterator[Row]) => {
      // tạo connection hbase buộc phải tạo bên trong mỗi partition (không được tạo bên ngoài). Tối ưu hơn sẽ dùng connectionPool để reuse lại connection trên các worker
      val hbaseConnection = HBaseConnectionFactory.createConnection()
      try {
        val table = hbaseConnection.getTable(TableName.valueOf("bai5", "dept_emp"))
        val puts = new util.ArrayList[Put]()
        for (row <- rows) {
          val dept_emp = row.getAs[String]("dept_emp")
          val de_from_date = row.getAs[String]("de_from_date")
          val de_to_date = row.getAs[String]("de_to_date")
          val emp_no = row.getAs[Int]("emp_no")
          val birth_date = row.getAs[String]("birth_date")
          val first_name = row.getAs[String]("first_name")
          val last_name = row.getAs[String]("last_name")
          val gender = row.getAs[String]("gender")
          val hire_date = row.getAs[String]("hire_date")
          val dept_no = row.getAs[String]("dept_no")
          val dept_name = row.getAs[String]("dept_name")
          val dm_from_date = row.getAs[String]("dm_from_date")
          val dm_to_date = row.getAs[String]("dm_to_date")


          val put = new Put(Bytes.toBytes(dept_emp))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("de_from_date"), Bytes.toBytes(de_from_date))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("de_to_date"), Bytes.toBytes(de_to_date))
          put.addColumn(Bytes.toBytes("employee"), Bytes.toBytes("emp_no"), Bytes.toBytes(emp_no))
          put.addColumn(Bytes.toBytes("employee"), Bytes.toBytes("birth_date"), Bytes.toBytes(birth_date))
          put.addColumn(Bytes.toBytes("employee"), Bytes.toBytes("first_name"), Bytes.toBytes(first_name))
          put.addColumn(Bytes.toBytes("employee"), Bytes.toBytes("last_name"), Bytes.toBytes(last_name))
          put.addColumn(Bytes.toBytes("employee"), Bytes.toBytes("gender"), Bytes.toBytes(gender))
          put.addColumn(Bytes.toBytes("employee"), Bytes.toBytes("hire_date"), Bytes.toBytes(hire_date))
          put.addColumn(Bytes.toBytes("department"), Bytes.toBytes("dept_no"), Bytes.toBytes(dept_no))
          put.addColumn(Bytes.toBytes("department"), Bytes.toBytes("dept_name"), Bytes.toBytes(dept_name))
          if(dm_from_date != null){
            put.addColumn(Bytes.toBytes("manager"), Bytes.toBytes("dm_from_date"), Bytes.toBytes(dm_from_date))
          }
          if(dm_to_date != null) {
            put.addColumn(Bytes.toBytes("manager"), Bytes.toBytes("dm_to_date"), Bytes.toBytes(dm_to_date))
          }
          puts.add(put)
          if (puts.size > batchPutSize) {
            table.put(puts)
            puts.clear()
          }
        }
        if (puts.size() > 0) {  // đẩy nốt phần còn lại
          table.put(puts)
        }
      } finally {
        hbaseConnection.close()
      }
    })

  }

  private def readMySqlThenPutToHBaseSalaries(): Unit = {
    println("----- Read employees on mySql then put to table bai5:salaries ----")

      // Load driver
    Class.forName("com.mysql.cj.jdbc.Driver")

    val batchSize = 300000 // Số lượng dòng dữ liệu mỗi lần truy vấn

    // Tạo kết nối
    connection1 = DriverManager.getConnection(url, username, password)

    // Thực hiện truy vấn
    val rowCountQuery = "SELECT COUNT(*) AS row_count FROM salaries"
    val rowCountStatement = connection1.createStatement()
    val rowCountResultSet = rowCountStatement.executeQuery(rowCountQuery)
    rowCountResultSet.next()
    val rowCount = rowCountResultSet.getInt("row_count")

    // Tính số lượng phần cần chia dữ liệu
    val partitions = math.ceil(rowCount.toDouble / batchSize).toInt

    // Thực hiện vòng lặp để thực hiện truy vấn và xử lý từng phần
    for (i <- 0 until partitions) {
      val offset = i * batchSize // Offset cho mỗi phần
      val limit = batchSize // Số lượng dòng dữ liệu trong mỗi phần
      var salaries : DataFrame = null
      try {
        connection = DriverManager.getConnection(url, username, password)
      // Thực hiện truy vấn SQL cho phần hiện tại
      val query = "SELECT concat(s.emp_no, \"_\", s.from_date) as row_key, s.from_date, s.to_date, s.salary, s.emp_no FROM salaries s LIMIT " + limit + " OFFSET " + offset
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery(query)

      salaries = {
        import spark.implicits._
        val rows = Iterator.continually(resultSet).takeWhile(_.next()).map { row =>
          (row.getString("row_key"),
            row.getString("from_date"),
            row.getString("to_date"),
            row.getInt("salary"),
            row.getInt("emp_no"),
          )
        }
        val df = rows.toSeq.toDF("row_key", "from_date", "to_date", "salary", "emp_no")
        df
      }

      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        // Đóng kết nối
        if (resultSet != null) resultSet.close()
        if (connection != null) connection.close()
      }
      salaries = salaries
        .withColumn("country", lit("US"))
        .repartition(5)

      val batchPutSize = 100

      salaries.foreachPartition((rows: Iterator[Row]) => {
        // tạo connection hbase buộc phải tạo bên trong mỗi partition (không được tạo bên ngoài). Tối ưu hơn sẽ dùng connectionPool để reuse lại connection trên các worker
        val hbaseConnection = HBaseConnectionFactory.createConnection()
        try {
          val table = hbaseConnection.getTable(TableName.valueOf("bai5", "salaries"))
          val puts = new util.ArrayList[Put]()
          for (row <- rows) {
            val row_key = row.getAs[String]("row_key")
            val from_date = row.getAs[String]("from_date")
            val to_date = row.getAs[String]("to_date")
            val salary = row.getAs[Int]("salary")
            val emp_no = row.getAs[Int]("emp_no")

            val put = new Put(Bytes.toBytes(row_key))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("from_date"), Bytes.toBytes(from_date))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("to_date"), Bytes.toBytes(to_date))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("salary"), Bytes.toBytes(salary))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("emp_no"), Bytes.toBytes(emp_no))

            puts.add(put)
            if (puts.size > batchPutSize) {
              table.put(puts)
              puts.clear()
            }
          }
          if (puts.size() > 0) {  // đẩy nốt phần còn lại
            table.put(puts)
          }
        } finally {
          hbaseConnection.close()
        }
      })
    }




  }

  private def readMySqlThenPutToHBaseTitles(): Unit = {
    println("----- Read employees on mySql then put to table bai5:titles ----")

    var titles : DataFrame = null

    try {
      // Load driver
      Class.forName("com.mysql.cj.jdbc.Driver")

      // Tạo kết nối
      connection = DriverManager.getConnection(url, username, password)

      // Thực hiện truy vấn
      val statement = connection.createStatement()
      val query = "Select concat(t.emp_no, \"_\", t.from_date) as row_key, t.from_date, t.to_date, t.title, t.emp_no from titles as t;"
      resultSet = statement.executeQuery(query)

      titles = {
        import spark.implicits._
        val rows = Iterator.continually(resultSet).takeWhile(_.next()).map { row =>
          (row.getString("row_key"),
            row.getString("from_date"),
            row.getString("to_date"),
            row.getString("title"),
            row.getInt("emp_no"),
          )
        }
        val df = rows.toSeq.toDF("row_key", "from_date", "to_date","title","emp_no")
        df
      }

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      // Đóng kết nối
      if (resultSet != null) resultSet.close()
      if (connection != null) connection.close()
    }

    titles = titles
      .withColumn("country", lit("US"))
      .repartition(5)

    val batchPutSize = 100

    titles.foreachPartition((rows: Iterator[Row]) => {
      // tạo connection hbase buộc phải tạo bên trong mỗi partition (không được tạo bên ngoài). Tối ưu hơn sẽ dùng connectionPool để reuse lại connection trên các worker
      val hbaseConnection = HBaseConnectionFactory.createConnection()
      try {
        val table = hbaseConnection.getTable(TableName.valueOf("bai5", "titles"))
        val puts = new util.ArrayList[Put]()
        for (row <- rows) {
          val row_key = row.getAs[String]("row_key")
          val from_date = row.getAs[String]("from_date")
          val to_date = row.getAs[String]("to_date")
          val title = row.getAs[String]("title")
          val emp_no = row.getAs[Int]("emp_no")

          val put = new Put(Bytes.toBytes(row_key))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("from_date"), Bytes.toBytes(from_date))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("to_date"), Bytes.toBytes(to_date))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("title"), Bytes.toBytes(title))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("emp_no"), Bytes.toBytes(emp_no))

          puts.add(put)
          if (puts.size > batchPutSize) {
            table.put(puts)
            puts.clear()
          }
        }
        if (puts.size() > 0) {  // đẩy nốt phần còn lại
          table.put(puts)
        }
      } finally {
        hbaseConnection.close()
      }
    })

  }

  private def readHbase51(deptNo: String): Unit = {
    println("----- Lấy được danh sách, nhân viên & quản lý của 1 phòng ban cần truy vấn ----")

    var row_key : DataFrame = null

    try {
      // Load driver
      Class.forName("com.mysql.cj.jdbc.Driver")

      // Tạo kết nối
      connection = DriverManager.getConnection(url, username, password)

      // Thực hiện truy vấn
      val statement = connection.createStatement()
      val query = "Select concat(dept_no,\"_\", emp_no) as row_key from dept_emp where dept_no = '" + deptNo + "'"
      resultSet = statement.executeQuery(query)

      row_key = {
        import spark.implicits._
        val rows = Iterator.continually(resultSet).takeWhile(_.next()).map { row =>
          (row.getString("row_key"))
        }
        val df = rows.toSeq.toDF("row_key")
        df
      }

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      // Đóng kết nối
      if (resultSet != null) resultSet.close()
      if (connection != null) connection.close()
    }

    import spark.implicits._
    val empListDF = row_key
      .repartition(5)
      .mapPartitions((rows: Iterator[Row]) => {
        val hbaseConnection = HBaseConnectionFactory.createConnection()
        val table = hbaseConnection.getTable(TableName.valueOf("bai5", "dept_emp"))
        try {
          rows.flatMap(row => {
            val get = new Get(Bytes.toBytes(row.getAs[String]("row_key")))
            get.addColumn(Bytes.toBytes("employee"), Bytes.toBytes("emp_no"))
            get.addColumn(Bytes.toBytes("employee"), Bytes.toBytes("birth_date"))
            get.addColumn(Bytes.toBytes("employee"), Bytes.toBytes("first_name"))
            get.addColumn(Bytes.toBytes("employee"), Bytes.toBytes("last_name"))
            get.addColumn(Bytes.toBytes("employee"), Bytes.toBytes("gender"))
            get.addColumn(Bytes.toBytes("employee"), Bytes.toBytes("hire_date"))
            if (table.get(get).getValue(Bytes.toBytes("employee"), Bytes.toBytes("emp_no")) != null) {
              Some(
                Bytes.toInt(table.get(get).getValue(Bytes.toBytes("employee"), Bytes.toBytes("emp_no"))),
                Bytes.toString(table.get(get).getValue(Bytes.toBytes("employee"), Bytes.toBytes("birth_date"))),
                Bytes.toString(table.get(get).getValue(Bytes.toBytes("employee"), Bytes.toBytes("first_name"))),
                Bytes.toString(table.get(get).getValue(Bytes.toBytes("employee"), Bytes.toBytes("last_name"))),
                Bytes.toString(table.get(get).getValue(Bytes.toBytes("employee"), Bytes.toBytes("gender"))),
                Bytes.toString(table.get(get).getValue(Bytes.toBytes("employee"), Bytes.toBytes("hire_date")))
              )
            }
            else {
              None
            }
          })
        }finally {
          //          hbaseConnection.close()
        }
      }).toDF("emp_no", "birth_date","first_name","last_name","gender","hire_date")

    empListDF.persist()
    empListDF.show(empListDF.count().toInt)

    val managerListDF = row_key
      .repartition(5)
      .mapPartitions((rows: Iterator[Row]) => {
        val hbaseConnection = HBaseConnectionFactory.createConnection()
        val table = hbaseConnection.getTable(TableName.valueOf("bai5", "dept_emp"))
        try {
          rows.flatMap(row => {
            val get = new Get(Bytes.toBytes(row.getAs[String]("row_key")))
            get.addColumn(Bytes.toBytes("manager"), Bytes.toBytes("dm_from_date"))
            if (table.get(get).getValue(Bytes.toBytes("manager"), Bytes.toBytes("dm_from_date")) != null) {
              get.addColumn(Bytes.toBytes("employee"), Bytes.toBytes("emp_no"))
              get.addColumn(Bytes.toBytes("employee"), Bytes.toBytes("birth_date"))
              get.addColumn(Bytes.toBytes("employee"), Bytes.toBytes("first_name"))
              get.addColumn(Bytes.toBytes("employee"), Bytes.toBytes("last_name"))
              get.addColumn(Bytes.toBytes("employee"), Bytes.toBytes("gender"))
              get.addColumn(Bytes.toBytes("employee"), Bytes.toBytes("hire_date"))
              Some(
                Bytes.toInt(table.get(get).getValue(Bytes.toBytes("employee"), Bytes.toBytes("emp_no"))),
                Bytes.toString(table.get(get).getValue(Bytes.toBytes("employee"), Bytes.toBytes("birth_date"))),
                Bytes.toString(table.get(get).getValue(Bytes.toBytes("employee"), Bytes.toBytes("first_name"))),
                Bytes.toString(table.get(get).getValue(Bytes.toBytes("employee"), Bytes.toBytes("last_name"))),
                Bytes.toString(table.get(get).getValue(Bytes.toBytes("employee"), Bytes.toBytes("gender"))),
                Bytes.toString(table.get(get).getValue(Bytes.toBytes("employee"), Bytes.toBytes("hire_date")))
              )
            }
            else {
              None
            }
          })
        }finally {
          //          hbaseConnection.close()
        }
      }).toDF("emp_no", "birth_date","first_name","last_name","gender","hire_date")

    managerListDF.persist()
    managerListDF.show(managerListDF.count().toInt)

  }

  private def readHbase52(date: String): Unit = {
    println("----- Tính tổng lương phải trả cho tất cả nhân viên hàng tháng ----")

    var row_key : DataFrame = null

    try {
      // Load driver
      Class.forName("com.mysql.cj.jdbc.Driver")

      // Tạo kết nối
      connection = DriverManager.getConnection(url, username, password)

      // Thực hiện truy vấn
      val statement = connection.createStatement()
      val query = "SELECT concat(s.emp_no, \"_\", s.from_date) as row_key FROM salaries where s.from_date < '" + date +"' and s.to_date > '" + date + "'"
      resultSet = statement.executeQuery(query)

      row_key = {
        import spark.implicits._
        val rows = Iterator.continually(resultSet).takeWhile(_.next()).map { row =>
          (row.getString("row_key"))
        }
        val df = rows.toSeq.toDF("row_key")
        df
      }

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      // Đóng kết nối
      if (resultSet != null) resultSet.close()
      if (connection != null) connection.close()
    }
    var totalSalaries: Long = 0
    import spark.implicits._
    val totalSalariesRDD = row_key
      .repartition(5)
      .mapPartitions(rows => {
        val hbaseConnection = HBaseConnectionFactory.createConnection()
        val table = hbaseConnection.getTable(TableName.valueOf("bai5", "salaries"))
        try {
          rows.map(row => {
            val get = new Get(Bytes.toBytes(row.getAs[String]("row_key")))
            get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("salary"))
            val salaryBytes = table.get(get).getValue(Bytes.toBytes("info"), Bytes.toBytes("salary"))
            if (salaryBytes != null) {
              Bytes.toLong(salaryBytes)
            } else {
              0L
            }
          })
        } finally {
          // hbaseConnection.close() // Đóng kết nối sau khi sử dụng
        }
      })

    // Tính tổng các mức lương từ RDD
    totalSalaries = totalSalariesRDD.reduce(_ + _)
    println(totalSalaries)

  }


  def main(args: Array[String]): Unit = {
//    readMySqlThenPutToHBaseDeptEmp()
//    readMySqlThenPutToHBaseSalaries()
//    readMySqlThenPutToHBaseTitles()
//    readHbase51("d001")
    readHbase52("1986-08-26")
  }
}
