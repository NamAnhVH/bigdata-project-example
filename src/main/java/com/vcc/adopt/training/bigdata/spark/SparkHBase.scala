package com.vcc.adopt.training.bigdata.spark

import com.vcc.adopt.config.ConfigPropertiesLoader
import com.vcc.adopt.utils.hbase.HBaseConnectionFactory
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Get, Put, Scan}
import org.apache.hadoop.hbase.filter.{PrefixFilter, SingleColumnValueFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

import java.util


object SparkHBase {

  val spark: SparkSession = SparkSession.builder().getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  spark.conf.set("spark.sql.debug.maxToStringFields", 10000)
  private val pageViewLogPath = ConfigPropertiesLoader.getYamlConfig.getProperty("pageViewLogPath")

  val schema = StructType(Seq(
    StructField("timeCreate", TimestampType, nullable = true),
    StructField("cookieCreate", TimestampType, nullable = true),
    StructField("browserCode", IntegerType, nullable = true),
    StructField("browserVer", StringType, nullable = true),
    StructField("osCode", IntegerType, nullable = true),
    StructField("osVer", StringType, nullable = true),
    StructField("ip", LongType, nullable = true),
    StructField("locId", IntegerType, nullable = true),
    StructField("domain", StringType, nullable = true),
    StructField("siteId", IntegerType, nullable = true),
    StructField("cId", IntegerType, nullable = true),
    StructField("path", StringType, nullable = true),
    StructField("referer", StringType, nullable = true),
    StructField("guid", LongType, nullable = true),
    StructField("flashVersion", StringType, nullable = true),
    StructField("jre", StringType, nullable = true),
    StructField("sr", StringType, nullable = true),
    StructField("sc", StringType, nullable = true),
    StructField("geographic", IntegerType, nullable = true),
    StructField("category", IntegerType, nullable = true)
  ))

  private def readHDFSThenPutToHBase(): Unit = {
    println("----- Read pageViewLog.parquet on HDFS then put to table pageviewlog ----")
    var df = spark.read.schema(schema).parquet(pageViewLogPath)
    df.show()
    df = df
      .withColumn("country", lit("US"))
      .repartition(5)  // chia dataframe thành 5 phân vùng, mỗi phân vùng sẽ được chạy trên một worker (nếu không chia mặc định là 200)

    val batchPutSize = 100  // để đẩy dữ liệu vào hbase nhanh, thay vì đẩy lẻ tẻ từng dòng thì ta đẩy theo lô, như ví dụ là cứ 100 dòng sẽ đẩy 1ần
    df.foreachPartition((rows: Iterator[Row]) => {
      // tạo connection hbase buộc phải tạo bên trong mỗi partition (không được tạo bên ngoài). Tối ưu hơn sẽ dùng connectionPool để reuse lại connection trên các worker
      val hbaseConnection = HBaseConnectionFactory.createConnection()
      try {
        val table = hbaseConnection.getTable(TableName.valueOf("bai4", "pageviewlog"))
        val puts = new util.ArrayList[Put]()
        for (row <- rows) {
          val timeCreate = Option(row.getAs[java.sql.Timestamp]("timeCreate")).map(_.getTime).getOrElse(0L)
          val cookieCreate = Option(row.getAs[java.sql.Timestamp]("cookieCreate")).map(_.getTime).getOrElse(0L)
          val browserCode = row.getAs[Int]("browserCode")
          val browserVer = Option(row.getAs[String]("browserVer")).getOrElse("")
          val osCode = row.getAs[Int]("osCode")
          val osVer = Option(row.getAs[String]("osVer")).getOrElse("")
          val ip = row.getAs[Long]("ip")
          val locId = row.getAs[Int]("locId")
          val domain = Option(row.getAs[String]("domain")).getOrElse("")
          val siteId = row.getAs[Int]("siteId")
          val cId = row.getAs[Int]("cId")
          val path = Option(row.getAs[String]("path")).getOrElse("")
          val referer = Option(row.getAs[String]("referer")).getOrElse("")
          val guid = row.getAs[Long]("guid")
          val flashVersion = Option(row.getAs[String]("flashVersion")).getOrElse("")
          val jre = Option(row.getAs[String]("jre")).getOrElse("")
          val sr = Option(row.getAs[String]("sr")).getOrElse("")
          val sc = Option(row.getAs[String]("sc")).getOrElse("")
          val geographic = row.getAs[Int]("geographic")
          val category = row.getAs[Int]("category")

          val put = new Put(Bytes.toBytes(timeCreate))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("timeCreate"), Bytes.toBytes(timeCreate))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cookieCreate"), Bytes.toBytes(cookieCreate))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("browserCode"), Bytes.toBytes(browserCode))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("browserVer"), Bytes.toBytes(browserVer))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("osCode"), Bytes.toBytes(osCode))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("osVer"), Bytes.toBytes(osVer))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("ip"), Bytes.toBytes(ip))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("locId"), Bytes.toBytes(locId))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("domain"), Bytes.toBytes(domain))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("siteId"), Bytes.toBytes(siteId))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cId"), Bytes.toBytes(cId))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("path"), Bytes.toBytes(path))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("referer"), Bytes.toBytes(referer))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("guid"), Bytes.toBytes(guid))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("flashVersion"), Bytes.toBytes(flashVersion))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("jre"), Bytes.toBytes(jre))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("sr"), Bytes.toBytes(sr))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("sc"), Bytes.toBytes(sc))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("geographic"), Bytes.toBytes(geographic))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("category"), Bytes.toBytes(category))

          puts.add(put)
          if(puts.size() > batchPutSize){
            table.put(puts)
            puts.clear()
          }
          if(puts.size() > 0){
            table.put(puts)
          }
        }
      } finally {
        hbaseConnection.close()
      }
    })
  }

  private def readHBase41(guid: Long, date: java.sql.Timestamp): Unit = {
    println("----- Liệt kê các url đã truy cập trong ngày của một guid (input: guid, date => output: ds url) ----")

    val hbaseConnection = HBaseConnectionFactory.createConnection()
    val table = hbaseConnection.getTable(TableName.valueOf("bai4", "pageviewlog"))
    try {
      val startRow = guid.toString + "_" + date.toString
      val stopRow = guid.toString + "_" + date.toString + "|"
      val scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(stopRow))

      // Thực hiện quét dữ liệu từ bảng HBase
      val scanner = table.getScanner(scan)

      // Liệt kê các URL đã truy cập trong ngày của GUID
      scanner.forEach(result => {
        val path = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("path")))
        println(path)
      })
    }finally {
      hbaseConnection.close()
    }

  }

  private def readHBase42(): Unit = {
    println("----- Các IP được sử dụng nhiều nhất của một guid (input: guid=> output: sort ds ip theo số lần xuất hiện) ----")

    val guidDF = spark.read.schema(schema).parquet(pageViewLogPath)
    import spark.implicits._
    val guidAndIpDF = guidDF
      .repartition(5)
      .mapPartitions((rows: Iterator[Row]) => {
        val hbaseConnection = HBaseConnectionFactory.createConnection()
        val table = hbaseConnection.getTable(TableName.valueOf("bai4", "pageviewlog"))
        try {
          rows.map(row => {
            val get = new Get(Bytes.toBytes(row.getAs[Long]("guid")))
            get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("ip"))  // mặc định sẽ lấy ra tất cả các cột, dùng lệnh này giúp chỉ lấy cột age
            (row.getAs[Long]("guid"), Bytes.toLong(table.get(get).getValue(Bytes.toBytes("cf"), Bytes.toBytes("ip"))))
          })
        }finally {
//          hbaseConnection.close()
        }
      }).toDF("guid", "ip")

    guidAndIpDF.persist()
    guidAndIpDF.show()

  }

  def main(args: Array[String]): Unit = {
//    readHDFSThenPutToHBase()
    readHBase42()
  }
}
