package com.vcc.adopt.training.bigdata.spark

import com.vcc.adopt.config.ConfigPropertiesLoader
import com.vcc.adopt.utils.hbase.HBaseConnectionFactory
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Get, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

import java.util


object SparkHBase {

  val spark: SparkSession = SparkSession.builder().getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  private val personInfoLogPath = ConfigPropertiesLoader.getYamlConfig.getProperty("personInfoLogPath")
  private val personIdListLogPath = ConfigPropertiesLoader.getYamlConfig.getProperty("personIdListLogPath")
  private val ageAnalysisPath = ConfigPropertiesLoader.getYamlConfig.getProperty("ageAnalysisPath")
  private val test = ConfigPropertiesLoader.getYamlConfig.getProperty("test")

  private def createDataFrameAndPutToHDFS(): Unit = {
    println(s"----- Make person info dataframe then write to parquet at ${personInfoLogPath} ----")

    // tạo person-info dataframe và lưu vào HDFS
    val data = Seq(
      Row(1L, "Alice", 25),
      Row(2L, "Bob", 30),
      Row(3L, "Charlie", 22),
      Row(4L, "Yorn", 22)
    )

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



    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    df.show()
    df.write
      .mode("overwrite")  // nếu file này đã tồn tại trước đó, sẽ ghi đè
      .parquet(personInfoLogPath)

    // tạo person-id-list và lưu vào HDFS
    df.select("personId")
      .write
      .mode("overwrite")
      .parquet(personIdListLogPath)
  }

  private def readHDFSThenPutToHBase(): Unit = {
    println("----- Read person-info.parquet on HDFS then put to table person:person-info ----")
//    var df = spark.read.parquet(personInfoLogPath)
    println("chill")
//    var df = spark.read.parquet(test)
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
    var df = spark.read
      .option("delimiter", "\t")
      .schema(schema)
      .format("parquet")
      .load(test)
    println(df)
    df = df
      .withColumn("country", lit("US"))
      .repartition(5)  // chia dataframe thành 5 phân vùng, mỗi phân vùng sẽ được chạy trên một worker (nếu không chia mặc định là 200)

    val batchPutSize = 100  // để đẩy dữ liệu vào hbase nhanh, thay vì đẩy lẻ tẻ từng dòng thì ta đẩy theo lô, như ví dụ là cứ 100 dòng sẽ đẩy 1ần

    df.foreachPartition((rows: Iterator[Row]) => {
      // tạo connection hbase buộc phải tạo bên trong mỗi partition (không được tạo bên ngoài). Tối ưu hơn sẽ dùng connectionPool để reuse lại connection trên các worker
      val hbaseConnection = HBaseConnectionFactory.createConnection()
      try {
        val table = hbaseConnection.getTable(TableName.valueOf("test", "test_info"))
        val puts = new util.ArrayList[Put]()
        for (row <- rows) {
          val timeCreate = row.getAs[java.sql.Timestamp]("timeCreate").getTime
          val cookieCreate = row.getAs[java.sql.Timestamp]("cookieCreate").getTime
          val browserCode = row.getAs[Int]("browserCode")
          val browserVer = row.getAs[String]("browserVer")
          val osCode = row.getAs[Int]("osCode")
          val osVer = row.getAs[String]("osVer")
          val ip = row.getAs[Long]("ip")
          val locId = row.getAs[Int]("locId")
          val domain = row.getAs[String]("domain")
          val siteId = row.getAs[Int]("siteId")
          val cId = row.getAs[Int]("cId")
          val path = row.getAs[String]("path")
          val referer = row.getAs[String]("referer")
          val guid = row.getAs[Long]("guid")
          val flashVersion = row.getAs[String]("flashVersion")
          val jre = row.getAs[String]("jre")
          val sr = row.getAs[String]("sr")
          val sc = row.getAs[String]("sc")
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

  private def readHBaseThenWriteToHDFS(): Unit = {
    println("----- Read person:info table to dataframe then analysis and write result to HDFS ----")
    /**
     * thống kê độ tuổi từ danh sách person_id
     * Cách xử lý:
     *    1. lấy danh sách person_id cần thống kê ở personIdListLogPath
     *    2. từ danh sách person_id lấy độ tuổi của mỗi người ở bảng person:person-info ở HBase
     *    3. dùng các phép transform trên dataframe để tính thống kê
     *    4. kết quả lưu vào HDFS
     */

    val personIdDF = spark.read.parquet(personIdListLogPath)
    import spark.implicits._
    val personIdAndAgeDF = personIdDF
      .repartition(5)
      .mapPartitions((rows: Iterator[Row]) => {
        val hbaseConnection = HBaseConnectionFactory.createConnection()
        val table = hbaseConnection.getTable(TableName.valueOf("person", "person_info"))
        try {
          rows.map(row => {
            val get = new Get(Bytes.toBytes(row.getAs[Long]("personId")))
              get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"))  // mặc định sẽ lấy ra tất cả các cột, dùng lệnh này giúp chỉ lấy cột age
              (row.getAs[Long]("personId"), Bytes.toInt(table.get(get).getValue(Bytes.toBytes("cf"), Bytes.toBytes("age"))))
          })
        }finally {
//          hbaseConnection.close()
        }
      }).toDF("personId", "age")

    personIdAndAgeDF.persist()
    personIdAndAgeDF.show()

    val analysisDF = personIdAndAgeDF.groupBy("age").count()
    analysisDF.show()
    analysisDF.write.mode("overwrite").parquet(ageAnalysisPath)

    personIdAndAgeDF.unpersist()

  }

  def main(args: Array[String]): Unit = {
//    createDataFrameAndPutToHDFS()
    readHDFSThenPutToHBase()
//    readHBaseThenWriteToHDFS()
  }
}
