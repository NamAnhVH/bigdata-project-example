package com.vcc.adopt.training.bigdata.spark

import com.vcc.adopt.config.ConfigPropertiesLoader
import com.vcc.adopt.utils.hbase.HBaseConnectionFactory
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Get, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

import java.util


object SparkHBase {

  val spark: SparkSession = SparkSession.builder().getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  spark.conf.set("spark.sql.debug.maxToStringFields", 10000)
  private val pageViewLogPath = ConfigPropertiesLoader.getYamlConfig.getProperty("pageViewLogPath")
  private val inputFilePath = ConfigPropertiesLoader.getYamlConfig.getProperty("inputFilePath")
  private val outputFilePath = ConfigPropertiesLoader.getYamlConfig.getProperty("outputFilePath")

  private val schema = StructType(Seq(
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

  private def createParquetAndPutToHDFS(): Unit = {
    println(s"----- Make person info dataframe then write to parquet at ${pageViewLogPath} ----")

    // tạo person-info dataframe và lưu vào HDFS
    val data = spark.read
      .schema(schema)
      .option("delimiter", "\t")
      .csv(inputFilePath)

    data.write
      .mode("overwrite")  // Nếu tập tin này đã tồn tại trước đó, sẽ ghi đè lên nó
      .parquet(pageViewLogPath)

    println(s"----- Done writing person info dataframe to Parquet at ${pageViewLogPath} ----")
  }

  private def bai3(): Unit = {
    val df: DataFrame = spark.read.schema(schema).parquet(pageViewLogPath)

    // Hiển thị schema của DataFrame để xác định các trường dữ liệu
//    df.printSchema()

    // 3.1. Lấy url đã truy cập nhiều nhất trong ngày của mỗi guid
//    val urlCountPerGuid = df.groupBy("guid", "path").count()
//    val windowSpec = Window.partitionBy("guid").orderBy(col("count").desc)
//    val topUrlPerGuid = urlCountPerGuid.withColumn("rank", row_number().over(windowSpec)).where(col("rank") === 1).drop("count")
//    topUrlPerGuid.show()
//
//    // 3.2. Các IP được sử dụng bởi nhiều guid nhất
//    val ipCountPerGuid = df.groupBy("ip").agg(countDistinct("guid").alias("guid_count"))
//    val topIPs = ipCountPerGuid.orderBy(col("guid_count").desc).limit(1000)
//    topIPs.show()
//
//    // 3.3. Lấy top 100 các domain được truy cập nhiều nhất
//    val topDomains = df.groupBy("domain").count().orderBy(col("count").desc).limit(100)
//    topDomains.show()
//
//    // 3.4. Lấy top 10 các LocId có số lượng IP không trùng nhiều nhất
//    val topLocIds = df.groupBy("locId").agg(countDistinct("ip").alias("unique_ip_count")).orderBy(col("unique_ip_count").desc).limit(10)
//    topLocIds.show()
//
//    // 3.5. Tìm trình duyệt phổ biến nhất trong mỗi hệ điều hành (osCode và browserCode)
//    val popularBrowserByOS = df.groupBy("osCode", "browserCode").count()
//    val windowSpecOS = Window.partitionBy("osCode").orderBy(col("count").desc)
//    val topBrowserByOS = popularBrowserByOS.withColumn("rank", row_number().over(windowSpecOS)).where(col("rank") === 1).drop("count")
//    topBrowserByOS.show()

    // 3.6. Lọc các dữ liệu có timeCreate nhiều hơn cookieCreate 10 phút, và chỉ lấy field guid, domain, path, timecreate và lưu lại thành file result.dat định dạng text và tải xuống.
    val filteredData = df.filter(col("timeCreate").cast("long") > col("cookieCreate").cast("long") + lit(600000))
      .select("guid", "domain", "path", "timeCreate")

    val stringTypedData = filteredData.selectExpr(
      "CAST(guid AS STRING) AS guid",
      "CAST(domain AS STRING) AS domain",
      "CAST(path AS STRING) AS path",
      "CAST(timeCreate AS STRING) AS timeCreate"
    )
    val tabSeparatedData = stringTypedData.withColumn("concatenated",
      concat_ws("\t", col("guid"), col("domain"), col("path"), col("timeCreate"))
    ).select("concatenated")
    tabSeparatedData.write.text(outputFilePath)

  }

  def main(args: Array[String]): Unit = {
//    createParquetAndPutToHDFS()
    bai3()
  }
}
