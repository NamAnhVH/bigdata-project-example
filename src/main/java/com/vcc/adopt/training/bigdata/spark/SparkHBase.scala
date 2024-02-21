package com.vcc.adopt.training.bigdata.spark

import com.vcc.adopt.config.ConfigPropertiesLoader
import com.vcc.adopt.training.bigdata.spark.SparkHBase.bai1
import com.vcc.adopt.utils.hbase.HBaseConnectionFactory
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Get, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

import java.util


object SparkHBase {

  val spark: SparkSession = SparkSession.builder().getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  spark.conf.set("spark.sql.debug.maxToStringFields", 10000)
  private val bai1 = ConfigPropertiesLoader.getYamlConfig.getProperty("bai1")

  private def findKmean(k: Int): Unit = {
    println("Start")
    val data: DataFrame = spark.read.text(bai1)
      .toDF("data")

    // Chuyển đổi dữ liệu từ cột 'data' sang cột 'x' và 'y'
    val parsedData = data.selectExpr("split(data, ',')[0] as x", "split(data, ',')[1] as y")

    // Hiển thị dữ liệu
    parsedData.show(-1)

    // Tạo một đối tượng KMeans
    val assembler = new VectorAssembler()
      .setInputCols(Array("x", "y"))
      .setOutputCol("features")

    // Áp dụng chuyển đổi vào dữ liệu
    val dataWithFeatures = assembler.transform(parsedData)

    // Hiển thị dữ liệu đã chuyển đổi
    dataWithFeatures.show(-1)

    // Tạo một đối tượng KMeans
    val kmeans = new KMeans()
      .setK(k) // Số lượng cụm
      .setSeed(1L) // Seed để tái tạo kết quả

    // Tạo và fit mô hình KMeans
    val model = kmeans.fit(dataWithFeatures)

    // Tính toán các centroid cuối cùng
    val centroids = model.clusterCenters

    // In ra các centroids cuối cùng
    centroids.foreach(println)

    // Dự đoán cụm cho mỗi điểm dữ liệu
    val predictions = model.transform(dataWithFeatures)

    // Hiển thị kết quả
    predictions.show()
  }

  def main(args: Array[String]): Unit = {
    findKmean(3)
  }
}
