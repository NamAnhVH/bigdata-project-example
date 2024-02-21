package com.vcc.adopt.training.bigdata.spark

import com.vcc.adopt.config.ConfigPropertiesLoader

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


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
    val parsedData = data.selectExpr("cast(split(data, ',')[0] as double) as x", "cast(split(data, ',')[1] as double) as y")

    // Hiển thị dữ liệu
//    parsedData.show()

    // Tạo một đối tượng KMeans
    val assembler = new VectorAssembler()
      .setInputCols(Array("x", "y"))
      .setOutputCol("features")

    val dataWithFeatures = assembler.transform(parsedData)
//    dataWithFeatures.show()

    val kmeans = new KMeans()
      .setK(k) // Số lượng cụm
      .setSeed(1L) // Seed để tái tạo kết quả

    val model = kmeans.fit(dataWithFeatures)

    val centroids = model.clusterCenters
    centroids.foreach(println)

    val predictions = model.transform(dataWithFeatures)
    predictions.show(30000)
  }

  def main(args: Array[String]): Unit = {
    findKmean(3)
  }
}
