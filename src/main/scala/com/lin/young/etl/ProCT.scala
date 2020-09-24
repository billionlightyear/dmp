package com.lin.young.etl

import org.apache.spark.sql.SparkSession

/**
 * Created with IntelliJ IDEA.
 * Description: 
 * User: LinY
 * Date: 2020-09-24
 * Time: 10:13
 */
object ProCT {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\Hadoop\\hadoop-2.7.6")

    val spark = SparkSession
      .builder
      .appName(this.getClass.getName)
      .master("local")
      .getOrCreate()

    val df = spark.read.load("output/part-00000-f03b3a71-f093-4f4d-ae5e-5e7a8f44a223-c000.gz.parquet")

    val ctDF = df.groupBy("provincename", "cityname").count()

    ctDF.write.partitionBy("provincename", "cityname").json("CTJson/")
  }
}
