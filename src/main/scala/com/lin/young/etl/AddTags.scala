package com.lin.young.etl

import org.apache.spark.sql.SparkSession

/**
 * Created with IntelliJ IDEA.
 * Description: 
 * User: LinY
 * Date: 2020-09-25
 * Time: 16:15
 */
object AddTags {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName(this.getClass.getName).getOrCreate()
    val df = spark.read.load("output/part-00000-f03b3a71-f093-4f4d-ae5e-5e7a8f44a223-c000.gz.parquet")

  }
}
