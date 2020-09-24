package com.lin.young.etl

import com.lin.young.util.RowUtils
import org.apache.spark.sql.SparkSession

/**
 * Created with IntelliJ IDEA.
 * Description: 
 * User: LinY
 * Date: 2020-09-24
 * Time: 20:05
 */
object ProviderDistribute {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName(this.getClass.getName).getOrCreate()

    val df = spark.read.load("output/part-00000-f03b3a71-f093-4f4d-ae5e-5e7a8f44a223-c000.gz.parquet")

    df.rdd.map(row => {
      (row.getAs[String]("ispname"), RowUtils.countUtils(row))
    })
      .reduceByKey((list1, list2) => {
        list1.zip(list2).map(t => {
          t._1 + t._2
        })
      }).take(10)
      .foreach(println)
  }
}
