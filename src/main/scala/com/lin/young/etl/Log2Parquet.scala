package com.lin.young.etl

import com.lin.young.bean.logs
import com.lin.young.util.String2Utils
import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory

/**
 * Created with IntelliJ IDEA.
 * Description: 
 * User: LinY
 * Date: 2020-09-23
 * Time: 14:36
 */
object Log2Parquet {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir","D:\\Hadoop\\hadoop-2.7.6")
    //判断参数是否为空
    if (args.length != 2) {
      sys.exit()
    }

    //接收参数
    val Array(inputPath, outputPath) = args

    //创建上下文
    val spark = SparkSession.builder()
      .master("local")
      .appName(this.getClass.getName)
      .getOrCreate()

    val config = ConfigFactory.load()

    val sc = spark.sqlContext

    sc.setConf("spark.sql.parquet.compression.codec", config.getString("spark.sql.parquet.compression.codec"))
    sc.setConf("spark.serializer", config.getString("spark.serializer"))

    //导入隐式转换
    import spark.implicits._
    val lines = spark.sparkContext.textFile(inputPath)
    lines.map(_.split(",", -1)).filter(_.length >= 85)
      .map(arr => {
        logs(
          arr(0),
          String2Utils.str2Int(arr(1)),
          String2Utils.str2Int(arr(2)),
          String2Utils.str2Int(arr(3)),
          String2Utils.str2Int(arr(4)),
          arr(5),
          arr(6),
          String2Utils.str2Int(arr(7)),
          String2Utils.str2Int(arr(8)),
          String2Utils.str2Double(arr(9)),
          String2Utils.str2Double(arr(10)),
          arr(11),
          arr(12),
          arr(13),
          arr(14),
          arr(15),
          arr(16),
          String2Utils.str2Int(arr(17)),
          arr(18),
          arr(19),
          String2Utils.str2Int(arr(20)),
          String2Utils.str2Int(arr(21)),
          arr(22),
          arr(23),
          arr(24),
          arr(25),
          String2Utils.str2Int(arr(26)),
          arr(27),
          String2Utils.str2Int(arr(28)),
          arr(29),
          String2Utils.str2Int(arr(30)),
          String2Utils.str2Int(arr(31)),
          String2Utils.str2Int(arr(32)),
          arr(33),
          String2Utils.str2Int(arr(34)),
          String2Utils.str2Int(arr(35)),
          String2Utils.str2Int(arr(36)),
          arr(37),
          String2Utils.str2Int(arr(38)),
          String2Utils.str2Int(arr(39)),
          String2Utils.str2Double(arr(40)),
          String2Utils.str2Double(arr(41)),
          String2Utils.str2Int(arr(42)),
          arr(43),
          String2Utils.str2Double(arr(44)),
          String2Utils.str2Double(arr(45)),
          arr(46),
          arr(47),
          arr(48),
          arr(49),
          arr(50),
          arr(51),
          arr(52),
          arr(53),
          arr(54),
          arr(55),
          arr(56),
          String2Utils.str2Int(arr(57)),
          String2Utils.str2Double(arr(58)),
          String2Utils.str2Int(arr(59)),
          String2Utils.str2Int(arr(60)),
          arr(61),
          arr(62),
          arr(63),
          arr(64),
          arr(65),
          arr(66),
          arr(67),
          arr(68),
          arr(69),
          arr(70),
          arr(71),
          arr(72),
          String2Utils.str2Int(arr(73)),
          String2Utils.str2Double(arr(74)),
          String2Utils.str2Double(arr(75)),
          String2Utils.str2Double(arr(76)),
          String2Utils.str2Double(arr(77)),
          String2Utils.str2Double(arr(78)),
          arr(79),
          arr(80),
          arr(81),
          arr(82),
          arr(83),
          String2Utils.str2Int(arr(84))
        )
      }).toDF().write.save(outputPath)


  }
}

