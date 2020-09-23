package com.lin.young.util

/**
 * Created with IntelliJ IDEA.
 * Description: 
 * User: LinY
 * Date: 2020-09-23
 * Time: 17:20
 */
object String2Utils {
  def str2Double(str: String) = {
    try {
      str.toDouble
    } catch {
      case _: Exception => 0.0
    }
  }

  def str2Int(str: String) =
    try {
      str.toInt
    } catch {
      case _: Exception => 0
    }

}
