package com.suke_w.udfs

import org.apache.spark.sql.api.java.UDF1

/**
 * 自定义函数(UDF)，接收几个参数就使用对应的UDFX
 *    如果自定义函数接收一个参数，则需要继承UDF1
 *    ......
 *    泛型最后一个参数类型为返回值类型，前面的参数类型为接收数据类型
 */
class MyUDFTest extends UDF1[String, String] {
  override def call(t1: String): String = {
    t1 + "_Test"
  }
}
