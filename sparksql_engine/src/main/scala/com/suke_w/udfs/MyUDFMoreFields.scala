package com.suke_w.udfs

import org.apache.spark.sql.api.java.UDF1

/**
 * 自定义函数支持返回多列字段
 */
class MyUDFMoreFields extends UDF1[String,String]{
  override def call(t1: String): String = {

  }
}
