package com.suke_w.udfs

import org.apache.spark.sql.api.java.UDF1

class MyUDF1 extends UDF1[String, String] {
  override def call(t1: String): String = {
    t1 + "_new"
  }
}
