package com.suke_w.udfs

import org.apache.spark.sql.api.java.UDF1
import redis.clients.jedis.Jedis

/**
 * 基于redis的自定义函数
 * 在这里每次创建新连接
 */
class MyUDFByRedis extends UDF1[String, String] {
  override def call(t1: String): String = {
    //创建jedis链接
    val jedis = new Jedis("bigdata04", 6379)
    val k1 = jedis.get("k1")
    //关闭链接
    jedis.close()
    t1 + "+" + k1
  }
}
