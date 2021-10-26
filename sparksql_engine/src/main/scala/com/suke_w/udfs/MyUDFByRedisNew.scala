package com.suke_w.udfs

import com.suke_w.utils.RedisUtil
import org.apache.spark.sql.api.java.UDF1

/**
 * 基于redis的自定义函数
 * 注意：
 * 连接池相关代码需要写到call函数内部，否则程序会报错，提示无法序列化
 */
class MyUDFByRedisNew extends UDF1[String, String] {
  override def call(t1: String): String = {
    RedisUtil.makePool("bigdata04", 6379, 1)
    val pool = RedisUtil.getPool
    val jedis = pool.getResource

    val value = jedis.get("k1")
    jedis.close()
    t1 + "_" + value + "_new"

  }
}
