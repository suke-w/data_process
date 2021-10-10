package com.suke_w.engine

import com.alibaba.fastjson.JSON
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 基于sparkSQL和SparkStreaming的通用实时计算引擎
 * 目前输入数据及输出数据都是json格式
 */
object DataProcessEngineBySparkSQLTest {
  def main(args: Array[String]): Unit = {
    val masterUrl = "local[2]"  //sparkStreaming中需要指定excutor数量
    val appSecond = 5  //saprkStreaming程序的间隔时间
    val appName  = "DataProcessEngineBySparkSQLTest"
    val inKafkaServers = "bigdata01:9092,bigdata02:9092,bigdata03:9092" //输入kafka地址，kafka集群
    val outKafkaServers = "bigdata01:9092,bigdata02:9092,bigdata03:9092" //输出kafka地址
    val inTopic = "stu"  //输入kafka中的topic名称
    val outTopic = "stu_clean" //输出kafka中的topic名称
    val groupId = "g1" //kafka消费者的groupId
    val inSchemaInfo = "{\"name\":\"string\",\"age\":\"int\"}" //输入数据Schema信息
    val outSchemaInfo = "{\"newname\":\"string\",\"age\":\"int\"}" //输出数据Schema信息
    val funcInfo = "[{\"name\":\"m1\",\"mainClass\":\"com.imooc.engine.udfs.MyUDF1\",\"param\":\"(String)\",\"returnType\":\"string\"}]" //json数组,需要使用的udf
    // 注意：针对sparksql 查询的字段顺序和目标表的字段顺序可以不一致
    // 建议输出schema信息的字段和sql中查询的字段完全一致。
    val sql = "select m1(name) as newname,age from source" //用户输入的sql


    //获取spark相关配置
    val conf = new SparkConf()
    conf.setMaster(masterUrl).setAppName(appName)
    val ssc = new StreamingContext(conf, Seconds(appSecond))
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    //kafka配置项
    val inKafkaParams = Map[String, Object](
      "bootstrap.servers" -> inKafkaServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean) //强制指定类型，避免出现问题
    )

    //指定输入topic名称
    val topics = Array(inTopic)

    //获取输入kafka数据流
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, inKafkaParams)
    )

    //stream数据既有key又有value，value就是Kafka中一行一行的数据
    stream.map(_.value())
      .foreachRDD(
        //此方法内，每隔五秒钟可以获取一批RDD数据，接下来将这一批RDD数据注册成DataFrame，然后就可以使用sql做一些查询操作
        rdd => {
          //1.获取输入数据的schema信息，组装structType
          val inSchemaInfo = JSON.parseObject(inSchemaInfo)
          val it = inSchemaInfo.entrySet().iterator()


          //2.解析输入的数据组装Row，kafka中的数据是json格式的

          //3.建表

          //4.注册自定义函数（在用到的情况下）

          //5.接收用户传过来的sql，执行查询操作

          //6.解析sql的执行结果

        }
      ) //每隔appSecond配置的时间，处理一批数据，foreachRDD里就是对应的这一批数据

  }

}
