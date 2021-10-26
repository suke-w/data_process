package com.suke_w.engine

import com.alibaba.fastjson.{JSON, JSONObject}
import com.suke_w.udfs.MyUDFTest
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.SparkConf
import org.apache.spark.sql.api.java.{UDF1, UDF2, UDF3}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.Properties
import scala.collection.mutable.ArrayBuffer

/**
 * 基于sparkSQL和SparkStreaming的通用实时计算引擎
 * 目前输入数据及输出数据都是json格式
 * 1.抽取方法，优化代码，支持常见基本数据类型
 * 2.支持复杂字段类型，数组
 */
object DataProcessEngineBySparkSQL2 {
  def main(args: Array[String]): Unit = {
    var masterUrl = "local[2]" //sparkStreaming中需要指定excutor数量
    var appSecond = 5 //saprkStreaming程序的间隔时间
    var appName = "DataProcessEngineBySparkSQLTest"
    var inKafkaServers = "bigdata01:9092,bigdata02:9092,bigdata03:9092" //输入kafka地址，kafka集群
    var outKafkaServers = "bigdata01:9092,bigdata02:9092,bigdata03:9092" //输出kafka地址
    var inTopic = "stu" //输入kafka中的topic名称
    var outTopic = "stu_clean" //输出kafka中的topic名称
    var groupId = "g1" //kafka消费者的groupId
    //演示一下array数据类型 array<string> array<int>
    var inSchemaInfo = "{\"name\":\"string\",\"age\":\"int\",\"favors\":\"array<string>\"}" //输入数据Schema信息
    var outSchemaInfo = "{\"newname\":\"string\",\"age\":\"int\",\"favors\":\"array<string>\"}" //输出数据Schema信息
    var funcInfo = "[{\"name\":\"m1\",\"mainClass\":\"com.suke_w.udfs.MyUDF1\",\"param\":\"(String)\",\"returnType\":\"string\"}]" //json数组,需要使用的udf
    // 注意：针对sparksql 查询的字段顺序和目标表的字段顺序可以不一致
    // 建议输出schema信息的字段和sql中查询的字段完全一致。
    var sql = "select m1(name) as newname,age,favors from source" //用户输入的sql

    if (args.length == 12) {
      masterUrl = args(0)
      appSecond = args(1).toInt
      appName = args(2)
      inKafkaServers = args(3)
      outKafkaServers = args(4)
      inTopic = args(5)
      outTopic = args(6)
      groupId = args(7)
      inSchemaInfo = args(8)
      outSchemaInfo = args(9)
      funcInfo = args(10)
      sql = args(11)
    }


    //获取spark相关配置
    val conf = new SparkConf()
    conf.setMaster(masterUrl).setAppName(appName)
    val ssc = new StreamingContext(conf, Seconds(appSecond))
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    //kafka配置项
    val inKafkaParams = getInkafkaServerConfig(inKafkaServers, groupId)

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
          val structType = getST(inSchemaInfo)
          //2.解析输入的数据组装Row，kafka中的数据是json格式的
          val rowRDD = rdd.map(line => {
            getRowRDD(line, inSchemaInfo)
          })
          //3.建表  即创建一个基于row的dataFrame
          val rowDF = sparkSession.createDataFrame(rowRDD, structType)
          //表名建议固定使用，这样无论数据源是哪个topic，都便于记忆
          rowDF.createOrReplaceTempView("source")
          //4.注册自定义函数（此步可选，在用到的情况下需要注册）
          //4.1 注册公共自定义函数，即所有任务都需要用到这个函数，在此处注册
          sparkSession.udf.register("MyUDF", new MyUDFTest, StringType)
          //4.2 个性化自定义函数，在配置任务时动态选择使用哪个自定义函数
          //动态注册需要解析json参数
          registerUDF(funcInfo, sparkSession)
          //5.接收用户传过来的sql，执行查询操作
          val resDF = sparkSession.sql(sql)
          //6.解析sql的执行结果
          resDF.rdd.foreachPartition(pr => {
            val kafkaProducer = new KafkaProducer[String, String](getOutKafkaServerConfig(outKafkaServers))
            pr.foreach(row => {
              val resJSON = getRes(outSchemaInfo, row)
              kafkaProducer.send(new ProducerRecord(outTopic, resJSON.toString))
            })
            kafkaProducer.close()
          })
        }) //每隔appSecond配置的时间，处理一批数据，foreachRDD里就是对应的这一批数据

    ssc.start()
    ssc.awaitTermination()
  }

  def getInkafkaServerConfig(inKafkaServers: String, groupId: String): Map[String, Object] = {
    Map[String, Object](
      "bootstrap.servers" -> inKafkaServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean) //强制指定类型，避免出现问题
    )
  }

  /**
   * 组装StructType
   *
   * @param inputSchemaInfo
   * @return
   */
  def getST(inputSchemaInfo: String): StructType = {
    val inSchemaInfoJson = JSON.parseObject(inputSchemaInfo)
    val it = inSchemaInfoJson.entrySet().iterator()
    val sfBuffer = new ArrayBuffer[StructField]()
    while (it.hasNext) {
      val entry = it.next()
      val fieldName = entry.getKey
      val fieldType = entry.getValue.toString
      fieldType match {
        case "string" => sfBuffer.append(StructField(fieldName, StringType, nullable = false))
        case "int" => sfBuffer.append(StructField(fieldName, IntegerType, nullable = false))
        case "long" => sfBuffer.append(StructField(fieldName, LongType, nullable = false))
        case "double" => sfBuffer.append(StructField(fieldName, DoubleType, nullable = false))
        case "float" => sfBuffer.append(StructField(fieldName, FloatType, nullable = false))
        case "boolean" => sfBuffer.append(StructField(fieldName, BooleanType, nullable = false))
        case "array<string>" => sfBuffer.append(StructField(fieldName, DataTypes.createArrayType(StringType), nullable = false))
        case "array<int>" => sfBuffer.append(StructField(fieldName, DataTypes.createArrayType(IntegerType), nullable = false))
        case _ => sfBuffer.append(StructField(fieldName, StringType, nullable = false))
      }
    }
    StructType(sfBuffer.toArray)
  }

  /**
   * 组装RowRDD
   *
   * @param line
   * @param inSchemaInfo
   */
  def getRowRDD(line: String, inSchemaInfo: String): Row = {
    val lineJson = JSON.parseObject(line)
    //根据输入数据schema信息获取字段，根据字段获取值，进行拼接
    val inSchemaInfoJson = JSON.parseObject(inSchemaInfo)
    val it2 = inSchemaInfoJson.entrySet().iterator()
    val buffer2 = new ArrayBuffer[Any]()
    while (it2.hasNext) {
      val entry = it2.next()
      val fieldName = entry.getKey
      val fieldType = entry.getValue.toString
      fieldType match {
        case "string" => buffer2.append(lineJson.getString(fieldName))
        case "int" => buffer2.append(lineJson.getIntValue(fieldName))
        case "long" => buffer2.append(lineJson.getLongValue(fieldName))
        case "double" => buffer2.append(lineJson.getDoubleValue(fieldName))
        case "float" => buffer2.append(lineJson.getFloatValue(fieldName))
        case "boolean" => buffer2.append(lineJson.getBooleanValue(fieldName))
        case "array<string>" => buffer2.append(lineJson.getObject(fieldName,new Array[String](0).getClass)) //直接使用Array无getClass方法
        case "array<int>" => buffer2.append(lineJson.getObject(fieldName,new Array[Int](0).getClass))
        case _ => buffer2.append(lineJson.getString(fieldName))
      }
    }
    Row.fromSeq(buffer2)

  }

  /**
   * 注册个性化自定义UDF
   *
   * @param funcInfo
   * @param sparkSession
   */
  def registerUDF(funcInfo: String, sparkSession: SparkSession): Unit = {
    if (!"".equals(funcInfo.trim)) {
      val funcInfoArray = JSON.parseArray(funcInfo)
      for (i <- 0 until funcInfoArray.size()) {
        val jsonObj = funcInfoArray.getJSONObject(i)
        val name = jsonObj.getString("name")
        val mainClass = jsonObj.getString("mainClass")
        val returnType = jsonObj.getString("returnType")
        val paramArray = jsonObj.getString("param").replace("(", "").replace(")", "")
          .split(",")
        val rType = if (returnType == "String") {
          StringType
        } else {
          StringType
        }
        paramArray.size match {
          case 1 => sparkSession.udf.register(name, Class.forName(mainClass).newInstance().asInstanceOf[UDF1[String, String]], rType)
          case 2 => sparkSession.udf.register(name, Class.forName(mainClass).newInstance().asInstanceOf[UDF2[String, String, String]], rType)
          case 3 => sparkSession.udf.register(name, Class.forName(mainClass).newInstance().asInstanceOf[UDF3[String, String, String, String]], rType)
        }
      }
    }
  }

  /**
   * 组装Kafka生产者配置参数
   *
   * @param outKafkaServers
   * @return
   */
  def getOutKafkaServerConfig(outKafkaServers: String): Properties = {
    val prop = new Properties()
    prop.put("bootstrap.servers", outKafkaServers)
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    prop
  }

  /**
   * 组装结果数据
   *
   * @param outSchemaInfo
   * @param row
   * @return
   */
  def getRes(outSchemaInfo: String, row: Row): JSONObject = {
    val resJSON = new JSONObject()
    val outSchemaInfoJson = JSON.parseObject(outSchemaInfo)
    val it3 = outSchemaInfoJson.entrySet().iterator()
    while (it3.hasNext) {
      val entry = it3.next()
      val fieldName = entry.getKey
      val valueType = entry.getValue.toString
      valueType match {
        case "string" => resJSON.put(fieldName, row.getAs[String](fieldName))
        case "int" => resJSON.put(fieldName, row.getAs[Int](fieldName))
        case "long" => resJSON.put(fieldName, row.getAs[Long](fieldName))
        case "double" => resJSON.put(fieldName, row.getAs[Double](fieldName))
        case "float" => resJSON.put(fieldName, row.getAs[Float](fieldName))
        case "boolean" => resJSON.put(fieldName, row.getAs[Boolean](fieldName))
        case "array<string>" => resJSON.put(fieldName, row.getAs[Seq[String]](fieldName).toArray) //比起上面两个模块的更改，这里调试了很多次，可见开发是要多尝试，不能凭空想象
        case "array<int>" => resJSON.put(fieldName, row.getAs[Seq[Int]](fieldName).toArray)
        case _ => resJSON.put(fieldName, row.getAs[String](fieldName))
      }
    }
    resJSON
  }

}

