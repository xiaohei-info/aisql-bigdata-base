package org.aisql.bigdata.base.gojira.monster.cake

import org.aisql.bigdata.base.util.DateUtil

/**
  * Author: xiaohei
  * Date: 2019/12/18
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
class Demor(projectName: String, basePackage: String, whoami: String) {

  def getFlinkDemo: String = {
    s"""package $basePackage.demo
       |
         |import com.typesafe.scalalogging.Logger
       |import $basePackage.service.kafka.flinkimpl._
       |import org.apache.flink.api.java.utils.ParameterTool
       |import org.apache.flink.runtime.state.filesystem.FsStateBackend
       |import org.apache.flink.streaming.api.scala._
       |import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
       |
       |/**
       |  * Author: $whoami
       |  * Date: ${DateUtil.currTime}
       |  */
       |object FlinkKafkaDemo {
       |
         |  private val logger = Logger(this.getClass)
       |
         |  def main(args: Array[String]) {
       |    val params = ParameterTool.fromArgs(args)
       |    implicit val env = StreamExecutionEnvironment.getExecutionEnvironment
       |
       |    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
       |    env.getConfig.setGlobalJobParameters(params)
       |    env.setStateBackend(new FsStateBackend("hdfs://ns1/tmp/flink/checkpoint/$projectName", false))
       |    env.enableCheckpointing(1000)
       |    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
       |    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
       |    env.getCheckpointConfig.setCheckpointTimeout(60000)
       |    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
       |
       |    // val service = new xxxtableFlinkKafkaService
       |
       |    env.execute("flink-kakfa-$projectName")
       |  }
       |}
       |
    """.stripMargin
  }

  def getSparkDemo: String = {
    s"""package $basePackage.demo
       |
       |import com.typesafe.scalalogging.Logger
       |import $basePackage.service.kafka.sparkimpl._
       |import org.apache.spark.SparkConf
       |import org.apache.spark.sql.SparkSession
       |import org.apache.spark.streaming.{Seconds, StreamingContext}
       |
         |/**
       |  * Author: $whoami
       |  * Date: ${DateUtil.currTime}
       |  */
       |object SparkKafkaDemo {
       |
       |    private val logger = Logger(this.getClass)
       |
        |    def main(args: Array[String]) {
       |        val sparkConf = new SparkConf().setAppName(s"spark-kafka-$projectName")
       |            .set("spark.shuffle.consolidateFiles", "true")
       |            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
       |            //      .set("spark.streaming.stopGracefullyOnShutdown", "true")
       |            .set("spark.streaming.backpressure.enabled", "true")
       |        val spark = SparkSession.builder.config(sparkConf).getOrCreate()
       |        val streamingContext = new StreamingContext(spark.sparkContext, Seconds(10))
       |
        |         // val service = new xxxtableSparkKafkaService
       |
       |        streamingContext.start()
       |        streamingContext.awaitTermination()
       |    }
       |}
      """.stripMargin
  }
}
