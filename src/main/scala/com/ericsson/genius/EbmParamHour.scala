package com.ericsson.genius


import com.ericsson.utils.GraceCloseUtils
import kafka.api.OffsetRequest
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import kafka.serializer.StringDecoder
import org.apache.log4j.LogManager
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.kafka.KafkaManager


object EbmParamHour extends Serializable{
    @transient lazy val log = LogManager.getRootLogger
    def initKafkaParams = {
        Map[String, String](
            "metadata.broker.list" -> "100.93.253.202:9092,100.93.253.106:9092,100.93.253.28:9092,100.93.253.30:9092,100.93.253.31:9092",
            "group.id" -> "spark_streaming",
            "auto.offset.reset" -> OffsetRequest.LargestTimeString
        )
    }

    def functionToCreateContext(): StreamingContext = {
        val sparkConf = new SparkConf().setAppName("sparkstreaming-ebmparamhour").setMaster("yarn")
//                .set("spark.streaming.kafka.maxRatePerPartition", "20000")
                .set("spark.streaming.backpressure.enabled", "true") // 激活反压
                .set("spark.streaming.backpressure.initialRate", "5000") // 启动反压功能后，读取的最大数据量
                .set("spark.kryoserializer.buffer.max", "1g")
                .set("spark.kryoserializer.buffer", "256m")
                .set("spark.executor.memory", "1g")
                .set("spark.driver.memory", "4g")
                .set("spark.sql.shuffle.partitions", "500")
        val spark: SparkSession = SparkSession.builder
                .config(sparkConf)
                .enableHiveSupport()
                .getOrCreate
        val ssc = new StreamingContext(spark.sparkContext, Seconds(5))


        val sqlContext = spark.sqlContext
        import sqlContext.implicits._

        // Create direct kafka stream with brokers and topics
        val topicsSet = "ebm_cz".split(",").toSet
        val kafkaParams = initKafkaParams
        val km = new KafkaManager(kafkaParams)
        val kafkaDirectStream = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
        log.info("======Initial Done======")

        //kafkaDirectStream.cache

        //transform
        val processedDStream = kafkaDirectStream.map(_._2)

        //action
        processedDStream.foreachRDD { rdd =>
            //log.info("### rdd count = " + rdd.count() + "### rdd partition count:" + rdd.partitions.length)
//            val df = rdd.map(_.split("\\|")).toDF()
//            df.show()
            rdd.foreachPartition(partitionOfRecords => {
                val jedis = RedisClient.pool.getResource
                jedis.select(5)//dbIndex
                val map:scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, Int]] = scala.collection.mutable.Map()
                var mapInside:scala.collection.mutable.Map[String, Int] = scala.collection.mutable.Map()
                partitionOfRecords.foreach(info => {
//                    println(info)
                    val msg = info.split("\\|")
                    val tableName = msg(0)
//                    println(tableName)
                    if(tableName.equals("L_SERVICE_REQUEST")){
                        val date_id = msg(2)
                        val hour_id = msg(3)
                        val mme_info = msg(4)
                        val result = msg(8)
                        val l_service_req_trigger = msg(12)
                        val taiArray = msg(20).split("-")
                        val taiFinal = taiArray(taiArray.length - 1)
                        val eventName1 = "Service建立成功率"
                        val key1 = date_id + "-" + hour_id + "-" +
                                    eventName1 + "-" + taiFinal + "-" + mme_info
                        val tag = "totalSample"
                        //map += (key1 -> (map.getOrElse(key1, 0) + 1))
                        mapInside = map.getOrElse(key1, scala.collection.mutable.Map())
                        mapInside.put(tag, mapInside.getOrElse(tag, 0) + 1)
                        map.put(key1, mapInside)
                        if(result.equals("SUCCESS")) {
                            val tag = "succSample"
                            mapInside = map.getOrElse(key1, scala.collection.mutable.Map())
                            mapInside.put(tag, mapInside.getOrElse(tag, 0) + 1)
                            map.put(key1, mapInside)
                        }
                        if(!l_service_req_trigger.equals("UE")){
                            val eventName2 = "寻呼成功率"
                            val key2 = date_id + "-" + hour_id + "-" +
                                    eventName2 + "-" + taiFinal + "-" + mme_info
                            mapInside = map.getOrElse(key2, scala.collection.mutable.Map())
                            mapInside.put(tag, mapInside.getOrElse(tag, 0) + 1)
                            map.put(key2, mapInside)
                            if(result.equals("SUCCESS")) {
                                val tag = "succSample"
                                mapInside = map.getOrElse(key2, scala.collection.mutable.Map())
                                mapInside.put(tag, mapInside.getOrElse(tag, 0) + 1)
                                map.put(key2, mapInside)
                            }
                        }
                    }else if(tableName.equals("L_ATTACH")){
                        val date_id = msg(2)
                        val hour_id = msg(3)
                        val mme_info = msg(4)
                        val result = msg(7)
                        val taiArray = msg(15).split("-")
                        val taiFinal = taiArray(taiArray.length - 1)
                        val eventName = "Attach成功率"
                        val key = date_id + "-" + hour_id + "-" +
                                eventName + "-" + taiFinal + "-" + mme_info
                        val tag = "totalSample"
                        mapInside = map.getOrElse(key, scala.collection.mutable.Map())
                        mapInside.put(tag, mapInside.getOrElse(tag, 0) + 1)
                        map.put(key, mapInside)
                        if(result.equals("SUCCESS")) {
                            val tag = "succSample"
                            mapInside = map.getOrElse(key, scala.collection.mutable.Map())
                            mapInside.put(tag, mapInside.getOrElse(tag, 0) + 1)
                            map.put(key, mapInside)
                        }
                    }else if(tableName.equals("L_DEDICATED_BEARER_ACTIVATE")){
                        val date_id = msg(2)
                        val hour_id = msg(3)
                        val mme_info = msg(4)
                        val result = msg(7)
                        val taiArray = msg(15).split("-")
                        val taiFinal = taiArray(taiArray.length - 1)
                        val eventName = "专载建立成功率"
                        val key = date_id + "-" + hour_id + "-" +
                                eventName + "-" + taiFinal + "-" + mme_info
                        val tag = "totalSample"
                        mapInside = map.getOrElse(key, scala.collection.mutable.Map())
                        mapInside.put(tag, mapInside.getOrElse(tag, 0) + 1)
                        map.put(key, mapInside)
                        if(result.equals("SUCCESS")) {
                            val tag = "succSample"
                            mapInside = map.getOrElse(key, scala.collection.mutable.Map())
                            mapInside.put(tag, mapInside.getOrElse(tag, 0) + 1)
                            map.put(key, mapInside)
                        }
                    }else if(tableName.equals("L_TAU")){
                        val date_id = msg(2)
                        val hour_id = msg(3)
                        val mme_info = msg(4)
                        val result = msg(7)
                        val taiArray = msg(16).split("-")
                        val taiFinal = taiArray(taiArray.length - 1)
                        val eventName = "TAU成功率"
                        val key = date_id + "-" + hour_id + "-" +
                                eventName + "-" + taiFinal + "-" + mme_info
                        val tag = "totalSample"
                        mapInside = map.getOrElse(key, scala.collection.mutable.Map())
                        mapInside.put(tag, mapInside.getOrElse(tag, 0) + 1)
                        map.put(key, mapInside)
                        if(result.equals("SUCCESS")) {
                            val tag = "succSample"
                            mapInside = map.getOrElse(key, scala.collection.mutable.Map())
                            mapInside.put(tag, mapInside.getOrElse(tag, 0) + 1)
                            map.put(key, mapInside)
                        }
                    }
                })
                for( (key, mapInside) <- map ){
                    for( (tag, v) <- mapInside){
                        jedis.hincrBy(key, tag, v)
                        jedis.expire(key, 10080)
                    }
                }
                if(jedis != null)
                    jedis.close()
            })
        }

        //update offsets in zk
        kafkaDirectStream.foreachRDD(rdd => {
            if (!rdd.isEmpty)
                km.updateZKOffsets(rdd)
        })
        ssc
    }

    def main(args: Array[String]) {
        val ssc = functionToCreateContext()
        // Start the computation
        ssc.start()
        GraceCloseUtils.stopByMarkFile(ssc)
        ssc.awaitTermination()
//        ssc.awaitTerminationOrTimeout(100000)
//        ssc.stop(true, true)
    }
}
