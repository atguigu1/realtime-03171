package com.atguigu.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.realtime.bean.StartupLog
import com.atguigu.realtime.util.{MyKafkaUtil, RedisUtil}
import com.atguigu.util.Constant
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * Author atguigu
 * Date 2020/8/18 9:35
 */
object DauAPp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("DauAPp")
        val ssc = new StreamingContext(conf, Seconds(3))
        
        // 1. 从kafka获取启动日志流
        val sourceStream: DStream[String] = MyKafkaUtil.getKafkaStream(ssc, Constant.STARTUP_LOG_TOPIC)
        // 2. 解析启动日志, 每条日志放入一个样例类
        val startupLogStream: DStream[StartupLog] = sourceStream.map(jsonLog => JSON.parseObject(jsonLog, classOf[StartupLog]))
        
       /* // 3. 去重, 中保留每个设备的第一次启动记录
        val filteredStartupLogStream = startupLogStream.filter(log => {
            // 把mid写到redis的set, 如果返回的是0, 保留, 如果1, 去掉
            val client: Jedis = RedisUtil.getRedisClient
            // key:"mids:2020-08-18"    value: set(mid1, mid2,...)
            val key = s"mids:${log.logDate}"
            val result = client.sadd(key, log.mid)
            //if(result == 1) true else false
            client.close()
            result == 1
        })*/
        
        // 3.去重  一个分区建立一个到redis的连接
        val filteredStartupLogStream = startupLogStream.mapPartitions(startupLogIt => {
            val client: Jedis = RedisUtil.getRedisClient
            val result = startupLogIt.filter(log => {
                val key = s"mids:${log.logDate}"
                val r = client.sadd(key, log.mid)
                r == 1
            })
            client.close()
            result
        })
        
        // 5. 把数据写入到hbase(phoenix)   print/save../foreachRDD
        import org.apache.phoenix.spark._
        filteredStartupLogStream.foreachRDD(rdd => {
            rdd.saveToPhoenix(
                "GMALL_DAU_0317",
                Seq("MID", "UID", "APPID", "AREA", "OS", "CHANNEL", "LOGTYPE", "VERSION", "TS", "LOGDATE", "LOGHOUR"),
                zkUrl = Option("hadoop102,hadoop103,hadoop104:2181")
            )
        })
        
        // 启动流
        ssc.start()
        // 阻止进程退出
        ssc.awaitTermination()
    }
}

/*
val filteredStartupLogStream =  startupLogStream.filter(log => {
            // 把mid写到redis的set, 如果返回的是0, 保留, 如果1, 去掉
    val client: Jedis = RedisUtil.getRedisClient
    // key:"mids:2020-08-18"    value: set(mid1, mid2,...)
    val key = s"mids:${log.logDate}"
    val result = client.sadd(key, log.mid)
    client.close()
    //if(result == 1) true else false
    result == 1
})


存在的问题: 与redis的连接数过多, 对redis造成压力

解决: 应该一个分区一个连接

 */