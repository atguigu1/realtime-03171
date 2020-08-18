package com.atguigu.realtime.app

import com.atguigu.realtime.util.MyKafkaUtil
import com.atguigu.util.Constant
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/8/18 9:35
 */
object DauAPp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("DauAPp")
        val ssc = new StreamingContext(conf, Seconds(3))
    
        val sourceStream: DStream[String] = MyKafkaUtil.getKafkaStream(ssc, Constant.STARTUP_LOG_TOPIC)
        sourceStream.print(1000)
        
        
        // 启动流
        ssc.start()
        // 阻止进程退出
        ssc.awaitTermination()
    }
}
