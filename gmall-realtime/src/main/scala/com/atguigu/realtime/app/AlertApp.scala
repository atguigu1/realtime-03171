package com.atguigu.realtime.app

import java.util

import com.atguigu.realtime.bean.{AlertInfo, EventLog}
import com.atguigu.realtime.util.{ESUtil, MyKafkaUtil}
import com.atguigu.util.Constant
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

import scala.util.control.Breaks._

// json4s

/**
 * Author atguigu
 * Date 2020/8/21 8:54
 */
object AlertApp extends BaseAPp {
    override var appName: String = "AlertApp"
    implicit val format: DefaultFormats.type = org.json4s.DefaultFormats
    
    override def doSomething(ssc: StreamingContext): Unit = {
        
        val sourceStream: DStream[String] = MyKafkaUtil
            .getKafkaStream(ssc, Constant.EVENT_LOG_TOPIC)
            .window(Minutes(5), Seconds(6)) // 添加窗口, 然后后面所有的操作都是基于这个窗口
        
        val eventLogStream = sourceStream.map(json => JsonMethods.parse(json).extract[EventLog])
        
        // 1. 按着设备id进行分组
        val eventLogGroupedStream: DStream[(String, Iterable[EventLog])] = eventLogStream
            .map(log => log.mid -> log)
            .groupByKey
        
        // 2. 计算优惠券的领取情况
        val alertInfoStream: DStream[(Boolean, AlertInfo)] = eventLogGroupedStream.map {
            case (mid, logIt) =>
                // 1. 保存领取优惠券的那些uid
                val uids = new util.HashSet[String]()
                // 2. 保存优惠券对应的商品id
                val items = new util.HashSet[String]()
                // 3. 保存5分钟内所有的事件
                val events = new util.ArrayList[String]()
                
                // 4. 记录5分钟内是否有点击商品的行为. 默认没有点击
                var isClicked = false
                breakable {
                    logIt.foreach(event => {
                        events.add(event.eventId)
                        event.eventId match {
                            case "coupon" =>
                                uids.add(event.uid)
                                items.add(event.itemId)
                            case "clickItem" =>
                                isClicked = true
                                break
                            case _ => // 其他事件不做任何处理
                        }
                    })
                }
                // (是否预警, 预警信息)
                (uids.size() >= 3 && !isClicked, AlertInfo(mid, uids, items, events, System.currentTimeMillis()))
        }
        
        alertInfoStream.cache()
        alertInfoStream.print(1000)
        // 3. 把预警信息吸入到es中
        alertInfoStream
            .filter(_._1)
            .map(_._2)
            .foreachRDD(rdd => {
                // 写入到es中
                rdd.foreachPartition((infoIt: Iterator[AlertInfo]) => {
                    // 每分钟只记录一次预警   id:  mid_分钟
                    ESUtil.insertBulk("gmall_coupon_alert", infoIt.map(info => (s"${info.mid}:${System.currentTimeMillis()/1000/60}", info)))
                })
            })
    }
    
}

/*
需求：
同一设备，5分钟内三次及以上用不同账号登录并领取优惠劵，
并且在登录到领劵过程中没有浏览商品。同时达到以上要求则产生一条预警日志。
同一设备，每分钟只记录一次预警

分析:
    
    1. 同一设备   按照设备id分组 group by mid
    
    2. 5分钟内   窗口 窗口的长度是5分钟   每隔6秒中计算一次
            1. reduceByKeyAndWindow
            2. stream.window
    
    3. 三次及以上用不同账号 取优惠劵
            统计多个个账户登录并且领取优惠券的 uid的个数
            
    4. uid 没有浏览商品这个事件
    
    5. 同一设备，每分钟只记录一次预警
            // 每分钟产生预警的设备放在redis,
            
            // 使用es来完成. es本身具有幂等性
    
    
    
    
    
    
    
    
    
    
    
    
    
 


 */