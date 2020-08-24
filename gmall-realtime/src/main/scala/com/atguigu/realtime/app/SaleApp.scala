package com.atguigu.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.realtime.bean.{OrderDetail, OrderInfo}
import com.atguigu.realtime.util.{MyKafkaUtil, RedisUtil}
import com.atguigu.util.Constant
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis


/**
 * Author atguigu
 * Date 2020/8/24 10:06
 */
object SaleApp extends BaseAPp {
    override var appName: String = "SaleApp"
    
    /**
     * 获取order_info和oder_detail的流数据
     *
     * @param ssc
     */
    def getOrderInfoAndOrderDetailStream(ssc: StreamingContext): (DStream[OrderInfo], DStream[OrderDetail]) = {
        val orderInfoStream = MyKafkaUtil
            .getKafkaStream(ssc, Constant.ORDER_INFO_TOPIC, "bigdata")
            .map(json => JSON.parseObject(json, classOf[OrderInfo]))
        
        val orderDetailStream = MyKafkaUtil
            .getKafkaStream(ssc, Constant.ORDER_DETAIL_TOPIC, "bigdata")
            .map(json => JSON.parseObject(json, classOf[OrderDetail]))
        
        (orderInfoStream, orderDetailStream)
        
    }
    
    
    
    /**
     * 对传入的连个参数流做fullJin
     *
     * @param orderInfoStream
     * @param orderDetailStream
     */
    def fullJoin(orderInfoStream: DStream[OrderInfo], orderDetailStream: DStream[OrderDetail]): Unit = {
        def cacheOrderInfo(client: Jedis, orderInfo: OrderInfo): Unit= ???
        
        
        
        // 1. 把两个流转成k-v形式, 然后才可以进行join
        val orderIdToOrderInfoStream: DStream[(String, OrderInfo)] = orderInfoStream.map(orderInfo => (orderInfo.id, orderInfo))
        val orderIdToOrderDetailStream: DStream[(String, OrderDetail)] = orderDetailStream.map(orderDetail => (orderDetail.order_id, orderDetail))
        // 2. 使用全连接
        orderIdToOrderInfoStream.fullOuterJoin(orderIdToOrderDetailStream).mapPartitions((it: Iterator[(String, (Option[OrderInfo], Option[OrderDetail]))]) => {
            // 1. 建立到redis的连接
            val client: Jedis = RedisUtil.getRedisClient
            // 2. 涉及到redis的操作
            val result = it.map {
                // order_info 和order_detail的数据同时到达
                case (orderId, (Some(orderInfo), Some(orderDetail))) =>
                // 1. 把order_info信息写入到缓存
                cacheOrderInfo(client, orderInfo)
                // 2. 把order_info的信息和oder_detail的信息封装到一起
                
                // 3. 去order_detail的缓存中查找对应的信息.  注意: 需要删除order_detail中的信息
                
                // order_info 和order_detail的数据没有同时到达
                case (orderId, (Some(orderInfo), None)) =>
                
                case (orderId, (None, Some(orderDetail))) =>
                
            }
            
            // 3. 关闭redis
            client.close()
            
            result
        })
        
    }
    
    override def doSomething(ssc: StreamingContext): Unit = {
        // 1. 获取两个流: order_info oder_detail
        val (orderInfoStream, orderDetailStream) = getOrderInfoAndOrderDetailStream(ssc)
        // 2. 对前面的两个流做join   全连接
        fullJoin(orderInfoStream, orderDetailStream)
        
        // 3. 去mysql反查user_info, 补齐user的数据
        
        
        // 4. 把数据(宽表数据)写入到es
    }
}
