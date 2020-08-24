package com.atguigu.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.realtime.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.realtime.util.{ESUtil, MyKafkaUtil, RedisUtil}
import com.atguigu.util.Constant
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis

import scala.collection.mutable


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
            .getKafkaStream(ssc, Constant.ORDER_INFO_TOPIC, "bigdata1")
            .map(json => JSON.parseObject(json, classOf[OrderInfo]))
        
        val orderDetailStream = MyKafkaUtil
            .getKafkaStream(ssc, Constant.ORDER_DETAIL_TOPIC, "bigdata2")
            .map(json => JSON.parseObject(json, classOf[OrderDetail]))
        
        (orderInfoStream, orderDetailStream)
        
    }
    
    
    /**
     * 对传入的连个参数流做fullJin
     *
     * @param orderInfoStream
     * @param orderDetailStream
     */
    def fullJoin(orderInfoStream: DStream[OrderInfo], orderDetailStream: DStream[OrderDetail]) = {
        
        /**
         * 把指定的k-v写入到redis
         *
         * @param client
         * @param expireTime
         * @param key
         * @param value
         */
        def sendToRedis(client: Jedis, expireTime: Int, key: String, value: String): Unit = {
            client.setex(key, expireTime, value)
        }
        
        def cacheOrderInfo(client: Jedis, orderInfo: OrderInfo): Unit = {
            implicit val f = org.json4s.DefaultFormats
            sendToRedis(client, 30 * 60, s"order_info:${orderInfo.id}", Serialization.write(orderInfo))
        }
        
        def cacheOrderDetail(client: Jedis, orderDetail: OrderDetail): Unit = {
            implicit val f = org.json4s.DefaultFormats
            sendToRedis(client, 30 * 60, s"order_detail:${orderDetail.order_id}:${orderDetail.id}", Serialization.write(orderDetail))
        }
        
        // 1. 把两个流转成k-v形式, 然后才可以进行join
        val orderIdToOrderInfoStream: DStream[(String, OrderInfo)] = orderInfoStream.map(orderInfo => (orderInfo.id, orderInfo))
        val orderIdToOrderDetailStream: DStream[(String, OrderDetail)] = orderDetailStream.map(orderDetail => (orderDetail.order_id, orderDetail))
        // 2. 使用全连接
        orderIdToOrderInfoStream.fullOuterJoin(orderIdToOrderDetailStream).mapPartitions((it: Iterator[(String, (Option[OrderInfo], Option[OrderDetail]))]) => {
            // 1. 建立到redis的连接
            val client: Jedis = RedisUtil.getRedisClient
            // 2. 涉及到redis的操作
            val result = it.flatMap {
                // order_info 和order_detail的数据同时到达
                case (orderId, (Some(orderInfo), Some(orderDetail))) =>
                    println(s"${orderId}  some some")
                    // 1. 把order_info信息写入到缓存
                    cacheOrderInfo(client, orderInfo)
                    // 2. 把order_info的信息和oder_detail的信息封装到一起
                    val saleDetail = SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
                    // 3. 去order_detail的缓存中查找对应的信息.  注意: 需要删除order_detail中的信息
                    // 3.1 先获取根order_id相关的所有的key 3.2. 根据key回去对应的value(order_detail)
                    import scala.collection.JavaConverters._
                    val saleDetails: mutable.Set[SaleDetail] = client.keys(s"order_detail:${orderInfo.id}:*").asScala.map(key => {
                        val orderDetailJson: String = client.get(key)
                        val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
                        // order_detail的缓存中的数据join成功之后,需要删除, 否则会出现重复数据
                        client.del(key)
                        SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
                    })
                    
                    saleDetails += saleDetail
                    saleDetails
                // order_info 和order_detail的数据没有同时到达
                case (orderId, (Some(orderInfo), None)) =>
                    println(s"${orderId} some none")
                    // 1. 把order_info信息写入到缓存
                    cacheOrderInfo(client, orderInfo)
                    // 2. 去order_detail的缓存中查找对应的信息.  注意: 需要删除order_detail中的信息
                    import scala.collection.JavaConverters._
                    val saleDetails: mutable.Set[SaleDetail] = client.keys(s"order_detail:${orderInfo.id}:*").asScala.map(key => {
                        val orderDetailJson: String = client.get(key)
                        val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
                        // order_detail的缓存中的数据join成功之后,需要删除, 否则会出现重复数据
                        client.del(key)
                        SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
                    })
                    saleDetails
                case (orderId, (None, Some(orderDetail))) =>
                    println(s"${orderId} none some")
                    // 1. 先去缓存查找对应的orderInfo
                    val orderInfoString: String = client.get(s"order_info:${orderDetail.order_id}")
                    // 2. 如果找到, 则组合成saleDetail. 如果没有找到, 应该把order_detail缓存
                    if (orderInfoString != null) {
                        val orderInfo = JSON.parseObject(orderInfoString, classOf[OrderInfo])
                        SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail) :: Nil
                    } else {
                        // a: 先把order_details缓存
                        cacheOrderDetail(client, orderDetail)
                        // b: 返回空集合
                        Nil
                    }
                
            }
            
            // 3. 关闭redis
            client.close()
            
            result
        })
        
    }
    
    /**
     * 给saleDetail添加user信息
     *
     * @param saleDetail
     * @param ssc
     * @return
     */
    def joinUserInfo(saleDetail: DStream[SaleDetail], ssc: StreamingContext): DStream[SaleDetail] = {
        // 1. 先读到user数据   DataFrame
        val url = "jdbc:mysql://hadoop102:3306/gmall?userSSL=false"
        val spark = SparkSession
            .builder()
            .config(ssc.sparkContext.getConf)
            .getOrCreate()
        import spark.implicits._
        
        def readUserInfos(ids: String) = {
            println(ids)
            spark.read
                .format("jdbc")
                // 只查询需要的用户数据, 不需要的可以不查
                // '1','2','3'..
                .option("query", s"select * from user_info where id in (${ids})")
                .option("url", url)
                .option("user", "root")
                .option("password", "aaaaaa")
                .load()
                .as[UserInfo]
                .rdd
        }
        // a  在driver 只执行一次
        // 2. 连接流和df
        saleDetail.map(saleInfo => (saleInfo.user_id, saleInfo)).transform((saleInfoRDD: RDD[(String, SaleDetail)]) => {
            saleInfoRDD.cache()
            // b  在driver 每个批次执行一次
            // 获取到所有需要用户的id
            val ids = saleInfoRDD.map(_._2.user_id).collect().mkString("'","','", "'")   // 1','2','3
            val userInfoRDD = readUserInfos(ids).map(user => (user.id, user))
            saleInfoRDD.join(userInfoRDD).map {
                case (_, (saleInfo, userInfo)) =>
                    saleInfo.mergeUserInfo(userInfo)
            }
        })
    }
    
    def save2ES(saleDetail: DStream[SaleDetail]): Unit = {
        //        saleDetail.cache()
        //        saleDetail.print(10000)
        saleDetail.foreachRDD(rdd => {
            rdd.foreachPartition(saleDetalIt => {
                ESUtil.insertBulk("gmall_sale_detail", saleDetalIt.map(sale => (sale.order_id + ":" + sale.order_detail_id, sale)))
            })
        })
    }
    
    override def doSomething(ssc: StreamingContext): Unit = {
        // 1. 获取两个流: order_info oder_detail
        val (orderInfoStream, orderDetailStream) = getOrderInfoAndOrderDetailStream(ssc)
        // 2. 对前面的两个流做join   全连接
        var saleDetail: DStream[SaleDetail] = fullJoin(orderInfoStream, orderDetailStream)
        // 3. 去mysql反查user_info, 补齐user的数据
        saleDetail = joinUserInfo(saleDetail, ssc)
        // 4. 把数据(宽表数据)写入到es
        save2ES(saleDetail)
    }
}

/*
在redis缓存order_info和order_detail的时候, 如何存?
    方便去做缓存
    方便从缓存取数据

order_detail(1):
    key                                                     value
    "order_detail:${order_id}:${order_detail_id}"              order_detail数据的json数据
    
    好处: 1.方便存取 2. 每个key可以分别设置不同的过期时间
    坏处: key可能过多
    
order_detail(2):
    key                                                     value
    "order_detail:${order_id}"                               hash
                                                            field                   value
                                                            oder_detail_id          order_detail数据的json数据
                                                            
      好处:   1. key比较少. 一个订单才有一个key
      坏处:   1. 没有办法设置不同的过期时间
      
 
order_info:
   key                                  value
   "order_info:${order_id}"             order_info的json格式的数据
   
   
 
fastjson:
    直接把json字符串解析成样例类是没有问题
    
    但是, 如果把样例类对象序列化成json字符串是不行的!


 */
