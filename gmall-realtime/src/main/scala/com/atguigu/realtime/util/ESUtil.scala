package com.atguigu.realtime.util

import com.atguigu.realtime.bean.AlertInfo
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index}
import org.apache.spark.rdd.RDD

/**
 * Author atguigu
 * Date 2020/8/22 13:51
 */
object ESUtil {
    // 先得到es的客户端
    val factory = new JestClientFactory
    val esUrl = "http://hadoop102:9200"
    val config = new HttpClientConfig.Builder(esUrl)
        .maxTotalConnection(100)
        .connTimeout(1000 * 10)
        .readTimeout(1000 * 10)
        .build()
    factory.setHttpClientConfig(config)
    
    
    def main(args: Array[String]): Unit = {
        /*insertSingle("user", User("a", 10))
        insertSingle("user", User("b", 20), "aaaaaa")*/
        
        /*insertBulk("user", List(
            ("1", User("a", 20)),
            ("2", User("b", 30)),
            ("3", User("c", 40))).toIterator)*/
    
        insertBulk("user", List(
            User("a", 20),
            User("b", 30),
            ("3", User("c", 40))).toIterator)
    
    
    }
    
    def insertBulk(index: String, sources: Iterator[Object]) = {
        val client: JestClient = factory.getObject
        val bulkBuilder = new Bulk.Builder()
                .defaultIndex(index)
                .defaultType("_doc")
    
        sources.foreach{
            case (id: String, source) =>
                val action = new Index.Builder(source)
                    .id(id)
                    .build()
    
                bulkBuilder.addAction(action)
            case source =>
                val action = new Index.Builder(source)
                    .build()
    
                bulkBuilder.addAction(action)
        }
        
        client.execute(bulkBuilder.build())
    
        client.shutdownClient()
    }
    
    
    def insertSingle(index: String, source: Object, id: String = null) = {
        val client: JestClient = factory.getObject
    
        val action: Index = new Index.Builder(source)
            .index(index)
            .`type`("_doc")
            .id(id)
            .build()
        client.execute(action)
        client.shutdownClient()
    }
    
    
    
    implicit class RichRDD(rdd: RDD[AlertInfo]){
        def saveToES(index: String) = {
            rdd.foreachPartition((infoIt: Iterator[AlertInfo]) => {
                // 每分钟只记录一次预警   id:  mid_分钟
                ESUtil.insertBulk(index, infoIt.map(info => (s"${info.mid}:${System.currentTimeMillis()/1000/60}", info)))
            })
        }
    }
    
    
}

case class User(name: String, age: Int)

/*
source:
    1. json 格式
    2. 样例类(java bean)

 */