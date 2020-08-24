package com.atguigu.canal

import java.net.{InetSocketAddress, SocketAddress}
import java.util

import com.alibaba.fastjson.JSONObject
import com.alibaba.otter.canal.client.{CanalConnector, CanalConnectors}
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.{EntryType, EventType, RowChange}
import com.atguigu.util.Constant
import com.google.protobuf.ByteString
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}

import scala.collection.JavaConverters._
import scala.util.Random

/**
 * Author atguigu
 * Date 2020/8/19 11:28
 */
object CanalClient {
    /**
     * 把指定的数据,写入到指定的topic
     *
     * @param topic
     * @param msg
     * @return
     */
    def sendToKafka(topic: String, msg: String) = {
        MyKafkaUtil.send(topic, msg)
    }
    
    /**
     * 处理RowData数据
     *
     * @param rowDatas
     * @param tableName
     * @param eventType
     */
    def handleRowDatas(rowDatas: util.List[CanalEntry.RowData], tableName: String, eventType: CanalEntry.EventType) = {
        if (tableName == "order_info" && eventType == EventType.INSERT && rowDatas != null && !rowDatas.isEmpty) {
            handleRowData(rowDatas, Constant.ORDER_INFO_TOPIC)
        }else if(tableName == "order_detail" && eventType == EventType.INSERT && rowDatas != null && !rowDatas.isEmpty){
            handleRowData(rowDatas, Constant.ORDER_DETAIL_TOPIC)
        }
    }
    
    private def handleRowData(rowDatas: util.List[CanalEntry.RowData], topic: String): Unit = {
        for (rowData <- rowDatas.asScala) {
            val obj = new JSONObject()
            // 所有的列
            val columns: util.List[CanalEntry.Column] = rowData.getAfterColumnsList
            for (column <- columns.asScala) {
                val name: String = column.getName
                val value: String = column.getValue
                obj.put(name, value)
            }
            new Thread(){
                override def run(): Unit = {
                    Thread.sleep(new Random().nextInt(20 * 1000))
                    sendToKafka(topic, obj.toJSONString)
                }
            }.start()
        }
    }
    
    def main(args: Array[String]): Unit = {
        val addr: SocketAddress = new InetSocketAddress("hadoop102", 11111)
        // 1. 连接到canal
        // 1.1 创建连接器对象
        val connector: CanalConnector = CanalConnectors.newSingleConnector(addr, "example", "", "")
        // 1.2 连接到canal服务器
        connector.connect();
        // 2. 拉取数据
        // 2.1 先订阅数据 订阅那些数据那些表
        connector.subscribe("gmall.*")
        
        while (true) {
            val msg = connector.get(100) // 最多拉取100条sql导致的变化的数据
            
            // 3. 解析数据
            // 3.1 所有的数据都在这里: Entry 表示一条sql导致的表
            val entries: util.List[CanalEntry.Entry] = msg.getEntries
            if (entries != null && entries.size() > 0) {
                // 3.1 解析entry
                
                for (entry <- entries.asScala) {
                    if (entry != null && entry.hasEntryType && entry.getEntryType == EntryType.ROWDATA) {
                        
                        val storeValue: ByteString = entry.getStoreValue
                        val rowChange: RowChange = RowChange.parseFrom(storeValue)
                        val rowDatas: util.List[CanalEntry.RowData] = rowChange.getRowDatasList
                        handleRowDatas(rowDatas, entry.getHeader.getTableName, rowChange.getEventType)
                    }
                }
                
            } else {
                System.out.println(msg);
                System.out.println("没有拉倒数据, 3s后重新拉取");
                Thread.sleep(3000)
            }
            
        }
    }
}
