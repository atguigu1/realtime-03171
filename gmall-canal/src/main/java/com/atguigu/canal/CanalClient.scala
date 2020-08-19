package com.atguigu.canal

import java.net.{InetSocketAddress, SocketAddress}
import java.util

import com.alibaba.otter.canal.client.{CanalConnector, CanalConnectors}
import com.alibaba.otter.canal.protocol.CanalEntry

/**
 * Author atguigu
 * Date 2020/8/19 11:28
 */
object CanalClient {
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
            val msg = connector.get(100)  // 最多拉取100条sql导致的变化的数据
            
            // 3. 解析数据
            // 3.1 所有的数据都在这里: Entry 表示一条sql导致的表
            val entries: util.List[CanalEntry.Entry] = msg.getEntries
            if(entries != null && entries.size() > 0){
                // 3.1 解析entry
                println(entries)
            }else{
                System.out.println("没有拉倒数据, 3s后重新拉取");
                Thread.sleep(3000)
            }
        }
    }
}
