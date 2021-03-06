package com.atguigu.canal

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 * Author atguigu
 * Date 2020/8/19 14:23
 */
object MyKafkaUtil {
    val props = new Properties()
    props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    
    
    val producer = new KafkaProducer[String, String](props)
    
    def send(topic: String, content: String): Unit = {
        producer.send(new ProducerRecord[String, String](topic, content))
    }
}
