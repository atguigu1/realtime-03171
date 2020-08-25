package com.atguigu.gmallpublisher.util


import io.searchbox.client.JestClientFactory
import io.searchbox.client.config.HttpClientConfig


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
    
    def getClient = factory.getObject
    
    def main(args: Array[String]): Unit = {
    
    }
    
    def getDSL(date: String, keyword: String, startpage: Int, size: Int) =
        s"""
           |{
           |  "query": {
           |    "bool": {
           |      "filter": {
           |        "term": {
           |          "dt": "${date}"
           |        }
           |      },
           |      "must": [
           |        {"match": {
           |          "sku_name": {
           |            "operator": "and",
           |            "query": "${keyword}"
           |          }
           |        }}
           |      ]
           |    }
           |  },
           |  "aggs": {
           |    "group_by_user_gender": {
           |      "terms": {
           |        "field": "user_gender",
           |        "size": 2
           |      }
           |    },
           |    "group_by_user_age": {
           |      "terms": {
           |        "field": "user_age",
           |        "size": 100
           |      }
           |    }
           |  },
           |  "size": ${size},
           |  "from": ${(startpage - 1) * size}
           |}
           |""".stripMargin
    
}


/*
source:
    1. json 格式
    2. 样例类(java bean)

 */