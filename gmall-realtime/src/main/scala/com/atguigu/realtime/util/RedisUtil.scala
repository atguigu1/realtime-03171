package com.atguigu.realtime.util

import redis.clients.jedis.Jedis

/**
 * Author atguigu
 * Date 2020/8/18 10:37
 */
object RedisUtil {
    val host = "hadoop102"
    val port = 6379
    
    def getRedisClient = new Jedis(host, port)
}
