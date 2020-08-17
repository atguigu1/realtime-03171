package com.atguigu.realtime.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.util.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author lzc
 * @Date 2020/8/17 10:35
 */
/*@Controller
@ResponseBody*/
@RestController  // === @Controller + @ResponseBody
public class LoggerController {
    @PostMapping("/log")
    public String doLog(@RequestParam("log") String log) {
        // 1. 给日志添加时间戳
        log = addTs(log);
        // 2. 把数据落盘, 给离线需求使用
        saveToDisk(log);
        // 3. 把数据发送到kafka
        sendToKafka(log);

        return "ok";
    }

    @Autowired
    KafkaTemplate<String, String> kafka;

    private void sendToKafka(String log) {
        // 不同的日志写入到不同的topic
        if (log.contains("startup")) {
            kafka.send(Constant.STARTUP_LOG_TOPIC, log); // 向topic写数据,如果topic不存在, 则自动创建:  partition? replication?
        } else {
            kafka.send(Constant.EVENT_LOG_TOPIC, log);
        }
    }

    /**
     * 把日志写入到磁盘
     *
     * @param log
     */

    private Logger logger = LoggerFactory.getLogger(LoggerController.class);

    private void saveToDisk(String log) {
        logger.info(log);
    }

    /**
     * 给日志添加时间戳
     * {"logType":"event","area":"shanghai","uid":"uid2834",
     * "eventId":"addComment","itemId":48,"os":"android",
     * "nextPageId":29,"appId":"gmall","mid":"mid_238","pageId":46}
     *
     * @param log
     * @return
     */
    private String addTs(String log) {
        JSONObject obj = JSON.parseObject(log);
        obj.put("ts", System.currentTimeMillis());
        return obj.toJSONString();
    }
}
