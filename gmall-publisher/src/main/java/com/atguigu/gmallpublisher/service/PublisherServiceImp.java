package com.atguigu.gmallpublisher.service;

import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2020/8/18 15:15
 */
@Service
public class PublisherServiceImp implements PublisherService {
    @Autowired
    DauMapper dau;

    @Override
    public Long getDau(String date) {
        return dau.getDau(date);
    }

    @Override
    public Map<String, Long> getHourDau(String date) {
        List<Map<String, Object>> hourDau = dau.getHourDau(date);

        HashMap<String, Long> result = new HashMap<>();
        for (Map<String, Object> map : hourDau) {
            String hour = map.get("LOGHOUR").toString();
            Long count = (Long) map.get("COUNT");
            result.put(hour, count);
        }
        return result;
    }

    @Autowired
    OrderMapper order;

    @Override
    public Double getTotalAmount(String date) {
        Double total = order.getTotalAmount(date);
        return total == null ? 0 : total;
    }

    @Override
    public Map<String, Double> getHourAmount(String date) {
        List<Map<String, Object>> hourDau = order.getHourAmount(date);

        HashMap<String, Double> result = new HashMap<>();
        for (Map<String, Object> map : hourDau) {
            String hour = map.get("CREATE_HOUR").toString();
            Double count = ((BigDecimal) map.get("SUM")).doubleValue();
            result.put(hour, count);
        }
        return result;
    }
}
