package com.atguigu.gmallpublisher.service;

import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderMapper;
import com.atguigu.gmallpublisher.util.ESUtil;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
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
    /*

    总数:
    聚合结果:
        1. 年龄
        2. 性别
     详情:
        ...

     Map(String-> Object)

     */
    @Override
    public Map<String, Object> getSaleDetailAndAgg(String date, String keyword, int startpage, int size) throws IOException {
        // 0 最终的返回的结果
        HashMap<String, Object> result = new HashMap<>();
        // 1. 先去查询es
        // 1.1 es客户端
        JestClient cli = ESUtil.getClient();
        // 1.2 查询
        Search search = new Search.Builder(ESUtil.getDSL(date, keyword, startpage, size))
                .addIndex("gmall_sale_detail")
                .addType("_doc")
                .build();
        SearchResult searchResult = cli.execute(search);
        // 2. 得到结果, 解析出来想要的数据
        // 2.1 先放入总数
        Long total = searchResult.getTotal();
        result.put("total", total);
        // 2.2 放入详情
        List<SearchResult.Hit<HashMap, Void>> hits = searchResult.getHits(HashMap.class);
        ArrayList<Map> details = new ArrayList<>();
        for (SearchResult.Hit<HashMap, Void> hit : hits) {
            HashMap source = hit.source;
            details.add(source);
        }
        result.put("details", details);

        // 2.3 性别聚合  Map("M" -> 26, "F" -> 10)
        Map<String, Long> genderAgg = new HashMap<>();
        List<TermsAggregation.Entry> genderBuckets = searchResult.getAggregations()
                .getTermsAggregation("group_by_user_gender").getBuckets();
        for (TermsAggregation.Entry bucket : genderBuckets) {
            String key = bucket.getKey();
            Long value = bucket.getCount();
            genderAgg.put(key, value);
        }
        result.put("genderAgg", genderAgg);

        // 2.4 年龄聚合
        Map<String, Long> ageAgg = new HashMap<>();
        List<TermsAggregation.Entry> ageBuckets = searchResult.getAggregations()
                .getTermsAggregation("group_by_user_age").getBuckets();
        for (TermsAggregation.Entry bucket : ageBuckets) {
            String key = bucket.getKey();
            Long value = bucket.getCount();
            ageAgg.put(key, value);
        }
        result.put("ageAgg", ageAgg);

        // 3. 把解析后的数据封装到一个Map集合中返回
        return result;

    }
}
