package com.atguigu.gmallpublisher.bean;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2020/8/25 14:01
 */
public class SaleInfo {
    private Long total;
    private List<Stat> stats = new ArrayList<>();
    private List<Map> detail;

    public void addStat(Stat stat){
        stats.add(stat);
    }

    public Long getTotal() {
        return total;
    }

    public void setTotal(Long total) {
        this.total = total;
    }

    public List<Stat> getStats() {
        return stats;
    }

    public List<Map> getDetail() {
        return detail;
    }

    public void setDetail(List<Map> detail) {
        this.detail = detail;
    }
}
