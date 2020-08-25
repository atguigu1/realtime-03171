package com.atguigu.gmallpublisher.bean;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author lzc
 * @Date 2020/8/25 13:57
 */
public class Stat {
    private String title;
    private List<Option> options = new ArrayList<>();

    public void addOption(Option opt){
        options.add(opt);
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<Option> getOptions() {
        return options;
    }
}
