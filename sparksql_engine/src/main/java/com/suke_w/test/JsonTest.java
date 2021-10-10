package com.suke_w.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class JsonTest {
    public static void main(String[] args) {


        String jsonMap = "{\"name\":\"string\",\"age\":\"int\"}";
        JSONObject jsonObject = JSON.parseObject(jsonMap);
    }
}
