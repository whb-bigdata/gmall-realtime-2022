package com.ab.gmall.realtime.util;


import com.ab.gmall.realtime.common.GmallConfig;
import com.alibaba.fastjson.JSONObject;

import java.sql.Connection;
import java.util.List;

public class DimUtil {
    public static JSONObject getDimInfo(Connection connection, String tableName, String id) throws Exception {
        //拼接SQL
        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id ='" + id + "'";
        System.out.println("查询SQL为：" + querySql);
        //查询Phoenix
        List<JSONObject> list = JdbcUtil.queryList(connection, querySql, JSONObject.class, false);
        JSONObject dimInfo = list.get(0);


        //返回结果数据,因为带where条件，所以只有一条数据
        return dimInfo;

    }


}
