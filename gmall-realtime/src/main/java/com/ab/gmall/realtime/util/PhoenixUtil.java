package com.ab.gmall.realtime.util;

import com.ab.gmall.realtime.common.GmallConfig;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class PhoenixUtil {
    private static Connection conn;
    //初始化sql执行环境
    public static void initializeConnection(){
        try {
            Class.forName(GmallConfig.PHOENIX_DRIVER);
            conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
            conn.setSchema(GmallConfig.HBASE_SCHEMA);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Phoenix 表数据导入方法
     * @param sinkTable  写入数据的phoenix目标表名
     * @param data  待写入的数据
     */
    public static void insertValues(String sinkTable, JSONObject data){
        if(conn == null){
            synchronized (PhoenixUtil.class){
                if (conn == null){
                    initializeConnection();
                }
            }
        }
        //获取字段名
        Set<String> columns = data.getJSONObject("data").keySet();
        //获取字段对应的值
        Collection<Object> values = data.getJSONObject("data").values();
        //拼接字段名
        String cloumnStr = StringUtils.join(columns, ",");
        //拼接字段值
        String valueStr = StringUtils.join(values, "','");
        //拼接插入语句
        String sql = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" + cloumnStr + ") values ('"
                        + valueStr + "')";
        PreparedStatement preparedStatement = null;
        try{
            System.out.println("插入语句为：" + sql);
            preparedStatement = conn.prepareStatement(sql);
            preparedStatement.execute();
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally{
            if(preparedStatement == null){
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
