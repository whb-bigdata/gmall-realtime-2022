package com.ab.gmall.realtime.app.func;

import com.ab.gmall.realtime.bean.TableProcess;
import com.ab.gmall.realtime.common.GmallConfig;
import com.ab.gmall.realtime.util.PhoenixUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject,String,JSONObject> {
    private MapStateDescriptor<String, TableProcess> stateDescriptor;
    private Connection connection;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> stateDescriptor) {
        this.stateDescriptor = stateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        connection.setSchema(GmallConfig.HBASE_SCHEMA);
    }

    //Value:{"before":null,"after":{"source_table":1,"sink_table":"三星","sink_columns":"/static/default.jpg"....},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1649744439676,"snapshot":"false","db":"gmall-210927-flink","sequence":null,"table":"base_trademark","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1649744439678,"transaction":null}
    @Override
    public void processBroadcastElement(String s, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        //todo 1.获取并解析数据为javaBean对象
        JSONObject jsonObject = JSON.parseObject(s);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);
        //todo 2.校验表是否存在，如果不存在则建表
        checkTable(tableProcess.getSinkTable(),tableProcess.getSinkColumns(),tableProcess.getSinkPk(),tableProcess.getSinkExtend());

    }
    //在Phoenix中校验并建表 create table if not exists db.tn(id varchar primary key,name varchar,....) xxx
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        PreparedStatement preparedStatement = null;
        try {
            //处理字段
            if (sinkPk == null || sinkPk.equals("")) {
                sinkPk = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }
            StringBuilder sql = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");
            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {
                //获取字段
                String column = columns[i];
                //判断是否为主键字段
                if (sinkPk.equals(column)) {
                    sql.append(column).append(" varchar primary key");
                } else {
                    sql.append(column).append(" varchar");
                }
                //不是最后一个字段，则添加"，"
                if (i < columns.length - 1) {
                    sql.append(",");
                }
            }
            sql.append(")").append(sinkExtend);
            System.out.println(sql);
            //预编译SQL
            preparedStatement = connection.prepareStatement(sql.toString());

            //执行
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException("建表" + sinkTable + "失败！");
        } finally {
            //资源释放
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    //Value:{"database":"gmall","table":"cart_info","type":"update","ts":1592270938,"xid":13090,"xoffset":1573,"data":{"id":100924,"user_id":"93","sku_id":16,"cart_price":4488.00,"sku_num":1,"img_url":"http://47.93.148.192:8080/group1/M00/00/02/rBHu8l-sklaALrngAAHGDqdpFtU741.jpg","sku_name":"华为 HUAWEI P40 麒麟990 5G SoC芯片 5000万超感知徕卡三摄 30倍数字变焦 8GB+128GB亮黑色全网通5G手机","is_checked":null,"create_time":"2020-06-14 09:28:57","operate_time":null,"is_ordered":1,"order_time":"2021-10-17 09:28:58","source_type":"2401","source_id":null},"old":{"is_ordered":0,"order_time":null}}
    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
    //1.获取广播变量配置数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(stateDescriptor);
        TableProcess tableProcess = broadcastState.get(jsonObject.getString("table"));
        String type = jsonObject.getString("type");
        if(tableProcess != null && ("bootstrap-insert".equals(type) || "insert".equals(type) || "update".equals(type))){
            //2.根据sinkTable配置信息过滤字段
            filter(jsonObject.getJSONObject("data"),tableProcess.getSinkColumns());
            //3.补充sinkTable字段写出
            jsonObject.put("sinkTable",tableProcess.getSinkTable());
            collector.collect(jsonObject);
        }

    }
    /**
     * @param data        {"id":"11","tm_name":"atguigu","logo_url":"/aaa/bbb"}
     * @param sinkColumns id,tm_name
     *                    <p>
     *                    {"id":"11","tm_name":"atguigu"}
     */
    private void filter(JSONObject data, String sinkColumns) {
        String[] split = sinkColumns.split(",");
        List<String> columnsList = Arrays.asList(split);
        data.entrySet().removeIf(co -> columnsList.contains(co.getKey()));

    }

}
