package com.ab.gmall.realtime.app.func;

import com.ab.gmall.realtime.common.GmallConfig;
import com.ab.gmall.realtime.util.PhoenixUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {


    //value：{"sinkTable":"dim_xxx","database":"gmall","table":"base_trademark","type":"insert","ts":1592270938,"xid":13090,"xoffset":1573,"data":{"id":"12","tm_name":"atguigu"},"old":{}}
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        String sinkTable = value.getString("sinkTable");
        value.remove("sinkTable");
        PhoenixUtil.insertValues(sinkTable,value);



    }


}
