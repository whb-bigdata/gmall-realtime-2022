package com.ab.gmall.realtime.app.func;

import com.ab.gmall.realtime.common.GmallConfig;
import com.ab.gmall.realtime.util.DimUtil;
import com.ab.gmall.realtime.util.ThreadPoolUtil;
import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class DimAsyncFunction <T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {

    private Connection connection;
    private ThreadPoolExecutor threadPoolExecutor;
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        threadPoolExecutor = ThreadPoolUtil.getThreadPoolExecutor();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.execute(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                //1.查询维表数据,用抽象方法getKey来，从外面传入
                JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, getKey(input));

                //2.将维表数据补充到JavaBean中，用抽象方法join来，从外面传入
                if (dimInfo != null) {
                    join(input, dimInfo);
                }

                //3.将补充之后的数据输出
                resultFuture.complete(Collections.singletonList(input));
            }
        });
    }
    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        //再次查询补充信息
        System.out.println("TimeOut:" + input);
    }
}
