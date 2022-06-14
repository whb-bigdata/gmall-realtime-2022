package com.ab.gmall.realtime.util;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {
    private static ThreadPoolExecutor threadPoolExecutor = null;

    private ThreadPoolUtil() {
    }

    public static ThreadPoolExecutor getThreadPoolExecutor() {
        //双重校验的单例
        if(threadPoolExecutor == null) {
            synchronized (ThreadPoolUtil.class) {
                if (threadPoolExecutor == null) {
                    //创建线程池
                    threadPoolExecutor = new ThreadPoolExecutor(4
                            ,20
                            , 60
                            , TimeUnit.SECONDS
                            , new LinkedBlockingDeque<>());//工作队列满了，才会开新的线程，最多到20个
                }

            }
        }

        return threadPoolExecutor;

    }

    public static void main(String[] args) {

    }
}
