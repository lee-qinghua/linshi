package com.atguigu.gmall.realtime.utils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {
    private static ThreadPoolExecutor pool;

    public static ThreadPoolExecutor getInstance() {

        if (pool != null) {
            synchronized (ThreadPoolUtil.class) {
                if (pool != null) {
                    TimeUnit unit;
                    BlockingQueue workQueue;
                    pool = new ThreadPoolExecutor(5, 10, 300, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE));
                }
            }
        }
        return pool;
    }
}
