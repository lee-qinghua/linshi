package com.atguigu.gmall.realtime.Funs;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.DimUtil;
import com.atguigu.gmall.realtime.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.text.ParseException;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;

public abstract class DimJoinAsyncIO<T> extends RichAsyncFunction<T, T> {
    private String tableName;

    public DimJoinAsyncIO(String tableName) {
        this.tableName = tableName;
    }

    public abstract String getId(T t);

    public abstract void join(T t, JSONObject jsonObject) throws ParseException;

    //线程池对象的父接口生命（多态）
    private ExecutorService executorService;

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化线程池对象
        System.out.println("初始化线程池对象");
        executorService = ThreadPoolUtil.getInstance();
    }

    // invoke方法就是查询数据库的操作
    @Override
    public void asyncInvoke(T t, ResultFuture<T> resultFuture) throws Exception {
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    long start = System.currentTimeMillis();
                    // 查询数据，需要知道查询哪个表？查询id的值
                    String id = getId(t);

                    //System.out.println("维度数据Json格式：" + dimInfoJsonObj);
                    JSONObject jsonObject = DimUtil.getDimInfo(tableName, id);

                    if (jsonObject != null) {
                        //维度关联  流中的事实数据和查询出来的维度数据进行关联
                        join(t, jsonObject);
                    }
                    //System.out.println("维度关联后的对象:" + obj);
                    long end = System.currentTimeMillis();
                    System.out.println("异步维度查询耗时" + (end - start) + "毫秒");
                    //将关联后的数据数据继续向下传递
                    resultFuture.complete(Arrays.asList(t));
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException("异步查询维表失败！");
                }
            }
        });
    }

    // 异步io查询失败时的操作
    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {

    }
}
