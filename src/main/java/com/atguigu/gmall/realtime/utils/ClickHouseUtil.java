package com.atguigu.gmall.realtime.utils;

import com.atguigu.gmall.realtime.Funs.NoInsert;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseUtil {

    public static <T> SinkFunction getSink(String sql) {
        SinkFunction<T> sink = JdbcSink.sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T obj) throws SQLException {
                        // todo 具体的操作
                        //执行写入操作   就是将当前流中的对象属性赋值给SQL的占位符 insert into visitor_stats_0820 values(?,?,?,?,?,?,?,?,?,?,?,?)
                        //obj  就是流中的一条数据对象

                        // 获取当前类所有的属性
                        Field[] fields = obj.getClass().getDeclaredFields();
                        //跳过的属性计数，因为有时候 我只需要插入一个类中的几个属性，在不需要插入的属性上加上自定义的注解
                        int skipOffset = 0;

                        for (int i = 0; i < fields.length; i++) {
                            Field field = fields[i];
                            // field 是否加上了这个注解，加上了注解的字段不需要插入
                            NoInsert annotation = field.getAnnotation(NoInsert.class);
                            if (annotation != null) {
                                skipOffset++;
                                continue;
                            }
                            //没加注解走下面的逻辑
                            field.setAccessible(true);
                            try {
                                // 获取字段的值
                                Object o = field.get(obj);
                                preparedStatement.setObject(i + 1 - skipOffset, o);
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }
                        }

                    }
                },
                // 构造者设计模式  给batchSize属性赋值，执行执行批次大小
                new JdbcExecutionOptions.Builder().withBatchSize(5).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .withUsername("default")
                        .withUrl("jdbc:clickhouse://hadoop202:8123/default").build()
        );
        return sink;
    }
}
