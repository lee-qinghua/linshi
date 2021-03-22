package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.VisitorStats;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class VisterStateApp {
    public static void main(String[] args) {
        //TODO 1.基本环境准备
        //1.1 设置流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(1);
        /*
        //1.3 检查点CK相关设置
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop202:8020/gmall/flink/checkpoint/VisitorStatsApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME","atguigu");
        */

        //TODO 2.从kafka主题中读取数据
        //2.1 声明读取的主题名以及消费者组
        String pageViewSourceTopic = "dwd_page_log";                    // 启动日志
        String uniqueVisitSourceTopic = "dwm_unique_visit";             // uv
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";      // 页面跳出
        String groupId = "visitor_stats_app";                           // 每一个app应该是不同的消费者组

        //2.2 从dwd_page_log主题中读取日志数据
        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        DataStreamSource<String> pvJsonStrDS = env.addSource(pageViewSource);

        //2.3 从dwm_unique_visit主题中读取uv数据
        FlinkKafkaConsumer<String> uvSource = MyKafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId);
        DataStreamSource<String> uvJsonStrDS = env.addSource(uvSource);

        //2.4 从dwm_user_jump_detail主题中读取跳出数据
        FlinkKafkaConsumer<String> userJumpSource = MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId);
        DataStreamSource<String> userJumpJsonStrDS = env.addSource(userJumpSource);

        // TODO 3.对各个流的数据进行结构的转换  jsonStr->VisitorStats
        // 3.1 转换pv流
        SingleOutputStreamOperator<VisitorStats> pvStatsDS = pvJsonStrDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String s) throws Exception {
                // 解析流中的数据变成我们想要的对象
                JSONObject jsonObj = JSON.parseObject(s);

                VisitorStats visitorStats = new VisitorStats(
                        "",
                        "",
                        jsonObj.getJSONObject("common").getString("vc"),
                        jsonObj.getJSONObject("common").getString("ch"),
                        jsonObj.getJSONObject("common").getString("ar"),
                        jsonObj.getJSONObject("common").getString("is_new"),
                        0L,
                        1L,
                        0L,
                        0L,
                        jsonObj.getJSONObject("page").getLong("during_time"),
                        jsonObj.getLong("ts")
                );
                return visitorStats;
            }
        });

        // 3.2 转换uv流
        SingleOutputStreamOperator<VisitorStats> uvStatsDS = uvJsonStrDS.map(
                new MapFunction<String, VisitorStats>() {
                    @Override
                    public VisitorStats map(String jsonStr) throws Exception {
                        //将json格式字符串转换为json对象
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        VisitorStats visitorStats = new VisitorStats(
                                "",
                                "",
                                jsonObj.getJSONObject("common").getString("vc"),
                                jsonObj.getJSONObject("common").getString("ch"),
                                jsonObj.getJSONObject("common").getString("ar"),
                                jsonObj.getJSONObject("common").getString("is_new"),
                                1L,
                                0L,
                                0L,
                                0L,
                                0L,
                                jsonObj.getLong("ts")
                        );
                        return visitorStats;
                    }
                }
        );
        //3.3 转换sv流（Session_count）  其实还是从dwd_page_log中获取数据
        SingleOutputStreamOperator<VisitorStats> svStatsDS = pvJsonStrDS.process(
                new ProcessFunction<String, VisitorStats>() {
                    @Override
                    public void processElement(String jsonStr, Context ctx, Collector<VisitorStats> out) throws Exception {
                        //将json格式字符串转换为json对象
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        //获取当前页面的lastPageId
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        if (lastPageId == null || lastPageId.length() == 0) {
                            VisitorStats visitorStats = new VisitorStats(
                                    "",
                                    "",
                                    jsonObj.getJSONObject("common").getString("vc"),
                                    jsonObj.getJSONObject("common").getString("ch"),
                                    jsonObj.getJSONObject("common").getString("ar"),
                                    jsonObj.getJSONObject("common").getString("is_new"),
                                    0L,
                                    0L,
                                    1L,
                                    0L,
                                    0L,
                                    jsonObj.getLong("ts")
                            );
                            out.collect(visitorStats);
                        }
                    }
                }
        );
        //3.4 转换跳出流
        SingleOutputStreamOperator<VisitorStats> userJumpStatsDS = userJumpJsonStrDS.map(
                new MapFunction<String, VisitorStats>() {
                    @Override
                    public VisitorStats map(String jsonStr) throws Exception {
                        //将json格式字符串转换为json对象
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        VisitorStats visitorStats = new VisitorStats(
                                "",
                                "",
                                jsonObj.getJSONObject("common").getString("vc"),
                                jsonObj.getJSONObject("common").getString("ch"),
                                jsonObj.getJSONObject("common").getString("ar"),
                                jsonObj.getJSONObject("common").getString("is_new"),
                                0L,
                                0L,
                                0L,
                                1L,
                                0L,
                                jsonObj.getLong("ts")
                        );
                        return visitorStats;
                    }
                }
        );

        //TODO 4. 将4条流合并到一起   注意：只能合并结构相同的流
        DataStream<VisitorStats> unionDS = pvStatsDS.union(uvStatsDS, svStatsDS, userJumpStatsDS);

        // TODO 5. 设置水位线
        unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                            @Override
                            public long extractTimestamp(VisitorStats visitorStats, long l) {
                                return visitorStats.getTs();
                            }
                        }))
                .keyBy(new KeySelector<VisitorStats0, Object>() {
                })

    }
}
