package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.Funs.DimJoinAsyncIO;
import com.atguigu.gmall.realtime.bean.OrderDetail;
import com.atguigu.gmall.realtime.bean.OrderInfo;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class OrderWideApp {
    // 订单表流和订单明细表流join
    // 再与需要的各种维度关联
    public static void main(String[] args) {
        //TODO 1.基本环境准备
        //1.1  准备本地测试流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1.2 设置并行度
        env.setParallelism(4);

        //1.3 设置Checkpoint
        //env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(60000);
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/checkpoint/uniquevisit"))

        //TODO 2.从Kafka的DWD层读取订单和订单明细数据
        //2.1 声明相关的主题以及消费者组
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";
        //2.2 读取订单主题数据
        FlinkKafkaConsumer<String> orderInfoSource = MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId);
        DataStreamSource<String> orderInfoJsonStrDS = env.addSource(orderInfoSource);

        //2.3 读取订单明细数据
        FlinkKafkaConsumer<String> orderDetailSource = MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId);
        DataStreamSource<String> orderDetailJsonStrDS = env.addSource(orderDetailSource);

        //TODO 3.对读取的数据进行结构的转换      jsonString -->OrderInfo|OrderDetail
        //3.1 转换订单数据结构
        SingleOutputStreamOperator<OrderInfo> orderInfoDS = orderInfoJsonStrDS.map(
                new RichMapFunction<String, OrderInfo>() {
                    SimpleDateFormat sdf = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }

                    @Override
                    public OrderInfo map(String jsonStr) throws Exception {
                        OrderInfo orderInfo = JSON.parseObject(jsonStr, OrderInfo.class);
                        orderInfo.setCreate_ts(sdf.parse(orderInfo.getCreate_time()).getTime());
                        return orderInfo;
                    }
                }
        );

        //3.2 转换订单明细数据结构
        SingleOutputStreamOperator<OrderDetail> orderDetailDS = orderDetailJsonStrDS.map(
                new RichMapFunction<String, OrderDetail>() {
                    SimpleDateFormat sdf = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }

                    @Override
                    public OrderDetail map(String jsonStr) throws Exception {
                        OrderDetail orderDetail = JSON.parseObject(jsonStr, OrderDetail.class);
                        orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());
                        return orderDetail;
                    }
                }
        );
        //TODO 4. 两个流指定事件时间字段
        //4.1 订单指定事件时间字段
        SingleOutputStreamOperator<OrderInfo> orderInfoWithTsDS = orderInfoDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                            @Override
                            public long extractTimestamp(OrderInfo orderInfo, long recordTimestamp) {
                                return orderInfo.getCreate_ts();
                            }
                        })
        );
        //4.2 订单明细指定事件时间字段
        SingleOutputStreamOperator<OrderDetail> orderDetailWithTsDS = orderDetailDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<OrderDetail>() {
                                    @Override
                                    public long extractTimestamp(OrderDetail orderDetail, long recordTimestamp) {
                                        return orderDetail.getCreate_ts();
                                    }
                                }
                        )
        );
        //TODO 5.按照订单id进行分组  指定关联的key
        KeyedStream<OrderInfo, Long> orderInfoKeyedDS = orderInfoWithTsDS.keyBy(OrderInfo::getId);
        KeyedStream<OrderDetail, Long> orderDetailKeyedDS = orderDetailWithTsDS.keyBy(OrderDetail::getOrder_id);

        //TODO 6.使用intervalJoin对订单和订单明细进行关联
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoKeyedDS.intervalJoin(orderDetailKeyedDS)
                .between(Time.milliseconds(-5), Time.milliseconds(5))
                .process(
                        new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                            @Override
                            public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context context, Collector<OrderWide> collector) throws Exception {
                                collector.collect(new OrderWide(orderInfo, orderDetail));
                            }
                        }
                );

        //TODO 7.关联用户维度
        // 优化方法1：加入旁路缓存查询mysql，先查redis把结果放在redis中 设置过期时间。 维表更改的时候 canal监控删除对应的缓存
        // 优化方法2：flink本身的优化方法 异步IO AsyncIO

        // apply the async I/O transformation
        SingleOutputStreamOperator<OrderWide> resultStream =
                AsyncDataStream.unorderedWait(orderWideDS, new DimJoinAsyncIO<OrderWide>("tablename") {
                    @Override
                    public String getId(OrderWide orderWide) {
                        return orderWide.getUser_id().toString();
                    }

                    // join方法就是在主流的基础上加一些属性，最后还是返回的主流
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) throws ParseException {
                        //获取用户生日
                        String birthday = dimInfoJsonObj.getString("BIRTHDAY");
                        //定义日期转换工具类
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        //将生日字符串转换为日期对象
                        Date birthdayDate = sdf.parse(birthday);
                        //获取生日日期的毫秒数
                        Long birthdayTs = birthdayDate.getTime();

                        //获取当前时间的毫秒数
                        Long curTs = System.currentTimeMillis();

                        //年龄毫秒数
                        Long ageTs = curTs - birthdayTs;
                        //转换为年龄
                        Long ageLong = ageTs / 1000L / 60L / 60L / 24L / 365L;
                        Integer age = ageLong.intValue();

                        //将维度中的年龄赋值给订单宽表中的属性
                        orderWide.setUser_age(age);

                        //将维度中的性别赋值给订单宽表中的属性
                        orderWide.setUser_gender(dimInfoJsonObj.getString("GENDER"));
                    }
                }, 1000, TimeUnit.MILLISECONDS, 100);

    }
}
