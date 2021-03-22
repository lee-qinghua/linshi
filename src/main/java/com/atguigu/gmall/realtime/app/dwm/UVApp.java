package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;
import java.util.Date;

public class UVApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1  准备本地测试流环境
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //1.2 设置并行度
        env.setParallelism(4);

        //TODO 2.从kafka中读取数据
        String sourceTopic = "dwd_page_log";
        String groupId = "unique_visit_app_group";
        String sinkTopic = "dwm_unique_visit";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);
        DataStreamSource<String> jsonStrDS = env.addSource(kafkaSource);

        //TODO 3.对读取到的数据进行结构的换换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = jsonStrDS.map(jsonStr -> JSON.parseObject(jsonStr));

        KeyedStream<JSONObject, String> keybyWithMidDS =
                jsonObjDS.keyBy(data -> data.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> filteredDS = keybyWithMidDS.filter(new RichFilterFunction<JSONObject>() {
            ValueState<String> lastVisitDateState = null;
            SimpleDateFormat sdf = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                sdf = new SimpleDateFormat("yyyyMMdd");
                ValueStateDescriptor<String> lastVisitDateStateDes =
                        new ValueStateDescriptor<>("lastVisitDateState", String.class);
                //设置状态存活时间
                StateTtlConfig config = StateTtlConfig.newBuilder(Time.days(1)).build();
                lastVisitDateStateDes.enableTimeToLive(config);
                lastVisitDateState = getRuntimeContext().getState(lastVisitDateStateDes);
            }

            @Override
            public boolean filter(JSONObject jsonObj) throws Exception {
                //首先判断当前页面是否从别的页面跳转过来的
                String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                if (lastPageId != null && lastPageId.length() > 0) {
                    return false;
                }
                //获取当前访问时间
                Long ts = jsonObj.getLong("ts");
                //将当前访问时间戳转换为日期字符串
                String logDate = sdf.format(new Date(ts));
                //获取状态日期
                String lastVisitDate = lastVisitDateState.value();
                //用当前页面的访问时间和状态时间进行对比
                if (lastVisitDate != null && lastVisitDate.length() > 0 && lastVisitDate.equals(logDate)) {
                    System.out.println("已访问：lastVisitDate-" + lastVisitDate + ",||logDate:" + logDate);
                    return false;
                } else {
                    System.out.println("未访问：lastVisitDate-" + lastVisitDate + ",||logDate:" + logDate);
                    lastVisitDateState.update(logDate);
                    return true;
                }
            }
        });
        //TODO 6. 向kafka中写回，需要将json转换为String
        //6.1 json->string
        SingleOutputStreamOperator<String> kafkaDS = filteredDS.map(jsonObj -> jsonObj.toJSONString());

        //6.2 写回到kafka的dwm层
        kafkaDS.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        env.execute();
    }

}
