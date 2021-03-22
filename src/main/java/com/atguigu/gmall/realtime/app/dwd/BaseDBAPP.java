package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;

public class BaseDBAPP {
    public static void main(String[] args) {
        //TODO 1.准备环境
        //1.1 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并新度
        env.setParallelism(1);
        //1.3 开启Checkpoint，并设置相关的参数
        //env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(60000);
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/checkpoint/basedbapp"));
        //重启策略
        // 如果说没有开启重启Checkpoint，那么重启策略就是noRestart
        // 如果说没有开Checkpoint，那么重启策略会尝试自动帮你进行重启   重启Integer.MaxValue
        //env.setRestartStrategy(RestartStrategies.noRestart());

        //TODO 2.从Kafka的ODS层读取数据
        String topic = "ods_base_db_m";
        String groupId = "base_db_app_group";

        //2.1 通过工具类获取Kafka的消费者
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> jsonStrDS = env.addSource(kafkaSource);
        //TODO 3.对DS中数据进行结构的转换      String-->Json
        SingleOutputStreamOperator<JSONObject> jsonObjDS = jsonStrDS.map(data -> JSON.parseObject(data));

        //TODO 4.对数据进行ETL   如果table为空 或者 data为空，或者长度<3  ，将这样的数据过滤掉
        SingleOutputStreamOperator<JSONObject> filteredDS = jsonObjDS.filter(
                jsonObj -> {
                    boolean flag = jsonObj.getString("table") != null
                            && jsonObj.getJSONObject("data") != null
                            && jsonObj.getString("data").length() > 3;
                    return flag;
                }
        );
        //TODO 5. 动态分流  事实表放到主流，写回到kafka的DWD层；如果维度表，通过侧输出流，写入到Hbase
        //5.1定义输出到Hbase的侧输出流标签
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE){};
        //5.2 主流 写回到Kafka的数据
//        SingleOutputStreamOperator<JSONObject> kafkaDS = filteredDS.process(
//                new TableProcessFunction(hbaseTag)
//        );


    }
}
