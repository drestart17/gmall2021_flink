package cn.srt.bigdata.gmall.realtime.app.dwd;

import cn.srt.bigdata.gmall.realtime.utils.MyKafkaUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class BaseLogApp {

    //1.创建主程序
    public static void main(String[] args) throws Exception {


        //1.1创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().createLocalEnvironmentWithWebUI(new Configuration());
        //1.2设置并行度 与kafka分区数保持一致
        env.setParallelism(4);
        //1.3设置checkpoint参数 每10s做一次ck,ck的语义的精准一次性
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        //1.4设置ck的过期时间：ck必须在1分钟内完成，否则会被抛弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //1.5设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://bigdata-test01:9820/flink/checkpoint"));
        //1.6设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2,60000));
        //1.7设置用户权限
        System.setProperty("HADOOP_USER_NAME","root");

        //2.指定topic
        String topic = "ods_base_log";
        //3.指定消费者组
        String groupId = "ods_dwd_base_log_app_test";
        //4.消费kafka的某个主题
        DataStreamSource<String> inputDS = env.addSource(MyKafkaUtil.getKafkaSouce(topic, groupId));

        //5.将String转换为json对象
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = inputDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                //将String字符串转换为json对象
                JSONObject jsonObject = JSONObject.parseObject(value);
                return jsonObject;
            }
        });

        //打印
        jsonObjectDS.print();

        //执行
        env.execute("dwd_base_log Job");
    }
}
