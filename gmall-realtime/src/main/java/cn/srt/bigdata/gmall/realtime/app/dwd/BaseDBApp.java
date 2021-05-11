package cn.srt.bigdata.gmall.realtime.app.dwd;

import cn.srt.bigdata.gmall.realtime.app.function.TableProcessFunction;
import cn.srt.bigdata.gmall.realtime.common.FlinkEnv;
import cn.srt.bigdata.gmall.realtime.utils.MyKafkaUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class BaseDBApp {

    public static void main(String[] args) throws Exception {

        /**
         * TODO 处理业务数据
         * 由于MaxWell中采集的数据都会写入到一个topic中，不便于日后数据的处理，因此需要处理
         * 处理方式：
         *      1.事实数据写入到kafka的dwd层
         *      2.维度数据写入到Hbase中
         * 难点：需要动态感知哪些表是维度表，哪些表是事实表
         * 实现方式：
         *      1.在Mysql中动态配置表的数据，由管理员进行维护
         *      2.Flink周期同步Mysql中的配置数据
         *      3.Flink根据配置进行分流（Hbase or Kafka的dwd层）
         */
        //TODO 1.创建流式执行环境
        StreamExecutionEnvironment env = FlinkEnv.createStreamEnv();

        //TODO 2.指定topic
        String topic = "ods_base_db_m";
        //TODO 3.指定消费者组
        String groupId = "ods_dwd_base_db_app_test";
        //TODO 4.消费kafka的某个主题
        DataStreamSource<String> inputDS = env.addSource(MyKafkaUtil.getKafkaSouce(topic, groupId));

        //TODO 5.对数据结构进行转换:将String转换成JSONObject
        SingleOutputStreamOperator<JSONObject> jsonDS = inputDS.map(
                data -> JSONObject.parseObject(data)
        );

        //TODO 6.将一些脏数据过滤
        SingleOutputStreamOperator<JSONObject> filterDS = jsonDS.filter(
                data -> {
                    //将结果为true的返回，结果为false的过滤掉
                    boolean flag = data.getString("table") != null &&
                            data.getJSONObject("data") != null &&
                            data.getString("data").length() > 3;
                    return flag;
                }
        );

        //TODO 7 根据Mysql的配置表动态分流
//        filterDS.process(new TableProcessFunction())

        //TODO 打印
        jsonDS.print("mysql");

        //TODO final 打印
        env.execute("dwd_base_app");
    }
}
