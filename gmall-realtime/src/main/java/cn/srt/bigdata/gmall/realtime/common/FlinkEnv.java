package cn.srt.bigdata.gmall.realtime.common;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkEnv {

    public static StreamExecutionEnvironment createStreamEnv() {

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
        return env;
    }
}
