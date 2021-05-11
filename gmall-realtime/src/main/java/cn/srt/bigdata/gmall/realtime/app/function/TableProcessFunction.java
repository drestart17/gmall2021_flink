package cn.srt.bigdata.gmall.realtime.app.function;

import cn.srt.bigdata.gmall.realtime.bean.TableProcess;
import cn.srt.bigdata.gmall.realtime.common.GmallConfig;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TableProcessFunction extends ProcessFunction {

    //TODO 1.需要将维度数据写入侧输出流，事实数据写到主流，因此需要定义一个侧输出流标签
    private OutputTag<JSONObject> outputTag;

    //TODO 2.使其在创建函数的时候指定侧输出流流到哪个标签去
    public TableProcessFunction(OutputTag<JSONObject> outputTag) {
        this.outputTag = outputTag;
    }

    //TODO 3.用于在内存中存储表配置对象(表名，表配置信息）
    private Map<String, TableProcess> tableProcessMap = new HashMap<>();
    //表示内存中已经存在的HBase表
    private Set<String> existsTables = new HashSet<>();

    //声明Phoenix连接,在初始化open时创建连接
    Connection conn = null;

    //TODO 4.初始化
    @Override
    public void open(Configuration parameters) throws Exception {

        //4.1 加载驱动
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        //4.2 创建连接
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void processElement(Object value, Context ctx, Collector out) throws Exception {

    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
