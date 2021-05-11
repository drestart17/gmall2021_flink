package cn.srt.bigdata.gmall.realtime.app.function;

import cn.srt.bigdata.gmall.realtime.bean.TableProcess;
import cn.srt.bigdata.gmall.realtime.common.GmallConfig;
import cn.srt.bigdata.gmall.realtime.utils.MySQLUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;

public class TableProcessFunction extends ProcessFunction {

    //TODO 1.需要将维度数据写入侧输出流，事实数据写到主流，因此需要定义一个侧输出流标签
    private OutputTag<JSONObject> outputTag;

    //TODO 2.使其在创建函数的时候指定侧输出流流到哪个标签去
    public TableProcessFunction(OutputTag<JSONObject> outputTag) {
        this.outputTag = outputTag;
    }

    //TODO 3.用于在内存中存储表配置对象(表名+操作类型，表配置信息）
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
        //4.3 创建Phoenix连接后初始化配置表信息
        initTableProcessMap();
    }

    @Override
    public void processElement(Object value, Context ctx, Collector out) throws Exception {

    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    /**
     * 初始化TableProcess配置表信息
     */
    private void initTableProcessMap() {

        System.out.println("更新配置的处理信息...");

        //1.查询配置表数据
        List<TableProcess> tableProcessList = MySQLUtil.queryList("select * from table_process", TableProcess.class, true);

        //2.遍历配置表数据
        for (TableProcess tableProcess : tableProcessList) {

            //2.1获取来源表
            String sourceTable = tableProcess.getSourceTable();
            //2.2获取操作类型
            String operateType = tableProcess.getOperateType();
            //2.3获取结果表表名
            String sinkTable = tableProcess.getSinkTable();
            //2.4获取sink类型
            String sinkType = tableProcess.getSinkType();
            //2.5拼接字段创建主键
            String key = sourceTable + ":" + operateType;
            //2.6将结果数据存入结果集合
            tableProcessMap.put(key, tableProcess);

            //2.7如果是向hbase中保存的表，则需要先检验下内存中是否存在这张表，如果不存在需要建表
            if("insert".equals(operateType) && "hbase".equals(sinkType)) {
                //将表往set中放，如果不存在，则true -->建表
                boolean notExist = existsTables.add(sourceTable);
                if(notExist) {
//                    checkTable()
                }
            }
        }


    }
}
