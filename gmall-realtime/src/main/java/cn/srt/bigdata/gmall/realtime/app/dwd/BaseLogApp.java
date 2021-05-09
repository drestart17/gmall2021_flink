package cn.srt.bigdata.gmall.realtime.app.dwd;

import cn.srt.bigdata.gmall.realtime.utils.MyKafkaUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Date;

public class BaseLogApp {

    //定义topic
    //start启动日志发往dwd_start_log
    private static final String TOPIC_START = "dwd_start_log";
    //page页面日志发往dwd_page_log
    private static final String TOPIC_PAGE = "dwd_page_log";
    //display曝光日志发往dwd_display_log
    private static final String TOPIC_DISPLAY = "dwd_display_log";

    //1.创建主程序
    public static void main(String[] args) throws Exception {


        //TODO 1.创建流式执行环境
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

        //TODO 2.指定topic
        String topic = "ods_base_log";
        //TODO 3.指定消费者组
        String groupId = "ods_dwd_base_log_app_test";
        //TODO 4.消费kafka的某个主题
        DataStreamSource<String> inputDS = env.addSource(MyKafkaUtil.getKafkaSouce(topic, groupId));

        //TODO 5.将String转换为json对象
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = inputDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                //将String字符串转换为json对象
                JSONObject jsonObject = JSONObject.parseObject(value);
                return jsonObject;
            }
        });

        /**
         * TODO 6.识别新老访客
         *
         * 老访客：首次访问时间不为空并且首次访问时间早于当日的
         * 新访客：首次访问时间为空或者首次访问时间晚于当日的
         *
         * 因为日志中的is_new不准，因此需要将判断后的is_new重新写会到日志中
         */
        //6.1根据mid进行分组(如果key-->value(json):getJSONObject)
        KeyedStream<JSONObject, String> midKeyedDS = jsonObjectDS.keyBy(data -> data.getJSONObject("common").getString("mid"));

        //6.2按照key分组后，需要取出第一次访问时间存到状态中
        SingleOutputStreamOperator<JSONObject> midWithNewFlagDS = midKeyedDS.map(new RichMapFunction<JSONObject, JSONObject>() {

            //6.2.1需要一个状态首先声明一个状态将访问时间存入
            private ValueState<String> firstVisitDateState;
            //6.2.1需要取出访问时间存入到状态中
            //6.2.2每来一条时间需要拿到ts跟当前日期做比较。如果ts不为空且小于当前日期：老访客；否则为新访客

            //创建日期格式
            private SimpleDateFormat simpleDateFormat;

            @Override
            public void open(Configuration parameters) throws Exception {
                //6.2.3初始化数据

                // 从运行时环境创建状态描述器
                firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("newMidDateState", String.class));

                simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
            }


            @Override
            public JSONObject map(JSONObject value) throws Exception {

                //打印数据：对比
//                System.out.println(value);
                //1.获取访问标记
                String isNew = value.getJSONObject("common").getString("is_new");

                //2.提取访问时间
                Long ts = value.getLong("ts");

                //3.判断时间是否为空并且跟当前日期比较
                if ("1".equals(isNew)) {

                    //取出状态值
                    String newMidDate = firstVisitDateState.value();

                    //获取当前日期访问时间
                    String tsDate = simpleDateFormat.format(new Date(ts));

                    //判断状态值和当前日期访问时间
                    //如果状态日期不为空并且状态日期不等于访问日期：则为老访客
                    if (newMidDate != null && newMidDate.length() > 0) {
                        if (!newMidDate.equals(tsDate)) {
                            //老访客：is_new = 0
                            isNew = "0";
                            //更新这条json记录
                            value.getJSONObject("common").put("is_new", isNew);
                        }
                    } else {
                        //如果复检后确实为新用户，则将状态更新为当前日期
                        firstVisitDateState.update(tsDate);
                    }
                }
                return value;
            }

        });

        //TODO 7.将日志通过侧输出流分为三类：页面日志，启动日志，曝光日志
        //7.1 定义两个侧输出流-->通过process()
        OutputTag<String> startTag = new OutputTag<String>("start"){};
        OutputTag<String> displayTag = new OutputTag<String>("display"){};

        //7.2 定义process()
        SingleOutputStreamOperator<String> pageDS = midWithNewFlagDS.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                        //7.2.1 获取数据中的启动相关字段
                        JSONObject startJson = value.getJSONObject("start");
                        //7.2.2 将json转换成string
                        String valueString = value.toString();
                        //7.2.3 如果startJson不为空，则将start启动日志放到startTag中
                        if (startJson != null && startJson.size() > 0) {

                            ctx.output(startTag, valueString);
                        } else {
                            //如果是非启动日志，则为页面日志或者曝光日志(都包含页面信息)
//                            System.out.println("pageString:" + valueString);
                            //将页面数据输出到主流
                            out.collect(valueString);
                            //如果是曝光日志且不为空，则将曝光数据输出到侧输出流
                            JSONArray displayArray = value.getJSONArray("displays");
                            if (displayArray != null && displayArray.size() > 0) {
                                for (int i = 0; i < displayArray.size(); i++) {
                                    JSONObject displayJson = displayArray.getJSONObject(i);
                                    //获取页面ID
                                    String pageId = value.getJSONObject("page").getString("page_id");
                                    //给曝光日志加上页面ID
                                    displayJson.put("page_id", pageId);
                                    //将曝光数据发送到曝光侧输出流
                                    ctx.output(displayTag, displayJson.toString());
                                }
                            }
                        }
                    }
                }
        );

        //7.3获取侧输出流
        DataStream<String> startDStream = pageDS.getSideOutput(startTag);
        DataStream<String> displayDStream = pageDS.getSideOutput(displayTag);


        //TODO 8.将不同的日志发送到kafka不同的topic主题中
        //8.1 调用工具类生成FlinkKafkaProducer对象
        FlinkKafkaProducer startSink = MyKafkaUtil.getKafkaSink(TOPIC_START);
        FlinkKafkaProducer pageSink = MyKafkaUtil.getKafkaSink(TOPIC_PAGE);
        FlinkKafkaProducer displaySink = MyKafkaUtil.getKafkaSink(TOPIC_DISPLAY);
        //8.2输出到各自的topic中去
        startDStream.addSink(startSink);
        pageDS.addSink(pageSink);
        displayDStream.addSink(displaySink);




        //TODO 打印
        pageDS.print("page");
        startDStream.print("start");
        displayDStream.print("display");


        //TODO final 执行
        env.execute("dwd_base_log Job");
    }
}
