package com.yangtzelsl;

import com.yangtzelsl.util.FlinkSourceUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

/**
 * @Author: liusilin
 * @Date: 2020/8/30 09:25
 * @Description:
 */
public class RealtimeETL {

    public static void main(String[] args) throws Exception {

        //用来解析配置文的工具类
        ParameterTool parameters = ParameterTool.fromPropertiesFile(args[0]);

        //使用Flink拉取Kafka中的数据，对数据进行清洗、过滤整理

        DataStream<String> lines = FlinkSourceUtils.createKafkaStream(parameters, SimpleStringSchema.class);

        lines.print();

        FlinkSourceUtils.env.execute();

    }
}
