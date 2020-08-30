package com.yangtzelsl.util;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @Author: liusilin
 * @Date: 2020/8/30 09:17
 * @Description:
 */
public class FlinkSourceUtils {

    public static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static <T> DataStream<T> createKafkaStream(ParameterTool parameters, Class<? extends DeserializationSchema<T>> clazz) throws Exception{

        //如果开启Checkpoint，偏移量会存储到哪呢？
        env.enableCheckpointing(parameters.getLong("checkpoint.interval", 300000));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);

        //就是将job cancel后，依然保存对应的checkpoint数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        String checkPointPath = parameters.get("checkpoint.path");
        if(checkPointPath != null) {
            env.setStateBackend(new FsStateBackend(checkPointPath));
        }

        int restartAttempts = parameters.getInt("restart.attempts", 30);
        int delayBetweenAttempts = parameters.getInt("delay.between.attempts", 30000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(restartAttempts, delayBetweenAttempts));

        Properties properties = parameters.getProperties();
        String topics = parameters.getRequired("kafka.topics");
        List<String> topicList = Arrays.asList(topics.split(","));
        FlinkKafkaConsumer<T> flinkKafkaConsumer = new FlinkKafkaConsumer<T>(topicList, clazz.newInstance(), properties);

        //在Checkpoint的时候将Kafka的偏移量保存到Kafka特殊的Topic中，默认是true
        flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(false);

        return env.addSource(flinkKafkaConsumer);
    }

//    public static <T> DataStream<T> createSocketStream(ParameterTool parameters, Class<? extends DeserializationSchema<T>> clazz) throws Exception{
//
//        String hostName = parameters.get("socket.hostname");
//        Integer port = Integer.valueOf(parameters.get("socket.port"));
//
//        DataStreamSource<String> socketDataSource = env.socketTextStream(hostName, port);
//
//         return env.addSource(new SocketTextStreamFunction(hostName, port))
//    }

    public static <T> DataStream<T> createMySQLStream(ParameterTool parameters, Class<? extends DeserializationSchema<T>> clazz) throws Exception{


        return env.addSource(new SourceFromMySQL());
    }


}
