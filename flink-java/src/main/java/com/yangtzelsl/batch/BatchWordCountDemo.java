package com.yangtzelsl.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * @Author: liusilin
 * @Date: 2020/10/22 20:01
 * @Description:
 */
public class BatchWordCountDemo {
    public static void main(String[] args) throws Exception {

        // 1.获取执行环境
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        // 2.加载数据源
        DataSource<String> ds = executionEnvironment.fromElements("flink flink flink", "spark spark spark");

        // 3.词频统计
        DataSet<Tuple2<String, Integer>> sum = ds.flatMap(new LineSplit())
                .groupBy(0)
                .sum(1);

        sum.print();

    }

    /**
     * 自定义 FlatMapFunction
     */
    public static final class LineSplit implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
