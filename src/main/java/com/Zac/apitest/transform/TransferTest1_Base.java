package com.Zac.apitest.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author huzhicong on 2021-06-09
 **/
public class TransferTest1_Base {
    public static void main (String[]  args){
        //环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);

        //从文件读取数据
        DataStream<String> inputStream = env.readTextFile("/Users/huzhicong/uni/PycharmProjects/FlinkTutorial/src/main/resources/hello.txt");

        //1. map, 把string转化成长度输出
        inputStream.map(new MyMapFunction());
    }
    //map
    public static class MyMapFunction implements MapFunction<String,Integer>{
        public Integer map(String inputString)throws Exception{
            return inputString.replace(" ","").length();
        }
    }

    //FlatMap

}
