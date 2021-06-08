package com.Zac.apitest.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceTest2_File {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从文件读取数据
        DataStream<String> dataStreamSource = env.readTextFile("/Users/huzhicong/uni/PycharmProjects/FlinkTutorial/src/main/resources/Sensor.txt");

        //打印输出
        dataStreamSource.print();

        //执行
        env.execute();
    }



}
