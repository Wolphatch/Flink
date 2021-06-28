package com.Zac.apitest.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransferTest7_Repartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        DataStream<String> stringDataStreamSource = env.readTextFile("C:\\Users\\shawg\\IdeaProjects\\Flink\\src\\main\\resources\\Sensor.txt");

        stringDataStreamSource.print("input");

        //shuffle
        DataStream<String> shuffled = stringDataStreamSource.shuffle();

        shuffled.print("Shuffled");

        env.execute();
    }
}
