package com.Zac.apitest.window;

import com.Zac.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import javax.lang.model.element.VariableElement;
import java.awt.*;

/**
 * @author huzhicong on 2021-06-27
 **/
public class windowTest2_CountWindow {
    public static void main(String[] args) throws  Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<String> stream = env.socketTextStream("localhost",7777);

        //map

        DataStream<SensorReading> mappedStream = stream.map(new MapFunction<String,SensorReading>(){
            public SensorReading map(String var1){
                String[] spiltted = var1.split(",");
                return new SensorReading(spiltted[0],Long.valueOf(spiltted[1]),Double.valueOf(spiltted[2]));
            }
        });

        // countwindow
        // 十个数滑动窗口,滑动数为2
        OutputTag<SensorReading> outputtag = new  OutputTag<SensorReading>("late"){};

        SingleOutputStreamOperator<Double> result =  mappedStream.keyBy("id")
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                //.timeWindow(Time.seconds(5),Time.seconds(2))
                .allowedLateness(Time.minutes(1))//允许数据迟到,如8：00-8：01，会在8：01输出数据，但是在8：01之后来到的，输入8-8.01之间的数据会直接放入8-8.01并作计算
                //.trigger
                //.evictor
                .sideOutputLateData(outputtag)
                .aggregate(new windowTest.myAggFunction());

        result.getSideOutput(outputtag).print("late");


        result.print();




        env.execute();
    }
}
