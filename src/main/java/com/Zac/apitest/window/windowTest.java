package com.Zac.apitest.window;

import com.Zac.apitest.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import sun.management.Sensor;
import sun.plugin2.os.windows.Windows;

import java.util.Iterator;

/**
 * @author huzhicong on 2021-06-27
 **/
public class windowTest {
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


        //1.window（求均值）
         DataStream<Double> result = mappedStream.keyBy("id")
                .timeWindow(Time.seconds(5))
                .aggregate(new myAggFunction());

        //2.全窗口(计数)
        SingleOutputStreamOperator<Tuple3<String,Long,Integer>> result2 =  mappedStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .apply(new WindowFunction<SensorReading, Tuple3<String,Long,Integer>, Tuple, TimeWindow>() {
                    //keyBy 产生的keyedstream是元组类型
                    public void apply(Tuple tuple, TimeWindow window, Iterable<SensorReading> input, Collector<Tuple3<String,Long,Integer>> out){
                        String id = tuple.getField(0);
                        Long windowend = window.getEnd();
                        Integer count = IteratorUtils.toList(input.iterator()).size();
                        out.collect(new Tuple3<String,Long,Integer>(id,windowend,count));
                    }
                });


        result2.print();
        //result.print();


        //execute
        env.execute();

    }

    public static class myAggFunction implements AggregateFunction<SensorReading, Double[],Double>{

        public Double[] createAccumulator(){
            Double[] acc = {0.0,0.0};
            return acc;
        };

        public Double[] add(SensorReading inputSensorReading,Double[] acc){
            acc[0]+=inputSensorReading.getTemperature();
            acc[1]+=1;
            return acc;
        };

        public Double getResult(Double[] acc){
            return (Double)acc[0]/acc[1];
        };

        public Double[] merge(Double[] acc1,Double[] acc2){
            acc1[0]+=acc2[0];
            acc1[1]+=acc2[1];

            return acc1;
        }
    }
}
