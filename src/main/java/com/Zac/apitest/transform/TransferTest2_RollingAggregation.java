package com.Zac.apitest.transform;

import com.Zac.apitest.beans.SensorReading;
import com.typesafe.config.ConfigException;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author huzhicong on 2021-06-10
 **/
public class TransferTest2_RollingAggregation {
    public static void main (String[] args) throws Exception{


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //read source file
        DataStream<String> stringDataStreamSource = env.readTextFile("C:\\Users\\shawg\\IdeaProjects\\Flink\\src\\main\\resources\\Sensor.txt");


        //1.1####################################################
        //Transfer Sensor to Sensoreading
        //匿名类写法
        DataStream<SensorReading> sensorReadingstream = stringDataStreamSource.map(new MapFunction<String, SensorReading>() {
            public SensorReading map(String s) throws Exception {
                String[]  splitedSensor = s.split(",");
                //防止温度读数为空
                //转型
                SensorReading sensorReadingInstance = new SensorReading(splitedSensor[0], Long.valueOf(splitedSensor[1]),splitedSensor[2]==null?0.0:Double.valueOf(splitedSensor[2]));
                return sensorReadingInstance;

           }
        });
        //######################################################

        /*
        //1.2######################################################
        //Transfer Sensor to Sensoreading
        //lambda 表达式
        // 如果返回值是一个类似Tuple2<String,Interger>的话，不要用lambda表达式。 会引起泛型擦除
        DataStream<SensorReading> sensorReadingStream = stringDataStreamSource.map(line -> {
            String[]  splitedSensor = line.split(",");
            SensorReading sensorReadingInstance = new SensorReading(splitedSensor[0], Long.valueOf(splitedSensor[1]),splitedSensor[2]==null?0.0:Double.valueOf(splitedSensor[2]));
            return sensorReadingInstance;
        });
        //######################################################
        */

        //分组
        //2.1第一种写法
        KeyedStream<SensorReading, String> sensorReadingStringKeyedStream = sensorReadingstream.keyBy(new KeySelector<SensorReading, String>() {
            @Override
            public String getKey(SensorReading sensorReading) throws Exception {
                return sensorReading.getId();
            }
        });

        //2.2第二种写法
        //之所以是 Tuple，是因为keyBy可以输入一个多个field
        //KeyedStream<SensorReading, Tuple> sensorReadingStringKeyedStream = sensorReadingstream.keyBy（id");




        //滚动聚合
        //3.1只修改了输出的SensorReading里面，temp最大值，时间没有修改
        //SingleOutputStreamOperator<SensorReading> maxTempKeyedBySensorId = sensorReadingStringKeyedStream.max("temperature");

        //3.2修改了输出的SensorReading里面，temp最大值，同时也修改了那个时候的事件戳，即时间戳和彼时temp的温度是对应的
        //换句话说，输出的是不同sensor下最高温度和最高温度发生的时间戳
        SingleOutputStreamOperator<SensorReading> maxTempKeyedBySensorId = sensorReadingStringKeyedStream.maxBy("temperature");

        //3.3使用reduce，输出至今为止，最高的温度是什么
        //换句话说：当前时间+至今最高温度，看下一个文件Test3

        maxTempKeyedBySensorId.print();

        env.execute();

    }
}


/*
public class FlatFun implements ResultTypeQueryable<String>, FlatMapFunction<Integer, String> {
@Override
public TypeInformation getProducedType() {
    return TypeInformation.of(String.class);
}
@Override
public void flatMap(Integer value, Collector<String> out) {
    out.collect(String.valueOf(value));
    System.out.println("flatFun");
}
}
stream.flatMap(new FlatFun())
            .print();
 */
