package com.Zac.apitest.transform;

import com.Zac.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

public class TransferTest4_MultipleStreams {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //read source file
        DataStream<String> stringDataStreamSource = env.readTextFile("C:\\Users\\shawg\\IdeaProjects\\Flink\\src\\main\\resources\\Sensor.txt");


        //Transfer Sensor to Sensoreading
        DataStream<SensorReading> sensorReadingstream = stringDataStreamSource.map(new MapFunction<String, SensorReading>() {
            public SensorReading map(String s) throws Exception {
                String[]  splitedSensor = s.split(",");
                //防止温度读数为空
                //转型
                SensorReading sensorReadingInstance = new SensorReading(splitedSensor[0], Long.valueOf(splitedSensor[1]),splitedSensor[2]==null?0.0:Double.valueOf(splitedSensor[2]));
                return sensorReadingInstance;

            }
        });

        // 按照30度为界，分为两条流
        SplitStream<SensorReading> splitedSensorReadingstream = sensorReadingstream.split(new OutputSelector<SensorReading>(){
            //返回迭代器，string指标签
            public Iterable<String> select(SensorReading var1){
               return (var1.getTemperature()>30) ? Collections.singletonList("High") : Collections.singletonList("Low");
            }
        });

        DataStream<SensorReading> HighTempDataStream = splitedSensorReadingstream.select("High");
        DataStream<SensorReading> LowTempDataStream = splitedSensorReadingstream.select("Low");
        DataStream<SensorReading> AllTempDataStream = splitedSensorReadingstream.select("High","Low");


        //合流
        //将高温流转化为二元组类型，与低温流连接合并之后，输出状态信息
        DataStream<Tuple2<String,Double>> TupledHighTempStream = HighTempDataStream.map(new MapFunction<SensorReading, Tuple2<String,Double>>() {
            public Tuple2<String,Double> map(SensorReading var1){
                return new Tuple2(var1.getId(),var1.getTemperature());
            }
        });

        //高温报警，低温正常
        ConnectedStreams<Tuple2<String, Double>, SensorReading> connected = TupledHighTempStream.connect(LowTempDataStream);

        DataStream<Object> warningStream = connected.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Tuple3 map1(Tuple2<String, Double> var1) throws Exception {
                return new Tuple3(var1.f0,var1.f1,"Warning: High Temperature");
            };

            public Tuple2 map2(SensorReading var2) throws Exception{
                return new Tuple2(var2.getId(),"Normal");
            }
        });

        warningStream.print();

        env.execute();

    }

}
