package com.Zac.apitest.transform;

import com.Zac.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransferTest3_Reduce {

    public static void main (String[] args) throws Exception{

        //Execution Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //read source file
        DataStream<String> stringDataStreamSource = env.readTextFile("C:\\Users\\shawg\\IdeaProjects\\Flink\\src\\main\\resources\\Sensor.txt");

        stringDataStreamSource.print();

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


        //分组
        KeyedStream<SensorReading, String> sensorReadingStringKeyedStream = sensorReadingstream.keyBy(new KeySelector<SensorReading, String>() {
            @Override
            public String getKey(SensorReading sensorReading) throws Exception {
                return sensorReading.getId();
            }
        });



        //3.3使用reduce，输出至今为止，最高的温度是什么
        //换句话说：最大的温度值和最新的时间戳

        SingleOutputStreamOperator<SensorReading> result = sensorReadingStringKeyedStream.reduce(new ReduceFunction<SensorReading>() {
            @Override

            //value 1 指当前分组（一定要是同一个key下）下，前一个状态，如第一个输入 Sensor_1,1547718201,11.4
            //value 2 指新进来的数据，如 Sensor_1,1547718201,12.4
            //返回一个 SensorReading， （id，当前时间，旧的和新的温度比较，选取最高）
            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {

                return new SensorReading(value1.getId(),value2.getTimestamp(), Math.max(value1.getTemperature(), value2.getTemperature()));
            }
        });

        result.print();
        env.execute();

    }
}
