package com.Zac.apitest.transform;

import com.Zac.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransferTest6_RichFunction {
    public static void main(String[] args) throws Exception {
        //Execution Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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


        //分组
        KeyedStream<SensorReading, String> sensorReadingKeyedStream = sensorReadingstream.keyBy(new KeySelector<SensorReading, String>() {
            @Override
            public String getKey(SensorReading sensorReading) throws Exception {
                return sensorReading.getId();
            }
        });


        //
        DataStream<Tuple2<String, Integer>> resultStream = sensorReadingKeyedStream.map(new MyMapper1());


        resultStream.print();

        env.execute();
    }


    //普通UDF
    public static class MyMapper0 implements MapFunction<SensorReading, Tuple2<String,Integer>>{
        public Tuple2<String,Integer> map(SensorReading var1) throws Exception{
            return new Tuple2<>(var1.getId(),var1.getId().length());
        }
    }

    //Rich Function
    public static class MyMapper1 extends RichMapFunction<SensorReading, Tuple2<String,Integer>>{
        public Tuple2<String,Integer> map(SensorReading value) throws Exception{

            //rich function 支持获取runtime context的信息 如这个子任务的编号
            return new Tuple2<>(value.getId(), getRuntimeContext().getIndexOfThisSubtask());
        };

        public void open(Configuration param)throws Exception{
            //初始化工作。一般是定义状态或者建立数据库连接
            //每个分区都会执行一次open
            System.out.println("Open");
        };

        public void close() throws Exception{
            //一般是关闭连接和清空状态
            //每个分区都会执行一次open
            System.out.println("Close");
        }
    }


}

/*

 */
