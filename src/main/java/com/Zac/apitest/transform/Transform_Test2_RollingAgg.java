package com.Zac.apitest.transform;


import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import scala.Tuple3;

import java.util.HashMap;
import java.util.Random;

/**
 * @author huzhicong on 2021-06-10
 **/
public class Transform_Test2_RollingAgg {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<org.apache.flink.api.java.tuple.Tuple3<String,Long,Double>> sensorReadingDataStreamSource = env.addSource(new MySource());

        DataStream result = sensorReadingDataStreamSource
                .keyBy(new KeySelector<org.apache.flink.api.java.tuple.Tuple3<String, Long, Double>, String>(){
                    public String getKey(org.apache.flink.api.java.tuple.Tuple3<String,Long,Double> var){
                        String sensor = var.getField(0);
                        return sensor;
                    }
                })
                .minBy(2);

        result.print();

        env.execute();

    }



    public static class MySource implements SourceFunction<org.apache.flink.api.java.tuple.Tuple3<String,Long,Double>>{

        int stop = 0;

        public void run(SourceFunction.SourceContext<org.apache.flink.api.java.tuple.Tuple3<String,Long,Double>> ctx){

            //生成随机初始温度
            Random random = new Random();
            //HashMap 存放温度
            HashMap<String,Double> sensorTemp = new HashMap<String,Double>();
            for (int i = 0; i<3; i++){
                sensorTemp.put("Sensor_"+(i+1),60 + random.nextGaussian()*20);
            }

            //不断循环，随机温度变化.持续50秒
            while (stop<5) {
                for (String sensorId : sensorTemp.keySet()) {
                    Double newTemp = sensorTemp.get(sensorId)+random.nextGaussian();
                    sensorTemp.put(sensorId,newTemp);
                    ctx.collect(new org.apache.flink.api.java.tuple.Tuple3<String, Long, Double>(sensorId,System.currentTimeMillis(),newTemp));
                }

                stop+=1;
            }

        }

        public void cancel(){

        }
    }


    public static class myKeySelector implements KeySelector<Tuple3<String,Long,Double>,String>{

        public String getKey(Tuple3<String,Long,Double> var)throws Exception{
            //获取sensor id作为key
            String sensor = var._1();
            return sensor;

        }
    }
}
