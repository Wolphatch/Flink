package com.Zac.apitest.source;

import com.Zac.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * @author huzhicong on 2021-06-09
 **/
public class SourceTest3_Costomized {
    public static void main(String[] args)throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SensorReading> streamData = env.addSource(new MySensorSource());

        streamData.print();

        env.execute();
    }

    //实现自定义source function

    public static class MySensorSource implements SourceFunction<SensorReading>{

        //控制run函数的停止
        private boolean running = true;

        //SourceContext<T>是一个泛型，SourceContext<SensorReading> 指sourcecontect的类是SensorReading
        public void run(SourceContext<SensorReading> ctx) throws Exception {

            //随机数发生器，控制温度波动
            Random random = new Random();

            //设置10个传感器的初始温度
            HashMap<String, Double> sensorTemp = new HashMap<String, Double>();

            //长生一个随机初始温度
            for(int i = 0; i<10; i++){
                //产生一个随机温度
                sensorTemp.put("sensor_"+(i+1),60+random.nextGaussian()*20);
            }

            //不停产生新的温度
            while(running){
                //一个for循环即一轮新的温度
                for (String sensorId: sensorTemp.keySet()){
                    //在初始温度基础上做一个随机波动
                    Double newTemp = sensorTemp.get(sensorId)+random.nextGaussian();
                    //更新温度
                    sensorTemp.put(sensorId,newTemp);
                    //SourceContext为上下文
                    //通过 ctx.collect() 搜集并转发新的温度
                    ctx.collect(new SensorReading(sensorId,System.currentTimeMillis(),newTemp));
                }
                //控制温度更新间隔
                Thread.sleep(1000L);
            }
        }

        //需要实现的第二个函数
        public void cancel(){

        }

    }
}
