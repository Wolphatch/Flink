package com.Zac.apitest.window;

import com.Zac.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowTest1_TimeWindow {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<String> stringDataStreamSource = env.socketTextStream("localhost",7777);

        DataStream<SensorReading> sensorReadingstream = stringDataStreamSource.map(new MapFunction<String, SensorReading>() {
            public SensorReading map(String s) throws Exception {
                String[]  splitedSensor = s.split(",");
                //防止温度读数为空
                //转型
                SensorReading sensorReadingInstance = new SensorReading(splitedSensor[0], Long.valueOf(splitedSensor[1]),splitedSensor[2]==null?0.0:Double.valueOf(splitedSensor[2]));
                return sensorReadingInstance;

            }
        });

        //maxBy返回有最大值的那个元素，max返回最大值

        //窗口函数之前需要一个keyby+reducefunction，之后也需要增量聚合函数（reducefunction或者AggregationFunction）或者全窗口函数（ProcessWindowFunction，WindowFunction）

        //window test,开一个15秒的窗口
        //TumblingEventTimeWindows: 基于事件发生的时间开窗口 (静态方法，无需初始化）
        //TumblingProsessingTimeWindows:基于集群机器事件做窗口 （静态方法。无需初始化）
        DataStream <Double> result = sensorReadingstream.keyBy("id")
                //.window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .timeWindow(Time.seconds(15)) //简单写法，一个参数是tumbling，两个是sliding
                //.window(EventTimeSessionWindows.withGap(Time.minutes(1))) //构造一个 1分钟timeout的session窗口
                //.countWindow(10,2) //一个参数就是tumbling，两个sliding
                .aggregate(new AggregateFunction<SensorReading, Double[], Double>() { //聚合方法，求每15s温度均值


                    //创建累加器
                    @Override
                    public Double[] createAccumulator(){
                        Double[] acc = {0.0,0.0};
                        return acc; //返回一个数组
                    }

                    //add方法
                    @Override
                    public Double[] add(SensorReading var1, Double[] acc){  //看泛型，第一个是输入数据，第二个是accumulator
                        acc[0]+=var1.getTemperature();
                        acc[1]+=1.0;

                        return acc;
                    }

                    //输出结果
                    @Override
                    public Double getResult(Double[] acc){

                        return (Double) acc[0]/acc[1];
                    }

                    @Override
                    public Double[] merge(Double[] acc1, Double[] acc2) {
                        acc1[0]+=acc2[0];
                        acc1[1]+=acc2[1];
                        return acc1 ;
                    }
                });


        result.print();
        //execute
        env.execute();



    }
}
