package com.Zac.apitest.source;

import com.Zac.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import java.util.Arrays;

public class SourceTest1_Collection {

    public static void main(String[] args)throws Exception{

        //创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从集合中读取数据
        //读取的是一个List<SensorReading>， 是一个集合
        DataStream<SensorReading> dataStream = env.fromCollection(
                Arrays.asList(
                        new SensorReading("Sensor_1",1547718201L,11.4),
                        new SensorReading("Sensor_5",1547718202L,13.4),
                        new SensorReading("Sensor_9",1547718207L,12.5),
                        new SensorReading("Sensor_2",1547718210L,9.4)

                )
        );

        //fromElements 可以直接读取int
        //DataStream<Integer> integerDataStreamSource = env.fromElements(1, 2, 4, 67, 189);

        //打印
        dataStream.print("data");

        //执行
        env.execute();

    }
}



/*
data:2> SensorReading{id='Sensor_2', timestamp=1547718210, temperature=9.4}
data:1> SensorReading{id='Sensor_9', timestamp=1547718207, temperature=12.5}
data:7> SensorReading{id='Sensor_1', timestamp=1547718201, temperature=11.4}
data:8> SensorReading{id='Sensor_5', timestamp=1547718202, temperature=13.4}
 */
