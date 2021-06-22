package com.Zac.apitest.sink;

import com.Zac.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;


/**
 * @author huzhicong on 2021-06-22
 **/
public class SinkTest1_Kafka {
    public static void main(String[] args) throws Exception {

        //context
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");

        //添加数据源(kafka的主题，反序列化schema，properties）
        DataStream<String> streamData= env.addSource(new FlinkKafkaConsumer011<String>("sensor",new SimpleStringSchema(),properties));

        // 转换成SensorReading的string
        DataStream<String> sensorData = streamData.map( new MapFunction<String,String>(){

            public String map(String var1){

                String[] sensor = var1.split(",");

               return new SensorReading(sensor[0],Long.valueOf(sensor[1]),Double.valueOf(sensor[2])).toString();

            }

        });


        //write

        sensorData.addSink(new FlinkKafkaProducer011<String>("localhost:9092","sinkTest",new SimpleStringSchema()) {





        });



        //执行
        env.execute();
    }
}

/*

Zookeeper:
    将conf下的zoo_sample.cfg 复制重命名为zoo.cfg。 并改变其数据存储位置
    ./bin/zkServer.sh start

Kafka:
    ./bin/kafka-server-start.sh -daemon ./config/server.properties

Kafka消费:
    ./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sinkTest

kafka-console-producer生产:
    ./kafka-console-producer.sh --broker-list localhost:9092 --topic sensor

启动此程序，并将Sensor.txt的数据输入terminal
 */





