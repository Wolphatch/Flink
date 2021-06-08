package com.Zac.apitest.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class SourceTest3_Kafka {
    public static void main(String[] args)throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //kafka 配置项
        //Properties 继承于 Hashtable。表示一个持久的属性集.属性列表中每个键及其对应值都是一个字符串。
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");

        //添加数据源(kafka的主题，反序列化schema，properties）
        DataStream streamData= env.addSource(new FlinkKafkaConsumer011<String>("sensor",new SimpleStringSchema(),properties));

        streamData.print();

        env.execute();


    }
}


/*
Zookeeper:
    将conf下的zoo_sample.cfg 复制重命名为zoo.cfg。 并改变其数据存储位置
    ./bin/zkServer.sh start

Kafka:
    ./bin/kafka-server-start.sh -daemon ./config/server.properties

jps:
    terminal 使用jps查看进程，如下：
    31987 TaskManagerRunner
    32723 QuorumPeerMain
    31702 StandaloneSessionClusterEntrypoint
    31302 RemoteMavenServer
    33016 Jps
    33007 Kafka

flink:
    ./bin/start-cluster.sh

kafka-console-producer:
    ./kafka-console-producer.sh --broker-list localhost:9092 --topic sensor

启动此程序，并将Sensor.txt的数据输入terminal


 */