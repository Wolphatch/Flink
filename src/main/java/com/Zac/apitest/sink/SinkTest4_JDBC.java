package com.Zac.apitest.sink;

import com.Zac.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import javax.security.auth.login.Configuration;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author huzhicong on 2021-06-22
 **/
public class SinkTest4_JDBC {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> streamData = env.readTextFile("/Users/huzhicong/uni/PycharmProjects/FlinkTutorial/src/main/resources/Sensor.txt");


        // 转换成SensorReading的string
        DataStream<SensorReading> sensorData = streamData.map( new MapFunction<String,SensorReading>(){

            public SensorReading map(String var1) throws Exception{

                String[] sensor = var1.split(",");

                return new SensorReading(sensor[0],Long.valueOf(sensor[1]),Double.valueOf(sensor[2]));

            }

        });

        sensorData.addSink(new myJDBC());

        env.execute();


    }


    //自定义sink
    public static  class myJDBC extends RichSinkFunction<SensorReading> {

        // 初始化connection
        Connection connection = null;

        //声明预编译语句
        PreparedStatement insertStatement = null;
        PreparedStatement updateStatement = null;

        //invoke 每来一个数据就会invoke一次，不太好，在open里面设置(使用rich function）
        public void open(Configuration config) throws Exception{
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test",
            "root","123"
            );

            //预编译语句
            insertStatement = connection.prepareStatement("insert into SensorTemp (id,temp) values (?,?)");
            updateStatement = connection.prepareStatement("update SensorTemp set temp = ? where id = ?  ");

        }

        public void invoke(SensorReading value, SinkFunction.Context context) throws Exception{
            //每来一条语句就执行open里的sql

            //直接执行更新语句，如果没有更新就插入
            updateStatement.setDouble(1,value.getTemperature());
            updateStatement.setString(2,value.getId());
            updateStatement.execute();

            if (updateStatement.getUpdateCount() ==0){
                insertStatement.setString(1,value.getId());
                insertStatement.setDouble(2,value.getTemperature());
            }


        };

        public void close()throws  Exception{

            insertStatement.close();
            updateStatement.close();
            connection.close();
        }
    }
}
