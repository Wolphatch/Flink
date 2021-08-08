package com.Zac.apitest.state;

import com.Zac.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.Collections;
import java.util.List;

/**
 * @author huzhicong on 2021-07-25
 **/
public class stateTest1_OperatorState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String>  stringData = env.socketTextStream("localhost",7777);

        DataStream<SensorReading> mappedData = stringData.map(new MapFunction<String, SensorReading>() {
            public SensorReading map(String var){
                String[] splited = var.split(",");
                return new SensorReading(splited[0],Long.valueOf(splited[1]),Double.valueOf(splited[2]));

            }

        });

        //定义一个有状态的map操作，统计分区数据个数
        SingleOutputStreamOperator<Integer> resultStream = mappedData.map(new MyCountMapper());

        resultStream.print("Result");

        env.execute();
    }

    // MyCountMapper(), 并且使用ListCheckpointed保存状态
    public static class MyCountMapper implements MapFunction<SensorReading,Integer>, ListCheckpointed<Integer> {

        //定义一个本地变量作为算子状态
        private Integer count = 0;

        public Integer map(SensorReading var){
            count+=1;
            return count;
        }

        //ListCheckpointed接口需要实现两个方法：snapshotState和restoreState
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception{
            return Collections.singletonList(this.count);
        }

        //从快照恢复状态
        public void restoreState(List<Integer> state) throws Exception{
            //
            if (!state.isEmpty()){
                count=state.get(0); //获取上一个保存点的（一个singletonlist 中的integer）计数数据
            }
            else{
                System.out.println("State is empty");
            }

        }


    }
}
