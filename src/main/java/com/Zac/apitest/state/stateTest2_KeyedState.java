package com.Zac.apitest.state;

import com.Zac.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author huzhicong on 2021-07-25
 **/
public class stateTest2_KeyedState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> stringData = env.socketTextStream("localhost",7777);

        DataStream<SensorReading> mappedData = stringData.map(new MapFunction<String, SensorReading>() {
            public SensorReading map(String var){
                String[] splited = var.split(",");
                return new SensorReading(splited[0],Long.valueOf(splited[1]),Double.valueOf(splited[2]));

            }

        });

        //定义一个有状态的map操作，统计sensor数据个数
        SingleOutputStreamOperator<Integer> resultStream = mappedData
                .keyBy("id")
                .map(new MyKeyCount());



        resultStream.print("Result");

        env.execute();
    }

    public static class MyKeyCount extends RichMapFunction<SensorReading, Integer>{
        //声明一个keyed state　
        private ValueState<Integer> State;

        public void open(Configuration cfg) {
            //赋值keyCountState, 不能直接在外面实现。和生命周期有关
            State = getRuntimeContext().getState(
                    new ValueStateDescriptor<Integer>(
                            "myKeyCountState",
                            Integer.class,
                            0
                    )
            );
        };

        //实现一个map方法
        public Integer map(SensorReading var) throws Exception{
            //将ValueState的值拿出来
            Integer count;
            if (State.value()==null){
                count = 0;
            }
            else{
                count = State.value()+1;
            }

            //赋值回去
            State.update(count);
            return count;
        };






    }


}

// keyed stream  通过取模运算之后，相同key的数据会被放在一个子任务流当中（每个任务流可以包含多个key）
// 同一流中的每个key在每个算子中都可以有一个状态，叫做keyed state
// state的结构有：value， List，Map，Reducing&Aggregation
