package com.Zac.apitest.state;

import com.Zac.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StateTest3_ApplicationCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> dataflow = env.socketTextStream("localhost", 7777);

        //转换成SensorReading
        DataStream<SensorReading> SensorReadingFlow = dataflow.map(new MapFunction<String, SensorReading>() {
            public SensorReading map(String var1) {
                String[] splited = var1.split(",");
                return new SensorReading(splited[0], Long.valueOf(splited[1]), Double.valueOf(splited[2]));
            }
        });

        //定义一个flatmap，监测温度跳变输出报警
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> resultStream = SensorReadingFlow
                .keyBy("id")
                .flatMap(new TempChangeWarning(10.0));


        resultStream.print();
        env.execute();
    }


    //实现自定义类
    public static class TempChangeWarning extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {

        //私有属性, 温度跳变阈值
        private Double threshold;

        //构造方法
        public TempChangeWarning(Double threshold) {
            this.threshold = threshold;
        }

        //定义状态，保存上一次的温度值
        private ValueState<Double> lastTempState;


        //生命周期初始阶段获取状态
        public void open(Configuration param) throws Exception {
            // getState: Gets a handle(ValueStateDescriptor) to the system's key/value list state
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class));
        }


        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {

            Double curTemp = value.getTemperature();
            //获取状态
            Double prevTemp = lastTempState.value();

            //对于每一个sensorreading，对比其温度和上一个温度的差值。 如果大于阈值则报警
            //对于第一个状态，将现有温度更新
            if (lastTempState.value() == null) {
                lastTempState.update(curTemp);
            } else {
                Double diff = Math.abs(curTemp - prevTemp);
                if (diff > threshold) {
                    out.collect(new Tuple3<>(value.getId(), prevTemp, curTemp));
                }
                lastTempState.update(curTemp);
            }

        }

        public void close() throws Exception {
            lastTempState.clear();
        }

    }

}










