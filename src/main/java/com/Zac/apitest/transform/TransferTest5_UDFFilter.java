package com.Zac.apitest.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransferTest5_UDFFilter {
    public static void main(String[] args) throws Exception{

        ParameterTool pTool = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> source = env.readTextFile(pTool.get("path"));

        DataStream<String> result = source.filter( new myFilterFunction("Sensor_1"));

        result.print();

        env.execute();
    }

    public static class myFilterFunction implements FilterFunction<String>{

        private String filterString;

        public myFilterFunction(String filterString){
            this.filterString = filterString;
        }

        public boolean filter(String var1){
            return var1.contains(this.filterString);
        }
    }
}
