package com.Zac.wc;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class streamWordCountwithNetCat {
    public static void main(String[] args) throws Exception{
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(8); //线程数，线程数要小于slot数
                                    // slot指最大可用并行数，Parallelism指算子实际使用并行数

        // parameter tool 从启动参数提取配置项
        ParameterTool pTool = ParameterTool.fromArgs(args);
        String host = pTool.get("host");
        int port = pTool.getInt("port");

        //从文本流读取数据(netcat)
        //nc -lp 7777
        //Run--Edit configration -- main函数的参数 --host localhost --port 7777
        DataStream<String> inputDataStream = env.socketTextStream(host,port);


        //基于数据流进行转换
        //使用keyby，按照当前key的hash code对数据做一个重分区
        //这里只是定义任务　
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new myFlatMapper())
                .keyBy(0)
                .sum(1);

        //因为是流数据，所以需要等数据来才行
        resultStream.print();

        //执行任务
        env.execute();
    }

    //源码： flatMap(FlatMapFunction<T, R> flatMapper)
    //用myFlatMapper实现FlatMapFunction
    // 这里实现源码中的 void flatMap(T var1, Collector<O> var2)

    public static class myFlatMapper implements FlatMapFunction<String, Tuple2<String,Integer>> {

        public void flatMap(String value, Collector<Tuple2<String,Integer>> out){
            String[] words = value.split(" ");
            for (String word:words){
                out.collect(new Tuple2<String,Integer> (word,1) );
            }
        }
    }

}

/*
####################################################
cli提交：

./bin/flink run -c com.Zac.wc.streamWordCountwithNetCat\
                -p 1\
 ~/uni/PycharmProjects/FlinkTutorial/target/FlinkTutorial-1.0-SNAPSHOT.jar \
 --host localhost --port 7777

###################################################
cli list job:

./bin/flink list (获取job id）
###################################################
cli cancel job:

./bin/flink cancel 1c0dec4f55909d31222f5ac91701246d
###################################################
 */

