package com.Zac.wc;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class streamWordCount {
    public static void main(String[] args) throws Exception{
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(8); //线程数

        //读取文件
        String dataSource = "/Users/huzhicong/uni/PycharmProjects/FlinkTutorial/src/main/resources/hello.txt";

        //source 为DataStreamSource类
        DataStreamSource<String> inputDataStream = env.readTextFile(dataSource);


        //基于数据流进行转换
        //使用keyby，按照当前key的hash code对数据做一个重分区
        //这里只是定义任务　
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new myFlatMapper())
                .keyBy(0)
                .sum(1).setParallelism(2);

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

/**
 *
 * 类似3>的含义是：线程编号（并行任务），默认并行度为4
 *
 * 输出：
 * 3> (enu,1) //enu 计算一次，这里保存了一个状态
 * 1> (buif,1)
 * 8> (oidnd,1)
 * 5> (dn,1)
 * 4> (byivuyvtuctycdyt,1)
 * 5> (d,1)
 * 6> (vyvyu,1)
 * 7> (onoid,1)
 * 2> (nud,1)
 * 2> (snuod,1)
 * 6> (vyvyu,2)
 * 6> (nuin,1)
 * 6> (oi,1)
 * 4> (byivuyvtuctycdyt,2)
 * 4> (vuyvuy,1)
 * 4> (vuyvuy,2)
 * 4> (denod,1)
 * 2> (snuod,2)
 * 3> (enu,2)  //enu计算了第二次，这里拿出之前保存的状态加上新的
 * 4> (vyv,1)
 * 5> (dn,2)
 * 3> (yivivyi,1)
 * 1> (biugyifyufuy,1)
 * 4> (vyvyivivyi,1)
 * 8> (oidnd,2)
 * 1> (biugyifyufuy,2)
 * 3> (yivivyi,2)
 * 4> (vyv,2)
 * 1> (buif,2)
 * 7> (onoid,2)
 * 4> (denod,2)
 * 2> (nud,2)
 * 5> (d,2)
 * 6> (nuin,2)
 * 6> (oi,2)
 *
 */
/*
*点击maven-生命周期-compile,然后点击package打包
* 在target/下会有打包好的jar文件
 */
