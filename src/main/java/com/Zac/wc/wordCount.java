package com.Zac.wc;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class wordCount {
    public static void main(String[] args) throws Exception{
        //创建执行环境
        //类似spark的sc
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 读取文件
        String inputPath="/Users/huzhicong/uni/PycharmProjects/FlinkTutorial/src/main/resources/hello.txt";

        //DataSource 是一个 operator，是一个dataset
        //Flink中的DataSet程序是实现数据集转换的常规程序（例如，Filter，映射，连接，分组）。数据集最初是从某些来源创建的（例如，通过读取文件或从本地集合创建）。
        //DataSource的父类是dataset
        DataSource<String> inputDataset = env.readTextFile(inputPath);

        //对数据集进行处理
        //数据集转换，格式为（"word"，1），然后mapreduce
        //flatMap是一个接口，其实现类返回值为void，需要两个参数： T var1, Collector<O> var2
        //var1 为输入， var2为收集器类，同来接收数据（调用Collector.coillect)
        DataSet<Tuple2<String,Integer>> resultSet = inputDataset.flatMap(new MyFlatMapper())
                    .groupBy(0)  //按照第0个位置的word groupby
                    .sum(1)
                    .sortPartition(1, Order.DESCENDING);      //groupby 的function为sum

        resultSet.print();
    }




    //---------------------------------------------
    //自定义类，实现FlatMapFunction接口
    //所以flatMap(new MyFlatMapper())调用接口--->
    // FlatMapFunction接口--->
    // MyFlatMapper实现类

    // 接口的两个参数 T和O为范型，这里定义为String和flink提供的元组类型Tuple2
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String,Integer>>{

        //类方法，获得value，输出Collector
        public void flatMap(String value, Collector<Tuple2<String,Integer>> out) throws Exception{
            //按空格分词
            String[] words = value.split(" ");

            //遍历所有word，包装成二元祖
            //for String word in Words
            for (String word:words)
                //使用collector收集每个单词，并转化为元祖
                out.collect(new Tuple2<String,Integer>(word,1));
        }

    }

    //---------------------------------------------

    //---------------------------------------------


}
