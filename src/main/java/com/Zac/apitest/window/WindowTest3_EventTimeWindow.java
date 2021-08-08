package com.Zac.apitest.window;

import com.Zac.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;

/**
 * @author huzhicong on 2021-06-28
 **/
public class WindowTest3_EventTimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        //设置时间语义为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);//基于事件时间
        env.getConfig().setAutoWatermarkInterval(5);//设置watermark产生的间隔

        //socket file
        DataStream<String> stream = env.socketTextStream("localhost",7777);

        //map,watermark
        DataStream<SensorReading> mappedwatermarkedStream = stream.map(new MapFunction<String,SensorReading>(){
            public SensorReading map(String var1){
                String[] spiltted = var1.split(",");
                return new SensorReading(spiltted[0],Long.valueOf(spiltted[1]),Double.valueOf(spiltted[2]));
            }
        })
        //获取时间戳,时间戳必须是毫秒数
        //Time.seconds() 输入乱序程度，两秒延迟
        //AssignerWithPunctuatedWatermarks
        //AssignerWithPeriodicWatermarks
        //BoundedOu....继承AssignerWithPeriodicWatermarks，只有在当前最大事件-延迟>上一个watermart的时候，才会更新
        //不会每条数据都放一个watermark
        //可以看看Bounded..的文档了解怎么实现assignTimestampsAndWatermarks
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {


            public long extractTimestamp (SensorReading element) {
                return element.getTimestamp()*1000L;
            }
        });

        //基于事件时间的开窗,统计15秒内温度的最小值
        SingleOutputStreamOperator<SensorReading> minTempStream = mappedwatermarkedStream.keyBy("id")
                .timeWindow((Time.seconds(5)))
                .minBy("temperature");

        minTempStream.print("minTemp");




        /**
        mappedStream.assignTimestampsAndWatermarks((new AssignerWithPeriodicWatermarks<SensorReading>() {

            private Long bound = 60 * 1000L; //延迟一分钟
            private Long MaxTs = Long.MIN_VALUE;


            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(MaxTs-bound);
            }

            @Override
            public long extractTimestamp(SensorReading element, long previousElementTimestamp) {
                Long maxTs = Math.max(MaxTs,element.getTimestamp()); //比较当前数据的事件和最大事件


                return element.getTimestamp();
            }
        }));
            **/


        /**
         * 非乱序数据
         * 无等待时间参数
         * mappedStream.assignTimestampsAndWatermarks(AscendingTimeStampExtractor<SensorReading>(){
         *     @Override
         *             public long extractTimestamp(SensorReading element) {
         *                 return element.getTimestamp()*1000L;
         *             }
         * }
         */





        //execute
        env.execute();

    }
}


/**
 *
 * 一般来说，对于一个窗口，事件事件前闭后开
 * 对于乱序数据来说，晚发生的事件可能比早发生的事件来得早
 * watermark允许window在接收到当前窗口最晚时间的事件之后不立即关闭窗口输出，而是等一段时间
 * 这样可以使晚来但是更早发生的数据被搜集到桶内
 * https://wbice.cn/article/Flink-Watermark.html#3-4%E3%80%81%E6%B0%B4%E4%BD%8D%E7%BA%BF%E8%A7%A3%E5%86%B3%E4%B9%B1%E5%BA%8F%E7%9A%84%E5%8E%9F%E7%90%86
 **/

/**
 * 当多并行度的时候，（以这个程序作为例子）
 * 数据源采用轮询将数据传给四个map算子
 * 每一个算子的watermark为算子当前的watermark。
 * 但对于下游的keyby来说，watermark为上一步map算子的四个watermark最小的一个。只有当四个map算子中最小的watermark都超过窗口之后，keyby+window才会输出
 */

/**
 * 事件时间：1547718799
 * watermark： 2s
 * watermark时间： 1547718797
 * 允许迟到时间： 60s
 * 窗口：1547718795 - 1547718810
 * 第一个窗口关闭时间：= 1547718810（第一个窗口结束） - 2 （watermark延迟） +60 （允许迟到时间）
 * 在窗口结束到窗口关闭前，每来一个数据，窗口就会输出一次
 */
