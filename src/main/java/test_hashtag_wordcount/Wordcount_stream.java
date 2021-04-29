package test_hashtag_wordcount;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Wordcount_stream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(4);
        String inputpath = "D:\\BC_hashtag\\hashtags\\src\\main\\resources\\test.txt";
        DataStream<String> inputSource= env.readTextFile(inputpath);
        //从服务器获取流数据
//        DataStream<String> inputSource= env.socketTextStream("47.100.46.122",7777);
//        InputStreamReader inputStreamReader = FilterTwitterStream.getStream();

        //keyBy 根据string的hashcode进行重分区
        DataStream<Tuple2<String, Integer>> resultStream
                = inputSource.flatMap(new Wordcount_batch.MyFlatMapper()).keyBy(0).sum(1);
//        map demo
//        DataStream<Integer> resultStream = inputSource.map(new MapFunction<String, Integer>() {
//            @Override
//            public Integer map(String s) throws Exception {
//                return s.length();
//            }
//        });


//      flatmap demo
//        DataStream<String> resultStream = inputSource.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public void flatMap(String s, Collector<String> collector) throws Exception {
//                String[] res = s.split(" ");
//                for(String r:res){
//                    collector.collect(r);
//                }
//
//            }
//        });

        //filter demo
//        DataStream<String> resultStream = inputSource.filter(new FilterFunction<String>() {
//            @Override
//            public boolean filter(String s) throws Exception {
//                return s.startsWith("h");
//            }
//        });

        //output
//        resultStream.print();

        //执行任务
        env.execute();
        //默认并行度为cpu核心数，可以理解为多线程同时操作，即多个分区同时处理
        //result:
        //1> (ok!,1)
        //4> (are,1)
        //5> (you,1)
        //5> (world,1)
        //6> (how,1)
        //3> (hello,1)
        //3> (I,1)
        //2> (yang,1)
        //3> (hello,2)
        //2> (am,1)
    }


}
