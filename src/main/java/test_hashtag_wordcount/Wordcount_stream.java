package test_hashtag_wordcount;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Wordcount_stream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(4);
        String inputpath = "D:\\hashtag-Flink\\src\\main\\resources\\test.txt";
//        DataStream<String> inputSource= env.readTextFile(inputpath);
        //从服务器获取流数据
        DataStream<String> inputSource= env.socketTextStream("47.100.46.122",7777);

        //keyBy 根据string的hashcode进行重分区
        DataStream<Tuple2<String, Integer>> resultStream
                = inputSource.flatMap(new Wordcount_batch.MyFlatMapper()).keyBy(0).sum(1);
        //output
        resultStream.print();

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
