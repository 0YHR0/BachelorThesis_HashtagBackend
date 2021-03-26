package test_hashtag_test;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import utils.DBUtil;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;

public class test {
    public static void main(String[] args) throws Exception {
//        Properties properties = new Properties();
//        properties.load(ClassLoader.getSystemClassLoader().getResourceAsStream("db.properties"));
//        System.out.println(properties.getProperty("username"));
//        String sql = "INSERT into hashtag_status (id, hashtag, location, count) values (default, ?,?,?)";
//        System.out.println(DBUtil.executeUpdate(sql, "yang","China Shanghai", 10));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //从集合中读取数据，可以把类的对象封装成集合
//        DataStream<String> collectionSource = env.fromCollection(Arrays.asList("aaa", "bbb", "ccc"));
        //直接读取元素
//        DataStream<String> eleSource = env.fromElements("as", "sss", "sda");

        //输出,参数可以区别哪个输出
//        collectionSource.print("collection");
//        eleSource.print("element");
        DataStream<String> testSource = env.addSource(new V2source());
        testSource.flatMap(new TweetParserV2())
                  .print("y");

        //参数为jobname
        env.execute("jobone");


    }


    public static class TweetParserV2 implements FlatMapFunction<String, TweetV2> {

        /**
         * The core method of the FlatMapFunction. Takes an element from the input data set and transforms
         * it into zero, one, or more elements.
         *
         * @param value The input value.
         * @param collector   The collector for returning result values.
         * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
         *                   to fail and may trigger recovery.
         */

        public void flatMap(String value, Collector<TweetV2> collector) throws Exception {
            if(!value.equals("")){
            TweetV2 tweet = TweetV2.fromString(value);//the value is the format of json
            if (tweet != null) {
                collector.collect(tweet);
            }
        }}
    }
}