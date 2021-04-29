package test_hashtag_test;


import Entity.Hashtag;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

public class ProcessingTwitterV2 {
    public static void main(String[] args) throws Exception {
//        Properties properties = new Properties();
//        properties.load(ClassLoader.getSystemClassLoader().getResourceAsStream("db.properties"));
//        System.out.println(properties.getProperty("username"));
//        String sql = "INSERT into hashtag_status (id, hashtag, location, count) values (default, ?,?,?)";
//        System.out.println(DBUtil.executeUpdate(sql, "yang","China Shanghai", 10));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.setParallelism(1);
        //从集合中读取数据，可以把类的对象封装成集合
//        DataStream<String> collectionSource = env.fromCollection(Arrays.asList("aaa", "bbb", "ccc"));
        //直接读取元素
//        DataStream<String> eleSource = env.fromElements("as", "sss", "sda");

        //输出,参数可以区别哪个输出
//        collectionSource.print("collection");
//        eleSource.print("element");
        DataStream<String> twitterSource = env.addSource(new V2source());
//        testSource.flatMap(new TweetParserV2())
//                  .print("y");

        DataStream<Tuple2<String,Integer>> resultStream = twitterSource.flatMap(new TweetParserV2())
                .flatMap(new HashtagParserV2())
                .keyBy(0)
                .sum(1);
        resultStream.print("test2");
        resultStream.addSink(new MysqlSink());
//                .print("test");
//        twitterSource.addSink(new MysqlSink());


        //参数为jobname
        env.execute("jobone");


    }

    /**
     * This class is used to parse the TweetV2 object to hashtag
     */
    public static class HashtagParserV2 implements FlatMapFunction<TweetV2, Tuple2<String, Integer>>{

        @Override
        public void flatMap(TweetV2 tweetV2, Collector<Tuple2<String, Integer>> collector) throws Exception {
            List<String> hashtags = tweetV2.getHashtags();
            for(String hashtag:hashtags){
                collector.collect(new Tuple2<String, Integer>(hashtag,1));
            }
        }
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

    /**
     * use the connection pool to get the connection,because several thread will connect to the mysql
     */
    public static class MysqlSink extends  RichSinkFunction<Tuple2<String, Integer>> {
        private static ComboPooledDataSource ds = null;
        private static Connection conn = null;
        private static PreparedStatement ps = null;

        //get the connection pool
        static{
            try{
                ds = new ComboPooledDataSource("mysql");
            }catch (Exception e) {
                throw new ExceptionInInitializerError(e);
            }

        }

        @Override
        public void open(Configuration parameters) throws Exception {
            conn = ds.getConnection();
            String sql = "Insert INTO hashtag_status (id,hashtag,location,count) values (default,?,'unknown',?) ON DUPLICATE KEY UPDATE count=?";
            ps = conn.prepareStatement(sql);
        }

        @Override
        public void close() throws Exception {
            ps.close();
            conn.close();
        }

        @Override
        public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
            ps.setObject(1, value.f0);
            ps.setObject(2, value.f1);
            ps.setObject(3, value.f1);
            int result = ps.executeUpdate();
            if(result == 0){
                throw new Exception("update mysql failed");
            }


        }
    }
}