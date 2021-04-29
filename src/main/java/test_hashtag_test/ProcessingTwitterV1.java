package test_hashtag_test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;


import java.util.Properties;

public class ProcessingTwitterV1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //get the properties used to link to Twitter API
        Properties twitterProperties = new Properties();

        twitterProperties.load(ClassLoader.getSystemClassLoader().getResourceAsStream("twitter.properties"));
        Properties props = new Properties();
        props.setProperty(TwitterSource.CONSUMER_KEY, twitterProperties.getProperty("CONSUMER_KEY"));
        props.setProperty(TwitterSource.CONSUMER_SECRET, twitterProperties.getProperty("CONSUMER_SECRET"));
        props.setProperty(TwitterSource.TOKEN, twitterProperties.getProperty("TOKEN"));
        props.setProperty(TwitterSource.TOKEN_SECRET, twitterProperties.getProperty("TOKEN_SECRET"));
        TwitterSource twitterSource = new TwitterSource(props);
        //get the filter of the twitter
        FilterTwitterStreamV1 customFilterInitializer = new FilterTwitterStreamV1();
        twitterSource.setCustomEndpointInitializer(customFilterInitializer);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<String> streamSource = env.addSource(twitterSource);

        streamSource.flatMap(new TweetParser())//encapsulate the string to the tweet object
                .map(new TweetKeyValue())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<TweetV1, Integer>>(Time.milliseconds(999)) {
            @Override
            public long extractTimestamp(Tuple2<TweetV1, Integer> tweetIntegerTuple2) {
                // return tweetIntegerTuple2.f0.time;
                return 1000;
            }
        })//map to key-value
                .keyBy(new KeySelector<Tuple2<TweetV1, Integer>, String>() {
                    public String getKey(Tuple2<TweetV1, Integer> tweetIntegerTuple2) throws Exception {
                        return tweetIntegerTuple2.f0.source;//use hashtag to classify
                    }
                })
                .timeWindow(Time.seconds(1))
//                .apply(new WindowFunction<Tuple2<Tweet, Integer>, Object, String, TimeWindow>() {
//                    @Override
//                    public void apply(String s, TimeWindow timeWindow, Iterable<Tuple2<Tweet, Integer>> iterable, Collector<Object> collector) throws Exception {
//
//                    }
//                })
//                .aggregate(new AggregateFunction<Tuple2<Tweet, Integer>, Integer, Integer>() {
//                    @Override
//                    public Integer createAccumulator() {
//                    //初始化累加器的值为0
//                        return 0;
//                    }
//                    //每条数据过来之后累加器加一
//                    @Override
//                    public Integer add(Tuple2<Tweet, Integer> tweetIntegerTuple2, Integer accumulator) {
//                        return accumulator + 1;
//                    }
//                    //返回累加器的值
//                    @Override
//                    public Integer getResult(Integer accumulator) {
//                        return accumulator;
//                    }
//                    //对多个窗口的结果进行聚合
//                    @Override
//                    public Integer merge(Integer a, Integer b) {
//                        return null;
//                    }
//                })


                .sum(1)
                .print();

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }

    public static class TweetKeyValue implements MapFunction<TweetV1, Tuple2<TweetV1, Integer>> {

        /**
         * The mapping method. Takes an element from the input data set and transforms
         * it into exactly one element.
         *
         * @param tweetV1 The input value.
         * @return The transformed value
         * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
         *                   to fail and may trigger recovery.
         */

        public Tuple2<TweetV1, Integer> map(TweetV1 tweetV1) throws Exception {
            return new Tuple2<TweetV1, Integer>(tweetV1,1);
        }
    }

    public static class TweetParser implements FlatMapFunction<String, TweetV1> {

        /**
         * The core method of the FlatMapFunction. Takes an element from the input data set and transforms
         * it into zero, one, or more elements.
         *
         * @param value The input value.
         * @param collector   The collector for returning result values.
         * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
         *                   to fail and may trigger recovery.
         */

        public void flatMap(String value, Collector<TweetV1> collector) throws Exception {
            TweetV1 tweetV1 = TweetV1.fromString(value);//the value is the format of json
            if (tweetV1 != null) {
                collector.collect(tweetV1);
            }
        }
    }
    }


