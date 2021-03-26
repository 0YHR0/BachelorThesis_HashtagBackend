package test_hashtag_test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;


import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Properties;

public class Twitter_test {
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
        TweetFilter customFilterInitializer = new TweetFilter();
        twitterSource.setCustomEndpointInitializer(customFilterInitializer);
        DataStream<String> streamSource = env.addSource(twitterSource);

        streamSource.flatMap(new TweetParser())//encapsulate the string to the tweet object
                .map(new TweetKeyValue())//map to key-value
                .keyBy(new KeySelector<Tuple2<Tweet, Integer>, String>() {
                    public String getKey(Tuple2<Tweet, Integer> tweetIntegerTuple2) throws Exception {
                        return tweetIntegerTuple2.f0.hashtagStr;//use hashtag to classify
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))//5 seconds one analyze
                .sum(1)
                .print();

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }

    public static class TweetKeyValue implements MapFunction<Tweet, Tuple2<Tweet, Integer>> {

        /**
         * The mapping method. Takes an element from the input data set and transforms
         * it into exactly one element.
         *
         * @param tweet The input value.
         * @return The transformed value
         * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
         *                   to fail and may trigger recovery.
         */

        public Tuple2<Tweet, Integer> map(Tweet tweet) throws Exception {
            return new Tuple2<Tweet, Integer>(tweet,1);
        }
    }

    public static class TweetParser implements FlatMapFunction<String, Tweet> {

        /**
         * The core method of the FlatMapFunction. Takes an element from the input data set and transforms
         * it into zero, one, or more elements.
         *
         * @param value The input value.
         * @param collector   The collector for returning result values.
         * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
         *                   to fail and may trigger recovery.
         */

        public void flatMap(String value, Collector<Tweet> collector) throws Exception {
            Tweet tweet = Tweet.fromString(value);//the value is the format of json
            if (tweet != null) {
                collector.collect(tweet);
            }
        }
    }
    }


