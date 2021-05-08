package hashtag_analyze_main.V1;

import Entity.Hashtag;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import hashtag_analyze_main.HashtagReduce;
import hashtag_analyze_main.MysqlSink;
import hashtag_analyze_main.Timer;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;


import java.util.Properties;

/**
 * The main entrance of Processing TweetV1 (The backup of V2)
 * @author Yang Haoran
 */
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

        //set the checkpoint of Flink
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000L);
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(1);

        //set the restart strategy
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10),Time.minutes(1)));

        //set state backend
        env.setStateBackend(new MemoryStateBackend());




        //process the data
        DataStream<Hashtag> resultStream = streamSource.flatMap(new TweetParserV1())//encapsulate the string to the tweet object
                .flatMap(new HashtagParserV1())
                .keyBy("text")
                .reduce(new HashtagReduce());

        resultStream.print();
        resultStream.addSink(new MysqlSink());

        //store the weekly, daily, monthly data.
        ComboPooledDataSource ds = new ComboPooledDataSource("mysql");
        Timer.setTimer(ds);

        // execute program
        env.execute("Flink Twitter V1");
    }
    }


