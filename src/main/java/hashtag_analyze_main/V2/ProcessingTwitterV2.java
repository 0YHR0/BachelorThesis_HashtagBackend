package hashtag_analyze_main.V2;


import Entity.Hashtag;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import hashtag_analyze_main.*;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.util.OutputTag;
import java.time.Instant;


/**
 * The main entrance of Flink(V2)
 * @author Yang Haoran
 */
public class ProcessingTwitterV2 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //set the time definition
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //set the checkpoint of Flink
        env.enableCheckpointing(30000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(50000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000L);
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(1);

        //set state backend
        env.setStateBackend(new MemoryStateBackend());

        //set the restart strategy
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10),Time.minutes(1)));

        //get the source of the Twitter API V2
        DataStream<String> twitterSource = env.addSource(new V2source());

        //defines the side output, to correct the result manually
        OutputTag<Hashtag> outputTag = new OutputTag<Hashtag>("late data"){};


        //parse the input String to the Tweet Object
        SingleOutputStreamOperator<Hashtag> resultStream = twitterSource.flatMap(new TweetParserV2())
                //flatmap the Tweet object to the Hashtag object
                .flatMap(new HashtagParserV2())
                //set the watermark: 500 ms
                //get the timestamp
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Hashtag>(org.apache.flink.streaming.api.windowing.time.Time.milliseconds(500)) {
                    @Override
                    public long extractTimestamp(Hashtag hashtag) {
                        String time = hashtag.getTime();
                        System.out.println("system time: " + System.currentTimeMillis());
                        Long ms = Instant.parse(time).toEpochMilli();
                        System.out.println("data of time: " + ms + "coming...");
                        return ms;
                    }
                })
                //collect the count of hashtags
                .keyBy("text")
                /**
                 *
                 *
                 *
                 * This part is used to release the pressure of the mysql, if for test, please annotate it
                 * -----------------------------------------------------------------------------------------------------
                 */
                .timeWindow(org.apache.flink.streaming.api.windowing.time.Time.seconds(30))
                .allowedLateness(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))
                .sideOutputLateData(outputTag)
                .aggregate(new AggregateHashtag())
                .keyBy("text")
                /**
                 * -----------------------------------------------------------------------------------------------------
                 * This part is used to release the pressure of the mysql, if for test, please annotate it
                 *
                 *
                 *
                 */
                .reduce(new HashtagReduce());


        resultStream.print("(hashtag, count)");

        //check the source of the stream
//        twitterSource.print("source");


        //write the result to mysql
        resultStream.addSink(new MysqlSink());

        //output the late coming data
        resultStream.getSideOutput(outputTag).print("late data");

        //set the time interval that watermark generated
        env.getConfig().setAutoWatermarkInterval(1000);

        //store the weekly, daily, monthly data.
        ComboPooledDataSource ds = new ComboPooledDataSource("mysql");
        Timer.setTimer(ds);

        //execute the job of analyzing hashtag.
        env.execute("hashtag count(V2)");



    }


}