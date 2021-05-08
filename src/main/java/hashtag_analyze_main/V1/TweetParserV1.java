package hashtag_analyze_main.V1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * This class is used to parse the string to tweet
 * @author Yang Haoran
 */
public class TweetParserV1 implements FlatMapFunction<String, TweetV1> {

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
