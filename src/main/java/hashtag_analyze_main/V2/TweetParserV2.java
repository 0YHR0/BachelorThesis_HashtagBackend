package hashtag_analyze_main.V2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Encapsulate the input String to the object of Tweet
 * @author Yang Haoran
 */
public class TweetParserV2 implements FlatMapFunction<String, TweetV2> {

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
