package hashtag_analyze_main.V2;

import Entity.Hashtag;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * This class is used to parse the TweetV2 object to hashtag
 * @author Yang Haoran
 */
public class HashtagParserV2 implements FlatMapFunction<TweetV2, Hashtag> {
    /**
     * parse the tweet to hashtag(one tweet --> n hashtags)
     * @param tweetV2 the input tweet
     * @param collector the output
     * @throws Exception
     */

    @Override
    public void flatMap(TweetV2 tweetV2, Collector<Hashtag> collector) throws Exception {
        List<String> hashtags = tweetV2.getHashtags();
        for(String hashtag:hashtags){
            Hashtag hashtagEntity = new Hashtag();
            hashtagEntity.setText(hashtag);
            hashtagEntity.setCount(1);
            hashtagEntity.setTime(tweetV2.getTime());
            collector.collect(hashtagEntity);
        }
    }
}
