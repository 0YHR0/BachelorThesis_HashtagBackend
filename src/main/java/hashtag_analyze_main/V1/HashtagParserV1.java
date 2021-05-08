package hashtag_analyze_main.V1;

import Entity.Hashtag;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * This class is used to parse the Tweet to Hashtag
 * @author Yang Haoran
 */
public class HashtagParserV1 implements FlatMapFunction<TweetV1, Hashtag> {

    @Override
    public void flatMap(TweetV1 tweetV1, Collector<Hashtag> collector) throws Exception {
        if(!tweetV1.hashtagStr.equals("")){
            for(String hashtag:tweetV1.hashtags){
                Hashtag hashtagEntity = new Hashtag();
                hashtagEntity.setText(hashtag);
                hashtagEntity.setCount(1);
                collector.collect(hashtagEntity);
            }
        }
    }
}
