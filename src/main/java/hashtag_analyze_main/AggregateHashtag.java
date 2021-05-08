package hashtag_analyze_main;

import Entity.Hashtag;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * This class is used to aggregate the hashtag in each window
 * @author Yang Haoran
 */
public class AggregateHashtag implements AggregateFunction<Hashtag, Hashtag, Hashtag> {
    @Override
    public Hashtag createAccumulator() {
        return new Hashtag();
    }

    @Override
    public Hashtag add(Hashtag hashtag, Hashtag hashtagAccumulator) {
        if(hashtagAccumulator.text.equals("")){
            hashtagAccumulator.text = hashtag.text;
        }
        hashtagAccumulator.count += hashtag.count;
        return hashtagAccumulator;
    }

    @Override
    public Hashtag getResult(Hashtag hashtagAccumulator) {
        return hashtagAccumulator;
    }

    @Override
    public Hashtag merge(Hashtag hashtagAccumulator1, Hashtag hashtagAccumulator2) {
        if(hashtagAccumulator1.text.equals("")) System.err.println("hashtag text is empty");
        if(hashtagAccumulator2.text.equals("")) System.err.println("hashtag text is empty");
        Hashtag result = new Hashtag();
        result.setText(hashtagAccumulator1.text);
        result.setCount(hashtagAccumulator1.count+hashtagAccumulator2.count);
        return result;
    }
}
