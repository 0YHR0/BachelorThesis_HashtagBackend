package hashtag_analyze_main;

import Entity.Hashtag;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

/**
 * count the number of the hashtags, and use the key state to store the Hashtag
 * @author Yang Haoran
 */
public class HashtagReduce extends RichReduceFunction<Hashtag> {
    public ValueState<Hashtag> countState;

    @Override
    public void open(Configuration parameters) throws Exception {
        //initialize the state
        countState = getRuntimeContext().getState(new ValueStateDescriptor<Hashtag>("key-count", Hashtag.class, new Hashtag()));
        super.open(parameters);
    }

    @Override
    public Hashtag reduce(Hashtag hashtag, Hashtag t1) throws Exception {
        Hashtag newHashtag = countState.value();
        newHashtag.setText(hashtag.text);
        newHashtag.setCount(hashtag.count+t1.count);
        countState.update(newHashtag);
        return newHashtag;
    }
}
