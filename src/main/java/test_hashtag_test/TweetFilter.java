package test_hashtag_test;

import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import java.io.Serializable;
import java.util.Arrays;

public class TweetFilter implements TwitterSource.EndpointInitializer, Serializable {
    @Override
    public StreamingEndpoint createEndpoint() {
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(Arrays.asList("Germany","German"));
        endpoint.languages(Arrays.asList("en"));
//        endpoint.addPostParameter("has","hashtags");
        return endpoint;
    }
}
