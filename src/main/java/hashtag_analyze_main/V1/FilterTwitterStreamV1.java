package hashtag_analyze_main.V1;

import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import java.io.Serializable;
import java.util.Arrays;

/**
 * This class is used to filter the Tweet of V1 API
 * @author Yang Haoran
 */
public class FilterTwitterStreamV1 implements TwitterSource.EndpointInitializer, Serializable {
    @Override
    public StreamingEndpoint createEndpoint() {
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(Arrays.asList("Germany","German"));
        endpoint.languages(Arrays.asList("en"));

        //does not have the authority
//        endpoint.addPostParameter("has","hashtags");


        return endpoint;
    }
}
