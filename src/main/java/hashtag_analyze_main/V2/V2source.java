package hashtag_analyze_main.V2;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
/**
 * This class is used to get the stream of the TwitterV2 api
 * @author Yang Haoran
 */
public class V2source implements SourceFunction<String> {
    private boolean running = true;
    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        InputStream stream = FilterTwitterStreamV2.getStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        String line = reader.readLine();
        while(running){
            while (line != null) {
                sourceContext.collect(line);
                line = reader.readLine();
            }
        }

    }

    @Override
    public void cancel() {
        running = false;

    }
}
