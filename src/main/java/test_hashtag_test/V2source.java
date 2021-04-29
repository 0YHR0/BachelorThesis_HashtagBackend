package test_hashtag_test;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

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
