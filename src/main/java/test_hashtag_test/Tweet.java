package test_hashtag_test;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class Tweet {

    private String text;
    private String userName;
    public String rawText;
    public String lang;
    public String source;

    private Tweet() {
    }

    public static Tweet fromString(String s) {

        ObjectMapper jsonParser = new ObjectMapper();
        Tweet tweet = new Tweet();
        tweet.rawText = s;

        try {
            JsonNode node = jsonParser.readValue(s, JsonNode.class);
            Boolean isLang = node.has("user") && node.get("user").has("lang");// &&
            // !node.get("user").get("lang").asText().equals("null");

            if(isLang && node.has("text"))
            {
                JsonNode userNode = node.get("user");
                tweet.text = node.get("text").asText();
                tweet.userName = userNode.get("name").asText();
                tweet.lang = userNode.get("lang").asText();

                if(node.has("source")){

                    String source = node.get("source").asText().toLowerCase();
                    if(source.contains("android"))
                        source = "Android";
                    else if (source.contains("iphone"))
                        source="iphone";
                    else if (source.contains("web"))
                        source="web";
                    else
                        source="unknow";

                    tweet.source =source;
                }


                return tweet;
            }

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;

    }

    @Override
    public String toString() {
        return "username: " + this.userName+ "; " +
                "lang: " + this.lang + "; " +
                "source: " +this.source + "; " +
//                "text: " +this.text + "; ";
         "raw..........: " + this.rawText + " ";
    }
}
