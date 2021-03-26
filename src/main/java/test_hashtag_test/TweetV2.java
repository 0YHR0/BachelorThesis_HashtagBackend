package test_hashtag_test;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The class defines the attribute of a tweet
 * @author Yang Haoran
 */
public class TweetV2 {

    private String text;
    private String userName;
    public String rawText;//the initial text
    public String source;
    public List<String> hashtags = new ArrayList<String>();
    public String hashtagStr = "";

    private TweetV2() {
    }

    /**
     * The function is used to pass the string to json format and encapsulate it to Tweet
     * @param s the input
     * @return the object Tweet
     */
    public static TweetV2 fromString(String s) {

        ObjectMapper jsonParser = new ObjectMapper();
        TweetV2 tweet = new TweetV2();
        tweet.rawText = s;

        try {
            JsonNode node = jsonParser.readValue(s, JsonNode.class);

            //get the user of the tweet
            if(node.has("user")){
                JsonNode userNode = node.get("user");
                tweet.userName = userNode.get("name").asText();
            }

            //get the text of tweet
            if(node.has("text")) {
                tweet.text = node.get("text").asText();
            }

            //get the source of tweet
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

            //get the hashtag of the tweet
            if(node.has("data")){
                JsonNode dataNode = node.get("data");
             if(dataNode.has("entities")){
                JsonNode entitiesNode = dataNode.get("entities");
                if(entitiesNode.has("hashtags")){
                    System.out.println("1111111111111111");
                    //!!!!!!!!!!!!!!!!
//                    tweet.hashtags = String.valueOf(entitiesNode.get("hashtags").elements());
//                    for (Iterator<JsonNode> it = entitiesNode.get("hashtags").elements(); it.hasNext(); ) {
//                        JsonNode t = it.next();
//                        tweet.hashtags.add(t.asText());
//                        System.out.println("11" + t.asText());
//                        tweet.hashtagStr += t.asText();
//                    }
                    Iterator<JsonNode> elements = entitiesNode.get("hashtags").elements();
                    while(elements.hasNext()){
                        JsonNode t = elements.next();
                        tweet.hashtags.add(t.get("tag").asText());
//                        System.out.println(t.get("text").asText());
                        tweet.hashtagStr += (t.get("tag").asText() + "---");
                    }
                }



            }}



            return tweet;


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
                "source: " +this.source + "; " +
                "hashtags: " +this.hashtagStr + "; " +
                "raw..........: " + this.rawText + " ";
    }
}
