package hashtag_analyze_main.V1;

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
public class TweetV1 {

    private String text;
    private String userName;
    public String rawText;//the initial text
    public String source = "unknow";
    public List<String> hashtags = new ArrayList<String>();
    public String hashtagStr = "";

    private TweetV1() {
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getRawText() {
        return rawText;
    }

    public void setRawText(String rawText) {
        this.rawText = rawText;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public List<String> getHashtags() {
        return hashtags;
    }

    public void setHashtags(List<String> hashtags) {
        this.hashtags = hashtags;
    }

    public String getHashtagStr() {
        return hashtagStr;
    }

    public void setHashtagStr(String hashtagStr) {
        this.hashtagStr = hashtagStr;
    }

    /**
     * The function is used to pass the string to json format and encapsulate it to Tweet
     * @param s the input
     * @return the object Tweet
     */
    public static TweetV1 fromString(String s) {

        ObjectMapper jsonParser = new ObjectMapper();
        TweetV1 tweetV1 = new TweetV1();
        tweetV1.rawText = s;

        try {
            JsonNode node = jsonParser.readValue(s, JsonNode.class);

            //get the user of the tweet
            if(node.has("user")){
                JsonNode userNode = node.get("user");
                tweetV1.userName = userNode.get("name").asText();
            }

            //get the text of tweet
            if(node.has("text")) {
                tweetV1.text = node.get("text").asText();
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

                    tweetV1.source =source;
            }

            //get the hashtag of the tweet
            if(node.has("entities")){
                JsonNode entitiesNode = node.get("entities");
                if(entitiesNode.has("hashtags")){
                    Iterator<JsonNode> elements = entitiesNode.get("hashtags").elements();
                    while(elements.hasNext()){
                        JsonNode t = elements.next();
                        tweetV1.hashtags.add(t.get("text").asText());
                        tweetV1.hashtagStr  = tweetV1.hashtagStr +"---" + t.get("text").asText();
                    }
                }



            }


                return tweetV1;


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
                "hashtags: " +this.hashtagStr + "; " ;
    }
}
