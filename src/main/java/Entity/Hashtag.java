package Entity;

public class Hashtag {
    public String text;
    public int count;
    public String location;
    public int id;

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "Hashtag{" +
                "text='" + text + '\'' +
                ", count=" + count +
                ", location='" + location + '\'' +
                ", id=" + id +
                '}';
    }
}
