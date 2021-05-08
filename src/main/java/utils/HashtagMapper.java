package utils;

import Entity.Hashtag;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * This class is use to encapsulate the data from the Database to Hashtag
 * @author Yang Haoran
 */
public class HashtagMapper implements RowMapper<Hashtag>{
    public Hashtag getRowMapper(ResultSet rs) throws SQLException {
        Hashtag hashtag = new Hashtag();
        hashtag.setId(rs.getInt(1));
        hashtag.setText(rs.getString(2));
        hashtag.setLocation(rs.getString(3));
        hashtag.setCount(rs.getInt(4));
        return hashtag;
    }
}
