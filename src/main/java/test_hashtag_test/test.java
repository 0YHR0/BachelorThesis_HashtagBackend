package test_hashtag_test;


import utils.DBUtil;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

public class test {
    public static void main(String[] args) throws IOException, SQLException {
//        Properties properties = new Properties();
//        properties.load(ClassLoader.getSystemClassLoader().getResourceAsStream("db.properties"));
//        System.out.println(properties.getProperty("username"));
        String sql = "INSERT into hashtag_status (id, hashtag, location, count) values (default, ?,?,?)";
        System.out.println(DBUtil.executeUpdate(sql, "yang","China Shanghai", 10));


    }
}