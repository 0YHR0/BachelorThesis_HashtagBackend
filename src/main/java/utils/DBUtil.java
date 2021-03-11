package utils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * A Tool class which used to get the connection of DB
 * @author Yang Haoran
 */
public class DBUtil {
    private static Connection conn;
    private static ThreadLocal<Connection> threadLocal = new ThreadLocal<Connection>();
    private static String driver;
    private static String url;
    private static String username;
    private static String password;

    /**
     * Load the configuration file.
     */
    static {
        Properties props = new Properties();
        try {
            props.load(ClassLoader.getSystemClassLoader().getResourceAsStream("db.properties"));
            driver = props.getProperty("driver");
            url = props.getProperty("url");
            username = props.getProperty("username");
            password = props.getProperty("password");

            Class.forName(driver);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the connection which is bind to the client.
     * @return A datasource connection.
     */
    public static Connection getConnection() {
        try {
            if (conn == null) {
                threadLocal.set(DriverManager.getConnection(url, username, password));
            }
            conn = threadLocal.get();
        } catch (SQLException e) {
            System.out.println("Fail to get the connection!");
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * Close the bind connection.
     * @param resultSet The result of the sql query from the DB.
     * @param statement The sql statement.
     * @param conn The DB connection.
     */
    public static void close(ResultSet resultSet, Statement statement, Connection conn) {
        try {
            if (resultSet != null) resultSet.close();
            if (statement != null) statement.close();
            if (conn != null) {
                conn.setAutoCommit(true);
                threadLocal.remove();
                conn.close();
                conn = null;
            }
        } catch (SQLException e) {
            System.out.println("Fail to close the resources!");
            e.printStackTrace();
        }

    }

    /**
     * Start the transaction.
     * @throws SQLException
     */
    public static void startTx() throws SQLException {
        conn = getConnection();

        if (conn != null) {
            conn.setAutoCommit(false);
        }
    }

    /**
     * Commit the transaction.
     * @throws SQLException
     */
    public static void commitTx() throws SQLException {
        conn = getConnection();

        if (conn != null) {
            conn.commit();
        }
    }

    /**
     * Rollback the transaction
     * @throws SQLException
     */
    public static void rollbackTx() throws SQLException {
        conn = getConnection();

        if (conn != null) {
            conn.rollback();
        }
    }

    /**
     * Execute the general update sql statement.
     * @param sql The sql statement.
     * @param args The parameters of the statement.
     * @return The result of influenced rows.
     * @throws SQLException
     */
    public static int executeUpdate(String sql, Object... args) throws SQLException {
        conn = getConnection();
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(sql);
            for (int i = 0; i < args.length; i++) {
                ps.setObject(i + 1, args[i]);
            }
            return ps.executeUpdate();
        } finally {
            close(null, ps, null);
        }
    }

    /**
     * Execute the general query statement.
     * @param sql The sql statement.
     * @param rowMapper A mapper which is used to encapsulate the data from the DB.
     * @param args The parameters of the statement.
     * @param <T> The type of queried object.
     * @return The result of influenced rows.
     * @throws SQLException
     */
    public static <T> List<T> executeQuery(String sql, RowMapper<T> rowMapper, Object... args) throws SQLException {
        conn = getConnection();
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<T> res = new ArrayList<T>();
        try {
            ps = conn.prepareStatement(sql);
            for (int i = 0; i < args.length; i++) {
                ps.setObject(i + 1, args[i]);
            }
            rs = ps.executeQuery();
            while (rs.next()) {
                res.add(rowMapper.getRowMapper(rs));
            }
        } finally {
            close(rs, ps, null);
        }
        return res;
    }

}

