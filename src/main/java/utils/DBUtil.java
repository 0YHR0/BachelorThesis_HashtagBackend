package utils;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * A Tool class which used to get the connection of DB
 * @author Yang Haoran
 */
public class DBUtil {
    private static ComboPooledDataSource ds =null;
    private static Connection conn;

    /**
     * Load the configuration file.
     */

    //get the connection pool
    static{

        try{
            ds = new ComboPooledDataSource("mysql");//use the mysql config to create the database
        }catch (Exception e) {

            throw new ExceptionInInitializerError(e);

        }

    }

    /**
     * Get the connection which is bind to the client.
     * @return A datasource connection.
     */
    public static Connection getConnection() throws SQLException {

        return ds.getConnection();
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
//                threadLocal.remove();
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

