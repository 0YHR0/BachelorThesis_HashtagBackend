package hashtag_analyze_main;


import Entity.Hashtag;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * This class is used to write the result to the mysql.
 * use the connection pool to get the connection,because several thread will connect to the mysql
 *
 */
public class MysqlSink extends RichSinkFunction<Hashtag> {
    private static ComboPooledDataSource ds = null;
    private static Connection conn = null;
    private static PreparedStatement ps = null;

    //get the connection pool
    static{
        try{
            ds = new ComboPooledDataSource("mysql");
        }catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }

    }

    /**
     * initialize the connection
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        conn = ds.getConnection();
        String sql = "Insert INTO hashtag_status (id,hashtag,location,count,count_daily,count_weekly,count_monthly,count_last_day,count_last_week,count_last_month) values (default,?,'unknown',?,1,1,1,0,0,0) ON DUPLICATE KEY UPDATE count=?,count_daily=?-count_last_day,count_monthly=?-count_last_month,count_weekly=?-count_last_week";
        ps = conn.prepareStatement(sql);
    }

    /**
     * close the connection and the prepared statement
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        ps.close();
        conn.close();
    }

    /**
     * sink the data
     * @param value the input value
     * @param context the context of the operator
     * @throws Exception
     */
    @Override
    public void invoke(Hashtag value, Context context) throws Exception {
        ps.setObject(1, value.getText());
        ps.setObject(2, value.getCount());
        ps.setObject(3, value.getCount());
        ps.setObject(4, value.getCount());
        ps.setObject(5, value.getCount());
        ps.setObject(6, value.getCount());
        int result = ps.executeUpdate();
        if(result == 0){
            throw new Exception("update mysql failed");
        }


    }
}
