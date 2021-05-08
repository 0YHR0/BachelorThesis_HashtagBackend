package hashtag_analyze_main;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This class is used to set some scheduled task.
 * @author Yang Haoran
 */
public class Timer {
    /**
     * record the hashtag count weekly and monthly in the temp
     * @param ds the connection pool to get the connection
     */
    public static void setTimer(ComboPooledDataSource ds){
        ScheduledExecutorService service = Executors.newScheduledThreadPool(3);
        service.scheduleAtFixedRate(new Runnable() {
            /**
             * record the hashtag count daily in the temp
             */
            @Override
            public void run() {

                try {
                    Connection conn = ds.getConnection();
                    String sql = "UPDATE hashtag_status set count_last_day=count";
                    PreparedStatement ps = conn.prepareStatement(sql);
                    System.out.println("-------------------day--------------------------");
                    int result = ps.executeUpdate();
                    if(result == 0){
                        throw new Exception("update mysql failed");
                    }
                    conn.close();

                } catch (Exception throwables) {
                    throwables.printStackTrace();
                }



            }
        },10,10, TimeUnit.SECONDS);

        service.scheduleAtFixedRate(new Runnable() {
            /**
             * record the hashtag count weekly in the temp
             */
            @Override
            public void run() {

                try {
                    Connection conn = ds.getConnection();
                    String sql = "UPDATE hashtag_status set count_last_week=count";
                    PreparedStatement ps = conn.prepareStatement(sql);
                    System.out.println("-------------------week--------------------------");
                    int result = ps.executeUpdate();
                    if(result == 0){
                        throw new Exception("update mysql failed");
                    }
                    conn.close();

                } catch (Exception throwables) {
                    throwables.printStackTrace();
                }


            }
        },1,1, TimeUnit.MINUTES);
        service.scheduleAtFixedRate(new Runnable() {
            /**
             * record the hashtag count monthly in the temp
             */
            @Override
            public void run() {

                try {
                    Connection conn = ds.getConnection();
                    String sql = "UPDATE hashtag_status set count_last_month=count";
                    PreparedStatement ps = conn.prepareStatement(sql);
                    System.out.println("-------------------month--------------------------");
                    int result = ps.executeUpdate();
                    if(result == 0){
                        throw new Exception("update mysql failed");
                    }
                    conn.close();

                } catch (Exception throwables) {
                    throwables.printStackTrace();
                }


            }
        },1,10, TimeUnit.MINUTES);


    }
}
