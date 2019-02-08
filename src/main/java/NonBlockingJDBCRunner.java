import com.oracle.adbaoverjdbc.DataSourceBuilder;
import jdk.incubator.sql2.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class NonBlockingJDBCRunner {
    private static final int totalInsertingRow = 10000;
    private String url = "jdbc:postgresql://localhost:5432/leizheng";
    private String user = "postgres";
    private String password = "postgres";

    public static void main(String[] args) throws SQLException {
        NonBlockingJDBCRunner service = new NonBlockingJDBCRunner();
        service.clearTestData();
        var currenttime = System.currentTimeMillis();
        service.insertTestData();
        System.out.println("total time:" + (System.currentTimeMillis() - currenttime) / 1000 + " seconds");
    }

    private void insertTestData() {
        // get the AoJ DataSourceFactory

        DataSourceFactory factory = DataSourceFactory.newFactory("com.oracle.adbaoverjdbc.DataSourceFactory");
        // get a DataSource and a Session
        try (DataSource ds = new DataSourceBuilder()
                .url(url)
                .username(user)
                .password(password)
                .build();
             Session conn = ds.getSession(t -> System.out.println("ERROR: " + t.getMessage()))) {

            // update CLARK to work in department 50

            for (int i = 0; i < totalInsertingRow; i++) {
                String SQL = "insert into testdata values ("+i+", '"+String.valueOf(i)+"')";
                conn.<Long>rowCountOperation(SQL)
                        .apply(c -> c.getCount())
                        .onError(t -> t.printStackTrace())
                        .submit();
            }
            conn.catchErrors();  // resume normal execution if there were any errors
        }
        // wait for the async tasks to complete before exiting
        ForkJoinPool.commonPool().awaitQuiescence(1, TimeUnit.MINUTES);
    }

    private void clearTestData() {
        // get a DataSource and a Session
        try (DataSource ds = new DataSourceBuilder()
                .url(url)
                .username(user)
                .password(password)
                .build();
             Session conn = ds.getSession(t -> System.out.println("ERROR: " + t.getMessage()))) {

            // update CLARK to work in department 50
            String SQL = "delete from testdata";
            conn.<Long>rowCountOperation(SQL)
                    .apply(c -> c.getCount())
                    .onError(t -> t.printStackTrace())
                    .submit();


            conn.catchErrors();  // resume normal execution if there were any errors
        }
        // wait for the async tasks to complete before exiting
        ForkJoinPool.commonPool().awaitQuiescence(1, TimeUnit.MINUTES);
    }
}
