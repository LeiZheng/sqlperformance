import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class BlockingJDBCRunner {
    public static final int totalInsertingRow = 10000;
    private String url = "jdbc:postgresql://localhost:5432/leizheng";
    private String user = "postgres";
    private String password = "postgres";

    public static void main(String[] args) throws SQLException {
        BlockingJDBCRunner service = new BlockingJDBCRunner();
        service.clearTestData();
        var currenttime = System.currentTimeMillis();
        final int threadCount = 1;
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        List<Future> futures = new ArrayList();
        for (int i = 0; i < threadCount; i++) {
            final int startIndex = i * totalInsertingRow / threadCount;
            futures.add(executorService.submit(() ->{
                for (int index = startIndex; index < startIndex + totalInsertingRow / threadCount; index ++) {
                    service.insertTestData(index);
                }
                return true;
            }));
        }

        futures.stream().forEach(x -> {
            try {
                x.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        });
        System.out.println("total time:" + (System.currentTimeMillis() - currenttime) / 1000 + " seconds");
        executorService.shutdown();
    }

    private void clearTestData() {
        String SQL = "delete from testdata";

        try (Connection conn = connect()) {
            PreparedStatement pstmt = conn.prepareStatement(SQL);
            pstmt.executeUpdate();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }

    }

    public void insertTestData(int id) throws SQLException {

        String SQL = "insert into testdata values (?, ?)";


        try (Connection conn = connect()) {
            PreparedStatement pstmt = conn.prepareStatement(SQL);

            pstmt.setInt(1, id);
            pstmt.setString(2, String.valueOf(id));

            pstmt.executeUpdate();

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }

    }

    public Connection connect() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }
}
