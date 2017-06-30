package ru.krupt.ossr;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@Slf4j
public class ClosingActiveOracleSession {

    public static void main(String[] args) throws SQLException, InterruptedException {
//        final Connection connection = DriverManager.getConnection("jdbc:oracle:thin:@//localhost:1567/ora12", "eissd", "eissd_local");
        final Connection connection = DriverManager.getConnection("jdbc:oracle:thin:@//10.184.55.10:1521/eissdtst.ural.rt.ru",
                "eissd_dev", "eissd_dev");
        final Statement statement = connection.createStatement();
        new Thread(() -> {
            try {
                /*statement.execute("BEGIN\n"
                        + "  LOOP\n"
                        + "    dbms_lock.sleep(5);\n"
                        + "  END LOOP;\n"
                        + "END;");*/
                final ResultSet resultSet = statement.executeQuery("SELECT * FROM tr_request WHERE worker_create = 21332112");
                while (resultSet.next()) {
                    // Ignore
                }
            } catch (SQLException e) {
                throw new RuntimeException("Ошибка выполнения PL/SQL блока", e);
            }
        }).start();
        log.info("[main] will sleep");
        Thread.sleep(10000);
        log.info("[main] awoke");
        statement.cancel();
        log.info("statement cancelled");
        statement.close();
        log.info("statement closed");
//        log.info("[main] aborting connection started");
//        connection.abort(Executors.newSingleThreadExecutor());
        connection.close();
        log.info("[main] connection closed");
    }
}
