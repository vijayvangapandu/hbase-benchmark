package benchmark.hbase.util;

import java.sql.Connection;
import java.sql.DriverManager;

public class PhoenixConnectionManager {

    public static Connection getConnection() throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        //conn =  DriverManager.getConnection("jdbc:phoenix:10.9.60.24:2181:/hbase-unsecure");
       return  DriverManager.getConnection("jdbc:phoenix:hdp2-zk-1.prod.dc1.eharmony.com:2181:/hbase-unsecure");
    }
}
