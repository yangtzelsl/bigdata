package com.yangtzelsl;

import java.sql.*;

/**
 * @Description: ImpalaHiveDriverDemo
 * @Author luis.liu
 * @Date: 2021/4/19 20:37
 * @Version 1.0
 */
public class ImpalaHiveDriverDemo {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        long currentTimeMillis = System.currentTimeMillis();
        count();
        System.out.println("耗时:" + (System.currentTimeMillis() - currentTimeMillis));
    }

    public static Connection getConnection() throws ClassNotFoundException, SQLException {
        String driver = "org.apache.hive.jdbc.HiveDriver";
        String JDBCUrl = "jdbc:hive2://x.x.x.x:21050/;auth=noSasl";
        String username = "";
        String password = "";

        Connection conn = null;
        Class.forName(driver);
        conn = (Connection) DriverManager.getConnection(JDBCUrl, username, password);
        return conn;
    }

    public static void count() throws ClassNotFoundException, SQLException {
        Connection conn = getConnection();
        String sql = "select created_user_id from amber_amp_dwd.dwd_amp_client_entity limit 1 ;";
        System.out.println("查询语句：" + sql);
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        int columnCount = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(rs.getString(i) + "\t");
            }
            System.out.println("");
        }
    }
}
