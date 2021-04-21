package com.yangtzelsl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @Description: TrinoDemo
 * @Author luis.liu
 * @Date: 2021/4/21 18:12
 * @Version 1.0
 */
public class TrinoDemo {
    public static void main(String[] args) throws Exception {
        Class.forName("io.prestosql.jdbc.PrestoDriver");
        Connection connection = DriverManager.getConnection("jdbc:presto://x.x.x.x:8080/hive/default", "root", "");
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("show catalogs");
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
        rs.close();
        connection.close();
    }
}
