package com.yangtzelsl;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @Author: luis.liu
 * @Date: 2019/8/1 14:39
 * @ref: https://prestodb.io/docs/current/installation/jdbc.html#
 */
public class PrestoJdbcDriver {

    public static void main(String[] args) throws SQLException {

        // jdbc:presto://host:port
        // jdbc:presto://host:port/catalog
        // jdbc:presto://host:port/catalog/schema

        String url = "jdbc:presto://example.net:8080/hive/sales";
        Connection connection1 = DriverManager.getConnection(url, "test", null);

        // URL parameters
        String urlPara = "jdbc:presto://example.net:8080/hive/sales";
        Properties properties = new Properties();
        properties.setProperty("user", "test");
        properties.setProperty("password", "secret");
        properties.setProperty("SSL", "true");
        Connection connection2 = DriverManager.getConnection(urlPara, properties);

        // properties
        String urlProp = "jdbc:presto://example.net:8080/hive/sales?user=test&password=secret&SSL=true";
        Connection connection3 = DriverManager.getConnection(urlProp);
    }

}
