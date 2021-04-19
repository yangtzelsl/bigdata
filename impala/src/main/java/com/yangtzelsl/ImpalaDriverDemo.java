package com.yangtzelsl;

import com.yangtzelsl.commons.conf.ConfigurationManagerJava;
import com.yangtzelsl.commons.constant.ConstantsJava;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.sql.*;

/**
 * @Description: ImpalaDriverDemo
 * @Author luis.liu
 * @Date: 2021/4/19 20:38
 * @Version 1.0
 */
public class ImpalaDriverDemo {
    static String JDBC_DRIVER = "com.cloudera.impala.jdbc41.Driver";
    // ;AuthMech=3;UID=hive;PWD=;UseSasl=0
    static String CONNECTION_URL = "jdbc:impala://x.x.x.x:21050/amber_amp_dwd";
    static String USER_NAME = "";
    static String PASS_WORD = "";

    public static void main(String[] args) throws SQLException, ConfigurationException {

        String driverName = ConfigurationManagerJava.getPropConfig().getString(ConstantsJava.IMPALA_JDBC_DRIVERNAME_JAVA);
        String jdbcUrl = ConfigurationManagerJava.getPropConfig().getString(ConstantsJava.IMPALA_JDBC_URL_JAVA);
        String jdbcUser = ConfigurationManagerJava.getPropConfig().getString(ConstantsJava.IMPALA_JDBC_URL_JAVA);
        String jdbcPassword = ConfigurationManagerJava.getPropConfig().getString(ConstantsJava.IMPALA_JDBC_PASSWORD_JAVA);

        Connection con = null;
        ResultSet rs = null;
        PreparedStatement ps = null;

        try {
            Class.forName(JDBC_DRIVER);
            con = DriverManager.getConnection(CONNECTION_URL, USER_NAME, PASS_WORD);
            ps = con.prepareStatement("select created_user_id from amber_amp_dwd.dwd_amp_client_entity limit 1");
            rs = ps.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getString(1) + '\t');
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //关闭rs、ps和con
            // 关闭资源
            if (rs != null) {
                try {
                    rs.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (con != null) {
                try {
                    con.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }
    }
}
