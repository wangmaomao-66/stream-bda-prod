// DorisTableChecker.java
package com.stream.realtime.lululemon3;

import java.sql.*;

public class DorisTableChecker {

    public static void main(String[] args) {
        checkTableAndCreateIfNeeded();
    }

    public static void checkTableAndCreateIfNeeded() {
        String url = "jdbc:mysql://127.0.0.1:9030/realtime";
        String username = "root";
        String password = "";

        try (Connection conn = DriverManager.getConnection(url, username, password);
             Statement stmt = conn.createStatement()) {

            System.out.println("✅ 成功连接到Doris数据库");

            // 检查表是否存在
            ResultSet rs = stmt.executeQuery("SHOW TABLES LIKE 'user_info_label'");
            if (rs.next()) {
                System.out.println("✅ user_info_label表已存在");

                // 查看表结构
                System.out.println("📋 表结构:");
                ResultSet descRs = stmt.executeQuery("DESC user_info_label");
                while (descRs.next()) {
                    System.out.println("  " + descRs.getString(1) + " | " + descRs.getString(2) + " | " + descRs.getString(3));
                }
                descRs.close();

            } else {
                System.out.println("❌ user_info_label表不存在，正在创建...");

                // 创建表
                String createTableSQL = "CREATE TABLE user_info_label (\n" +
                        "    userid VARCHAR(100),\n" +
                        "    ds DATE, \n" +
                        "    ts DATETIME,\n" +
                        "    username VARCHAR(200),\n" +
                        "    user_base_info TEXT,\n" +
                        "    login_time TEXT,\n" +
                        "    consumption_level VARCHAR(20),\n" +
                        "    device_info TEXT,\n" +
                        "    search_info TEXT,\n" +
                        "    category_info TEXT,\n" +
                        "    shoping_gender TEXT,\n" +
                        "    is_check_sensitive_comment VARCHAR(5),\n" +
                        "    sensitive_word TEXT,\n" +
                        "    exact_age INT\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(userid, ds)\n" +
                        "DISTRIBUTED BY HASH(userid) BUCKETS 10\n" +
                        "PROPERTIES (\n" +
                        "    \"replication_num\" = \"1\"\n" +
                        ")";

                stmt.execute(createTableSQL);
                System.out.println("✅ user_info_label表创建成功");
            }

            // 检查是否有exact_age字段
            boolean hasExactAge = false;
            ResultSet descRs = stmt.executeQuery("DESC user_info_label");
            while (descRs.next()) {
                if ("exact_age".equals(descRs.getString(1))) {
                    hasExactAge = true;
                    break;
                }
            }
            descRs.close();

            if (!hasExactAge) {
                System.out.println("❌ 缺少exact_age字段，正在添加...");
                stmt.execute("ALTER TABLE user_info_label ADD COLUMN exact_age INT NULL AFTER sensitive_word");
                System.out.println("✅ exact_age字段添加成功");
            }

        } catch (Exception e) {
            System.err.println("❌ 检查表失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
}