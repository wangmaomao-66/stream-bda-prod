// UserProfileUtils.java
package com.stream.realtime.lululemon3;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONArray;

import java.sql.*;
import java.util.*;
import java.text.SimpleDateFormat;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.Period;

/**
 * 用户画像工具类 - 包含所有数据处理和计算方法
 */
public class UserProfileUtils {

    // 统计计数器
    private static int totalProcessed = 0;
    private static int sensitiveCount = 0;
    private static int p0Count = 0;
    private static int p1Count = 0;
    private static int p2Count = 0;

    // 数据库连接配置
    public static final String PG_URL = "jdbc:postgresql://127.0.0.1:5432/spider_db";
    public static final String PG_USERNAME = "postgres";
    public static final String PG_PASSWORD = "wyh123.0";

    // Doris连接配置
    public static final String DORIS_FENODES = "127.0.0.1:8030";
    public static final String DORIS_DATABASE = "realtime";
    public static final String DORIS_TABLE = "user_info_label";
    public static final String DORIS_USERNAME = "root";
    public static final String DORIS_PASSWORD = "";

    // 年龄段定义
    public static final String[] AGE_GROUPS = {"18-24", "25-29", "30-34", "35-39", "40-49", "50+"};

    // 动态计算的类目偏好系数
    private static Map<String, double[]> categoryPreference = new HashMap<>();
    // 动态计算的维度权重
    private static double[] dimensionWeights = new double[7];

    // 数据库连接状态
    private static boolean dorisConnected = false;
    private static boolean postgresqlDriverLoaded = false;

    /**
     * 初始化工具类
     */
    public static void initialize() {
        // 加载PostgreSQL驱动
        try {
            Class.forName("org.postgresql.Driver");
            postgresqlDriverLoaded = true;
            System.out.println("✅ PostgreSQL驱动加载成功");
        } catch (ClassNotFoundException e) {
            System.err.println("❌ PostgreSQL驱动加载失败: " + e.getMessage());
        }
    }

    /**
     * 初始化偏好系数和权重
     */
    public static void initializePreferenceCoefficients() {
        System.out.println("🔄 初始化偏好系数和权重...");
        setDefaultPreferenceCoefficients();
        dimensionWeights = new double[]{0.30, 0.20, 0.15, 0.10, 0.10, 0.10, 0.05};
        System.out.println("✅ 偏好系数和权重初始化完成");
    }

    /**
     * 设置默认偏好系数
     */
    private static void setDefaultPreferenceCoefficients() {
        categoryPreference.put("运动服饰", new double[]{0.8, 0.9, 0.7, 0.6, 0.4, 0.3});
        categoryPreference.put("家居用品", new double[]{0.2, 0.4, 0.6, 0.8, 0.9, 0.7});
        categoryPreference.put("健康食品", new double[]{0.1, 0.3, 0.5, 0.7, 0.8, 0.9});
        categoryPreference.put("潮流服饰", new double[]{0.9, 0.8, 0.6, 0.4, 0.2, 0.1});
        categoryPreference.put("商务服饰", new double[]{0.1, 0.4, 0.7, 0.8, 0.6, 0.5});
        categoryPreference.put("其他", new double[]{0.5, 0.5, 0.5, 0.5, 0.5, 0.5});
        System.out.println("⚠️ 使用默认偏好系数");
    }

    /**
     * 获取Doris连接属性
     */
    public static Properties getDorisConnectionProperties() {
        Properties properties = new Properties();
        properties.setProperty("format", "json");
        properties.setProperty("read_json_by_line", "true");
        properties.setProperty("strip_outer_array", "true");
        properties.setProperty("connect_timeout", "10000");
        properties.setProperty("socket_timeout", "60000");
        properties.setProperty("retries", "3");
        properties.setProperty("batch.size", "1024");
        properties.setProperty("batch.interval", "5000");
        properties.setProperty("doris.request.tablet.size", "1");
        properties.setProperty("doris.request.redirect.limit", "1");
        return properties;
    }

    /**
     * 转换为Doris JSON格式
     */
    public static String convertToDorisJsonFormat(DataClasses.UserProfileResult userProfileResult) {
        JSONObject dorisRecord = new JSONObject();

        String userId = userProfileResult.getUserID() != null ? userProfileResult.getUserID() : "unknown";
        DataClasses.UserCompleteInfo userInfo = userProfileResult.getUserCompleteInfo();

        // 主键字段
        dorisRecord.put("userid", userId);
        dorisRecord.put("ds", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        dorisRecord.put("ts", new Timestamp(System.currentTimeMillis()).toString());

        // 其他字段
        dorisRecord.put("username", userInfo.username != null ? userInfo.username : "用户_" + userId);
        dorisRecord.put("user_base_info", buildUserBaseInfo(userInfo).toJSONString());
        dorisRecord.put("login_time", buildLoginTimeArray(userInfo).toJSONString());
        dorisRecord.put("consumption_level", calculateConsumptionLevel(userInfo));
        dorisRecord.put("device_info", buildDeviceInfo(userInfo).toJSONString());
        dorisRecord.put("search_info", buildSearchInfo().toJSONString());
        dorisRecord.put("category_info", buildCategoryInfo().toJSONString());
        dorisRecord.put("shoping_gender", buildShoppingGender(userInfo).toJSONString());
        dorisRecord.put("is_check_sensitive_comment", !"正常".equals(userProfileResult.getCommentLevel()) ? "1" : "0");
        dorisRecord.put("sensitive_word", buildSensitiveWordArray(userProfileResult).toJSONString());
        dorisRecord.put("exact_age", userInfo.age > 0 ? userInfo.age : 0);

        String jsonResult = dorisRecord.toJSONString();
        System.out.println("📤 准备写入Doris JSON: " + jsonResult);
        return jsonResult;
    }

    // 辅助方法
    private static JSONObject buildUserBaseInfo(DataClasses.UserCompleteInfo userInfo) {
        JSONObject userBaseInfo = new JSONObject();
        userBaseInfo.put("birthday", userInfo.birthday != null ? userInfo.birthday : "");
        userBaseInfo.put("decade", userInfo.decade != null ? userInfo.decade : "");
        userBaseInfo.put("gender", userInfo.gender != null ? userInfo.gender : "");
        userBaseInfo.put("zodiac_sign", userInfo.zodiacSign != null ? userInfo.zodiacSign : "");
        userBaseInfo.put("weight", userInfo.weight != null ? userInfo.weight : "");
        userBaseInfo.put("height", userInfo.height != null ? userInfo.height : "");
        userBaseInfo.put("age", userInfo.age > 0 ? userInfo.age : 0);
        userBaseInfo.put("age_group", userInfo.ageGroup != null ? userInfo.ageGroup : "");
        userBaseInfo.put("exact_age", userInfo.age > 0 ? userInfo.age : 0);
        return userBaseInfo;
    }

    private static JSONArray buildLoginTimeArray(DataClasses.UserCompleteInfo userInfo) {
        JSONArray loginTimeArray = new JSONArray();
        if (userInfo.lastLoginTime != null) {
            loginTimeArray.add(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(userInfo.lastLoginTime));
        } else {
            loginTimeArray.add(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        }
        return loginTimeArray;
    }

    private static JSONObject buildDeviceInfo(DataClasses.UserCompleteInfo userInfo) {
        JSONObject deviceInfo = new JSONObject();
        deviceInfo.put("device_type", userInfo.deviceType != null ? userInfo.deviceType : "");
        deviceInfo.put("os", userInfo.os != null ? userInfo.os : "");
        deviceInfo.put("os_version", userInfo.osVersion != null ? userInfo.osVersion : "");
        deviceInfo.put("ip", userInfo.ip != null ? userInfo.ip : "");
        deviceInfo.put("network_type", userInfo.networkType != null ? userInfo.networkType : "");
        return deviceInfo;
    }

    private static JSONObject buildSearchInfo() {
        JSONObject searchInfo = new JSONObject();
        searchInfo.put("recent_searches", new JSONArray());
        return searchInfo;
    }

    private static JSONObject buildCategoryInfo() {
        JSONObject categoryInfo = new JSONObject();
        categoryInfo.put("preferred_categories", new JSONArray());
        return categoryInfo;
    }

    private static JSONObject buildShoppingGender(DataClasses.UserCompleteInfo userInfo) {
        JSONObject shoppingGender = new JSONObject();
        shoppingGender.put("gender", userInfo.gender != null ? userInfo.gender : "");
        shoppingGender.put("shoping_id", new JSONArray());
        return shoppingGender;
    }

    private static JSONArray buildSensitiveWordArray(DataClasses.UserProfileResult userProfileResult) {
        JSONArray sensitiveWordArray = new JSONArray();
        if (!"正常".equals(userProfileResult.getCommentLevel()) &&
                userProfileResult.getDetectedSensitiveWords() != null &&
                !userProfileResult.getDetectedSensitiveWords().isEmpty()) {

            JSONObject sensitiveRecord = new JSONObject();
            sensitiveRecord.put("trigger_time", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
            sensitiveRecord.put("trigger_word", userProfileResult.getDetectedSensitiveWords());
            sensitiveRecord.put("orderid", userProfileResult.getOrderID());
            sensitiveRecord.put("comment_level", userProfileResult.getCommentLevel());
            sensitiveWordArray.add(sensitiveRecord);
        }
        return sensitiveWordArray;
    }

    /**
     * 从PostgreSQL获取用户完整信息
     */
    public static DataClasses.UserCompleteInfo getUserCompleteInfoFromPG(String userId) {
        DataClasses.UserCompleteInfo userInfo = new DataClasses.UserCompleteInfo();

        if (userId == null || userId.isEmpty() || !postgresqlDriverLoaded) {
            System.out.println("⚠️ 无法获取用户信息: userId=" + userId + ", postgresqlDriverLoaded=" + postgresqlDriverLoaded);
            return userInfo;
        }

        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        int maxRetries = 3;
        int retryCount = 0;

        while (retryCount < maxRetries) {
            try {
                conn = DriverManager.getConnection(PG_URL, PG_USERNAME, PG_PASSWORD);
                System.out.println("✅ PostgreSQL连接成功，开始查询用户: " + userId);

                String baseSql = "SELECT uname, phone_num, birthday, gender, address FROM user_info_base WHERE user_id = ?";
                pstmt = conn.prepareStatement(baseSql);
                pstmt.setString(1, userId);
                rs = pstmt.executeQuery();

                if (rs.next()) {
                    userInfo.userId = userId;
                    userInfo.username = rs.getString("uname");
                    userInfo.phone = rs.getString("phone_num");
                    userInfo.birthday = rs.getString("birthday");

                    String genderValue = rs.getString("gender");
                    userInfo.gender = convertGender(genderValue);

                    userInfo.address = rs.getString("address");

                    // 计算年龄信息
                    if (userInfo.birthday != null && !userInfo.birthday.isEmpty()) {
                        try {
                            LocalDate birthDate = parseBirthday(userInfo.birthday);
                            if (birthDate != null) {
                                LocalDate now = LocalDate.now();
                                int age = Period.between(birthDate, now).getYears();
                                userInfo.age = age;
                                userInfo.ageGroup = calculateAgeGroup(age);
                                userInfo.decade = (birthDate.getYear() / 10) * 10 + "s";
                                userInfo.zodiacSign = calculateZodiacSign(birthDate.getMonthValue(), birthDate.getDayOfMonth());
                                System.out.println("🎂 用户 " + userId + " 年龄计算: 生日=" + birthDate + ", 准确年龄=" + age + ", 年龄段=" + userInfo.ageGroup);
                            }
                        } catch (Exception e) {
                            System.err.println("❌ 解析生日失败: " + userInfo.birthday);
                            userInfo.age = 0;
                        }
                    } else {
                        userInfo.age = 0;
                    }
                    break;
                }

                // 查询设备信息
                if (rs != null) {
                    rs.close();
                }
                if (pstmt != null) {
                    pstmt.close();
                }

                String deviceSql = "SELECT brand, plat, platv, softv, device, ip, net, ts FROM user_device_base WHERE userkey = ? ORDER BY ts DESC LIMIT 1";
                pstmt = conn.prepareStatement(deviceSql);
                pstmt.setString(1, userId);
                rs = pstmt.executeQuery();

                if (rs.next()) {
                    userInfo.deviceType = rs.getString("brand");
                    userInfo.os = rs.getString("plat");
                    userInfo.osVersion = rs.getString("platv");
                    userInfo.ip = rs.getString("ip");
                    userInfo.networkType = rs.getString("net");
                    userInfo.lastLoginTime = rs.getTimestamp("ts");
                }

                break;

            } catch (SQLException e) {
                retryCount++;
                System.err.println("❌ 从PostgreSQL获取用户信息失败 (尝试 " + retryCount + "/" + maxRetries + "): " + e.getMessage());
                if (retryCount >= maxRetries) {
                    System.err.println("❌ 重试次数用尽，无法连接到PostgreSQL数据库");
                } else {
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            } finally {
                try {
                    if (rs != null) {
                        rs.close();
                    }
                    if (pstmt != null) {
                        pstmt.close();
                    }
                    if (conn != null) {
                        conn.close();
                    }
                } catch (SQLException e) {
                    System.err.println("关闭数据库连接失败: " + e.getMessage());
                }
            }
        }

        return userInfo;
    }

    /**
     * 解析生日字符串为LocalDate
     */
    public static LocalDate parseBirthday(String birthday) {
        if (birthday == null || birthday.isEmpty()) {
            return null;
        }

        String trimmedBirthday = birthday.trim();
        System.out.println("🔍 解析生日: " + trimmedBirthday);

        try {
            if (trimmedBirthday.matches("\\d{4}-\\d{2}-\\d{2}")) {
                return LocalDate.parse(trimmedBirthday);
            } else if (trimmedBirthday.matches("\\d{4}/\\d{2}/\\d{2}")) {
                return LocalDate.parse(trimmedBirthday.replace('/', '-'));
            } else if (trimmedBirthday.matches("\\d{8}")) {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
                return LocalDate.parse(trimmedBirthday, formatter);
            } else if (trimmedBirthday.matches("\\d+")) {
                long timestamp = Long.parseLong(trimmedBirthday);
                if (timestamp > 1000000000000L) {
                    return new Date(timestamp).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
                } else {
                    return new Date(timestamp * 1000).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
                }
            } else if (trimmedBirthday.length() >= 10) {
                String datePart = trimmedBirthday.substring(0, 10);
                if (datePart.matches("\\d{4}-\\d{2}-\\d{2}")) {
                    return LocalDate.parse(datePart);
                } else if (datePart.matches("\\d{4}/\\d{2}/\\d{2}")) {
                    return LocalDate.parse(datePart.replace('/', '-'));
                }
            } else if (trimmedBirthday.matches("\\d{4}-\\d{2}-\\d{2}.*")) {
                String datePart = trimmedBirthday.substring(0, 10);
                return LocalDate.parse(datePart);
            }
        } catch (Exception e) {
            System.err.println("❌ 解析生日格式失败: " + trimmedBirthday);
        }

        System.err.println("❌ 无法识别的生日格式: " + trimmedBirthday);
        return null;
    }

    /**
     * 性别字段自动识别转换
     */
    public static String convertGender(String genderValue) {
        if (genderValue == null || genderValue.trim().isEmpty()) {
            return "未知";
        }

        String gender = genderValue.trim().toLowerCase();

        if (gender.equals("1")) {
            return "男";
        }
        if (gender.equals("0")) {
            return "女";
        }
        if (gender.equals("2")) {
            return "其他";
        }
        if (gender.equals("男") || gender.equals("男性")) {
            return "男";
        }
        if (gender.equals("女") || gender.equals("女性")) {
            return "女";
        }
        if (gender.equals("其他") || gender.equals("未知")) {
            return "其他";
        }
        if (gender.equals("male") || gender.equals("m")) {
            return "男";
        }
        if (gender.equals("female") || gender.equals("f")) {
            return "女";
        }
        if (gender.equals("other") || gender.equals("o")) {
            return "其他";
        }
        if (gender.contains("先生") || gender.contains("男士")) {
            return "男";
        }
        if (gender.contains("女士") || gender.contains("小姐")) {
            return "女";
        }
        if (gender.contains("男") || gender.contains("male") || gender.contains("m")) {
            return "男";
        } else if (gender.contains("女") || gender.contains("female") || gender.contains("f")) {
            return "女";
        } else {
            return "未知";
        }
    }

    /**
     * 计算年龄段
     */
    public static String calculateAgeGroup(int age) {
        if (age >= 18 && age <= 24) {
            return "18-24";
        } else if (age >= 25 && age <= 29) {
            return "25-29";
        } else if (age >= 30 && age <= 34) {
            return "30-34";
        } else if (age >= 35 && age <= 39) {
            return "35-39";
        } else if (age >= 40 && age <= 49) {
            return "40-49";
        } else if (age >= 50) {
            return "50+";
        } else {
            return "未知";
        }
    }

    /**
     * 计算星座
     */
    public static String calculateZodiacSign(int month, int day) {
        if ((month == 3 && day >= 21) || (month == 4 && day <= 19)) {
            return "白羊座";
        } else if ((month == 4 && day >= 20) || (month == 5 && day <= 20)) {
            return "金牛座";
        } else if ((month == 5 && day >= 21) || (month == 6 && day <= 21)) {
            return "双子座";
        } else if ((month == 6 && day >= 22) || (month == 7 && day <= 22)) {
            return "巨蟹座";
        } else if ((month == 7 && day >= 23) || (month == 8 && day <= 22)) {
            return "狮子座";
        } else if ((month == 8 && day >= 23) || (month == 9 && day <= 22)) {
            return "处女座";
        } else if ((month == 9 && day >= 23) || (month == 10 && day <= 23)) {
            return "天秤座";
        } else if ((month == 10 && day >= 24) || (month == 11 && day <= 22)) {
            return "天蝎座";
        } else if ((month == 11 && day >= 23) || (month == 12 && day <= 21)) {
            return "射手座";
        } else if ((month == 12 && day >= 22) || (month == 1 && day <= 19)) {
            return "摩羯座";
        } else if ((month == 1 && day >= 20) || (month == 2 && day <= 18)) {
            return "水瓶座";
        } else if ((month == 2 && day >= 19) || (month == 3 && day <= 20)) {
            return "双鱼座";
        } else {
            return "未知";
        }
    }

    /**
     * 基于多维度计算年龄标签
     */
    public static String calculateAgeLabel(DataClasses.UserCompleteInfo userInfo) {
        if (userInfo.ageGroup != null && !userInfo.ageGroup.equals("未知")) {
            return userInfo.ageGroup;
        }

        double[] ageScores = new double[AGE_GROUPS.length];
        double[] categoryScores = calculateCategoryPreferenceScore(userInfo);
        double[] deviceScores = calculateDevicePreferenceScore(userInfo);
        double[] timeScores = calculateTimeBehaviorScore(userInfo);

        for (int i = 0; i < ageScores.length; i++) {
            ageScores[i] += categoryScores[i] * dimensionWeights[0];
            ageScores[i] += deviceScores[i] * dimensionWeights[6];
            ageScores[i] += timeScores[i] * dimensionWeights[3];
        }

        int maxIndex = 0;
        for (int i = 1; i < ageScores.length; i++) {
            if (ageScores[i] > ageScores[maxIndex]) {
                maxIndex = i;
            }
        }

        return AGE_GROUPS[maxIndex];
    }

    private static double[] calculateCategoryPreferenceScore(DataClasses.UserCompleteInfo userInfo) {
        double[] scores = new double[AGE_GROUPS.length];
        Arrays.fill(scores, 0.5);
        return scores;
    }

    private static double[] calculateDevicePreferenceScore(DataClasses.UserCompleteInfo userInfo) {
        double[] scores = new double[AGE_GROUPS.length];
        if (userInfo.deviceType != null) {
            if (userInfo.deviceType.toLowerCase().contains("iphone") || userInfo.deviceType.toLowerCase().contains("apple")) {
                scores[0] = 0.8;
                scores[1] = 0.7;
                scores[2] = 0.6;
                scores[3] = 0.4;
                scores[4] = 0.3;
                scores[5] = 0.2;
            } else if (userInfo.deviceType.toLowerCase().contains("android")) {
                Arrays.fill(scores, 0.5);
            } else {
                Arrays.fill(scores, 0.5);
            }
        } else {
            Arrays.fill(scores, 0.5);
        }
        return scores;
    }

    private static double[] calculateTimeBehaviorScore(DataClasses.UserCompleteInfo userInfo) {
        double[] scores = new double[AGE_GROUPS.length];
        if (userInfo.lastLoginTime != null) {
            int hour = userInfo.lastLoginTime.toLocalDateTime().getHour();
            if (hour >= 22 || hour <= 2) {
                scores[0] = 0.8;
                scores[1] = 0.6;
                scores[2] = 0.4;
                scores[3] = 0.2;
                scores[4] = 0.1;
                scores[5] = 0.1;
            } else if (hour >= 9 && hour <= 18) {
                scores[0] = 0.2;
                scores[1] = 0.4;
                scores[2] = 0.6;
                scores[3] = 0.7;
                scores[4] = 0.6;
                scores[5] = 0.4;
            } else {
                Arrays.fill(scores, 0.5);
            }
        } else {
            Arrays.fill(scores, 0.5);
        }
        return scores;
    }

    /**
     * 计算消费水平
     */
    public static String calculateConsumptionLevel(DataClasses.UserCompleteInfo userInfo) {
        if (userInfo.deviceType != null &&
                (userInfo.deviceType.toLowerCase().contains("iphone") ||
                        userInfo.deviceType.toLowerCase().contains("apple"))) {
            return "high";
        } else {
            return "media";
        }
    }

    /**
     * 将用户画像结果转换为控制台输出格式
     */
    public static String convertToConsoleFormat(DataClasses.UserProfileResult userProfileResult) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n📋 用户画像分析结果:\n");
        sb.append("├── 用户ID: ").append(userProfileResult.getUserID()).append("\n");
        sb.append("├── 订单ID: ").append(userProfileResult.getOrderID()).append("\n");
        sb.append("├── 商品名称: ").append(userProfileResult.getProductName()).append("\n");
        sb.append("├── 评论等级: ").append(getColorfulLevel(userProfileResult.getCommentLevel())).append("\n");
        sb.append("├── 是否黑名单: ").append(userProfileResult.getIsBlack() ? "✅ 是" : "❌ 否").append("\n");
        sb.append("├── 黑名单天数: ").append(userProfileResult.getBlackDays()).append("\n");

        DataClasses.UserCompleteInfo userInfo = userProfileResult.getUserCompleteInfo();
        if (userInfo != null) {
            sb.append("├── 准确年龄: ").append(userInfo.age).append("岁\n");
            sb.append("├── 年龄段: ").append(userInfo.ageGroup).append("\n");
            sb.append("├── 性别: ").append(userInfo.gender).append("\n");
            sb.append("├── 星座: ").append(userInfo.zodiacSign).append("\n");
            sb.append("├── 年代: ").append(userInfo.decade).append("\n");
            sb.append("├── 设备类型: ").append(userInfo.deviceType != null ? userInfo.deviceType : "未知").append("\n");
        }

        sb.append("├── 检测到的敏感词: ").append(userProfileResult.getDetectedSensitiveWords()).append("\n");
        sb.append("├── 原始评论: ").append(userProfileResult.getComment()).append("\n");
        sb.append("└── 标记评论: ").append(userProfileResult.getMarkedComment()).append("\n");

        return sb.toString();
    }

    /**
     * 获取带颜色的等级显示
     */
    private static String getColorfulLevel(String level) {
        switch (level) {
            case "P0":
                return "🔴P0🔴";
            case "P1":
                return "🟡P1🟡";
            case "P2":
                return "🔵P2🔵";
            default:
                return "✅正常✅";
        }
    }

    /**
     * 生成处理统计报告
     */
    public static String generateStatisticsReport() {
        return String.format(
                "\n📈 处理统计报告:\n" +
                        "├── 总处理条数: %d\n" +
                        "├── 敏感评论数: %d\n" +
                        "├── P0级别: %d\n" +
                        "├── P1级别: %d\n" +
                        "├── P2级别: %d\n" +
                        "└── Doris连接: %s\n",
                getTotalProcessed(),
                getSensitiveCount(),
                getP0Count(),
                getP1Count(),
                getP2Count(),
                isDorisConnected() ? "✅ 已连接" : "❌ 未连接"
        );
    }

    // 统计方法
    public static void incrementTotalProcessed() {
        totalProcessed++;
    }

    public static void incrementSensitiveCount() {
        sensitiveCount++;
    }

    public static void incrementP0Count() {
        p0Count++;
    }

    public static void incrementP1Count() {
        p1Count++;
    }

    public static void incrementP2Count() {
        p2Count++;
    }

    public static int getTotalProcessed() {
        return totalProcessed;
    }

    public static int getSensitiveCount() {
        return sensitiveCount;
    }

    public static int getP0Count() {
        return p0Count;
    }

    public static int getP1Count() {
        return p1Count;
    }

    public static int getP2Count() {
        return p2Count;
    }

    public static boolean isDorisConnected() {
        return dorisConnected;
    }

    public static void setDorisConnected(boolean connected) {
        dorisConnected = connected;
    }

    public static boolean isPostgresqlDriverLoaded() {
        return postgresqlDriverLoaded;
    }
}