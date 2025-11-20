// FlinkProcessors.java
package com.stream.realtime.lululemon3;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Flink数据处理类 - 包含所有Flink算子处理逻辑
 */
public class FlinkProcessors {

    // CommentDataParser 类
    public static class CommentDataParser extends RichFlatMapFunction<String, DataClasses.CommentData> {
        @Override
        public void flatMap(String value, Collector<DataClasses.CommentData> out) throws Exception {
            try {
                JSONObject jsonNode = JSON.parseObject(value);
                String op = jsonNode.getString("op");

                if ("r".equals(op) || "c".equals(op) || "u".equals(op)) {
                    JSONObject after = jsonNode.getJSONObject("after");

                    if (after != null) {
                        DataClasses.CommentData commentData = new DataClasses.CommentData();
                        commentData.setId(after.getString("ID"));
                        commentData.setOrderID(after.getString("OrderID"));
                        commentData.setUserID(after.getString("UserID"));
                        commentData.setProductID(after.getString("ProductID"));
                        commentData.setProductName(after.getString("ProductName"));

                        String commentTextJson = after.getString("CommentText");
                        String commentText = "";
                        String productNameFromComment = "";

                        if (commentTextJson != null && !commentTextJson.isEmpty()) {
                            try {
                                JSONObject commentJson = JSON.parseObject(commentTextJson);
                                commentText = commentJson.getString("comment");
                                productNameFromComment = commentJson.getString("product");

                                if (commentText == null) {
                                    commentText = commentTextJson;
                                }
                            } catch (Exception e) {
                                commentText = commentTextJson;
                            }
                        }
                        commentData.setComment(commentText);
                        commentData.setProductNameFromComment(productNameFromComment);

                        String existingSensitiveWords = after.getString("SensitiveWords");
                        commentData.setExistingSensitiveWords(existingSensitiveWords);

                        String createdDate = after.getString("CreatedDate");
                        if (createdDate != null) {
                            try {
                                if (createdDate.matches("\\d+")) {
                                    long timestamp = Long.parseLong(createdDate);
                                    commentData.setCreatedDate(formatTimestamp(timestamp));
                                } else {
                                    commentData.setCreatedDate(createdDate);
                                }
                            } catch (Exception e) {
                                commentData.setCreatedDate("日期解析失败");
                            }
                        } else {
                            commentData.setCreatedDate("无日期");
                        }

                        commentData.setOperationType(op);
                        out.collect(commentData);
                    }
                }
            } catch (Exception e) {
                System.err.println("解析评论数据失败: " + e.getMessage());
            }
        }

        private String formatTimestamp(long timestamp) {
            try {
                if (timestamp > 1000000000000L) {
                    return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(timestamp));
                } else {
                    return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(timestamp * 1000));
                }
            } catch (Exception e) {
                return String.valueOf(timestamp);
            }
        }
    }

    // UserProfileProcessor 类
    public static class UserProfileProcessor extends RichFlatMapFunction<DataClasses.CommentData, DataClasses.UserProfileResult> {

        private Set<String> p0Keywords;
        private Set<String> p1Keywords;
        private Set<String> p2Keywords;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            // P0: 政治敏感、色情、反动、毒品等严重违规内容
            p0Keywords = new HashSet<>(Arrays.asList(
                    "党", "政府", "国家", "领导", "主席", "总理", "总统", "政策", "政治", "政权",
                    "色情", "淫秽", "成人", "情色", "黄色", "猥亵", "性爱", "做爱", "av", "毛片",
                    "反动", "颠覆", "暴乱", "叛乱", "分裂", "独立", "暴动",
                    "毒品", "吸毒", "贩毒", "大麻", "冰毒", "海洛因", "可卡因", "摇头丸",
                    "来复枪", "警察腐败", "腐败", "开发票"
            ));

            // P1: 脏话，对线语句，地域歧视，地狱笑话
            p1Keywords = new HashSet<>(Arrays.asList(
                    "操", "妈", "爹", "奶", "逼", "屌", "鸡", "鸭", "死", "杀", "傻逼", "傻b", "sb",
                    "垃圾", "废物", "混蛋", "他妈", "你妈", "操你", "去死", "滚蛋", "闭嘴", "神经病",
                    "脑残", "智障", "白痴", "弱智", "贱人", "婊子", "王八蛋", "狗日的", "龟儿子",
                    "你算什么东西", "你配吗", "你也配", "不服来战", "有本事", "敢不敢",
                    "河南人", "东北人", "上海人", "北京人", "外地人", "乡下人", "农村人", "穷鬼",
                    "地域黑", "地方歧视", "地域攻击", "地图炮",
                    "地狱笑话", "黑色幽默", "悲剧玩笑", "死人笑话", "灾难玩笑"
            ));

            // P2: 一般负面评论
            p2Keywords = new HashSet<>(Arrays.asList(
                    "不好", "差评", "垃圾", "骗人", "上当", "后悔", "别买", "不推荐", "坑爹",
                    "浪费钱", "质量差", "服务差", "态度差", "慢死了", "等半天", "再也不买"
            ));

            System.out.println("✅ 开始处理评论数据和用户画像计算");
        }

        @Override
        public void flatMap(DataClasses.CommentData commentData, Collector<DataClasses.UserProfileResult> out) throws Exception {
            String existingSensitiveWords = commentData.getExistingSensitiveWords();
            String comment = commentData.getComment();

            if (comment == null || comment.isEmpty()) {
                return;
            }

            UserProfileUtils.incrementTotalProcessed();

            // 获取用户完整信息
            DataClasses.UserCompleteInfo userInfo = UserProfileUtils.getUserCompleteInfoFromPG(commentData.getUserID());

            // 计算用户画像标签
            String ageLabel = UserProfileUtils.calculateAgeLabel(userInfo);
            String genderLabel = userInfo.gender != null ? userInfo.gender : "未知";
            String zodiacSign = userInfo.zodiacSign != null ? userInfo.zodiacSign : "未知";
            String decade = userInfo.decade != null ? userInfo.decade : "未知";

            // 敏感词检测
            String commentLevel = "正常";
            boolean isBlack = false;
            int blackDays = 0;
            String detectedWordsStr = "";
            List<String> detectedWords = new ArrayList<>();

            // 如果SQL Server表中的SensitiveWords字段不为空，直接定为P0级别
            if (existingSensitiveWords != null && !existingSensitiveWords.trim().isEmpty()) {
                commentLevel = "P0";
                detectedWords.add("P0:数据库标记敏感词");
                detectedWordsStr = existingSensitiveWords;
                System.out.println("🚨 P0级别敏感评论检测: 数据库标记敏感词 - " + existingSensitiveWords);
            } else {
                // 正常的内容分析
                String contentAnalysis = analyzeCommentContent(comment, detectedWords);
                commentLevel = contentAnalysis;
                detectedWordsStr = String.join(", ", detectedWords);
            }

            // 根据分级结果设置黑名单状态
            switch (commentLevel) {
                case "P0":
                    isBlack = true;
                    blackDays = 365;
                    UserProfileUtils.incrementP0Count();
                    UserProfileUtils.incrementSensitiveCount();
                    break;
                case "P1":
                    isBlack = true;
                    blackDays = 60;
                    UserProfileUtils.incrementP1Count();
                    UserProfileUtils.incrementSensitiveCount();
                    break;
                case "P2":
                    isBlack = false;
                    blackDays = 0;
                    UserProfileUtils.incrementP2Count();
                    UserProfileUtils.incrementSensitiveCount();
                    break;
                default:
                    break;
            }

            String markedComment = markSensitiveWords(comment, detectedWordsStr, commentLevel);

            DataClasses.UserProfileResult result = new DataClasses.UserProfileResult();
            result.setId(commentData.getId());
            result.setUserID(commentData.getUserID());
            result.setOrderID(commentData.getOrderID());
            result.setProductID(commentData.getProductID());
            result.setProductName(commentData.getProductName());
            result.setComment(comment);
            result.setMarkedComment(markedComment);
            result.setCommentLevel(commentLevel);
            result.setIsBlack(isBlack);
            result.setBlackDays(blackDays);
            result.setDetectedSensitiveWords(detectedWordsStr);
            result.setExistingSensitiveWords(commentData.getExistingSensitiveWords());
            result.setOperationType(commentData.getOperationType());
            result.setProcessTime(new Date());
            result.setCreatedDate(commentData.getCreatedDate());
            result.setUserCompleteInfo(userInfo);
            result.setAgeLabel(ageLabel);
            result.setGenderLabel(genderLabel);
            result.setZodiacSign(zodiacSign);
            result.setDecade(decade);

            out.collect(result);

            // 输出日志 - 增强日志，包含准确年龄
            if (!"正常".equals(commentLevel)) {
                System.out.println("🚨 " + getColorfulLevel(commentLevel) + " 检测到！用户: " + commentData.getUserID());
                System.out.println("📊 用户画像: 准确年龄" + userInfo.age + "岁, 年龄段" + ageLabel + " 性别" + genderLabel + " 星座" + zodiacSign);
                System.out.println("💬 评论内容: " + (comment.length() > 50 ? comment.substring(0, 50) + "..." : comment));
                System.out.println("🔍 检测到的敏感词: " + detectedWordsStr);
            } else {
                System.out.println("✅ 正常评论 - 用户: " + commentData.getUserID() + ", 准确年龄: " + userInfo.age + "岁");
            }

            // 增强控制台输出
            System.out.println(UserProfileUtils.convertToConsoleFormat(result));

            if (UserProfileUtils.getTotalProcessed() % 10 == 0) {
                System.out.println(UserProfileUtils.generateStatisticsReport());
            }
        }

        private String analyzeCommentContent(String comment, List<String> detectedWords) {
            if (comment == null || comment.trim().isEmpty()) {
                return "正常";
            }

            String content = comment.toLowerCase();
            boolean foundP0 = false;
            boolean foundP1 = false;
            boolean foundP2 = false;

            for (String keyword : p0Keywords) {
                if (content.contains(keyword)) {
                    detectedWords.add("P0:" + keyword);
                    foundP0 = true;
                }
            }

            for (String keyword : p1Keywords) {
                if (content.contains(keyword)) {
                    detectedWords.add("P1:" + keyword);
                    foundP1 = true;
                }
            }

            for (String keyword : p2Keywords) {
                if (content.contains(keyword)) {
                    detectedWords.add("P2:" + keyword);
                    foundP2 = true;
                }
            }

            if (foundP0) {
                return "P0";
            } else if (foundP1) {
                return "P1";
            } else if (foundP2) {
                return "P2";
            } else {
                return "正常";
            }
        }

        private String markSensitiveWords(String comment, String detectedWords, String level) {
            if (detectedWords == null || detectedWords.isEmpty()) {
                return comment;
            }

            String markedComment = comment;
            String[] wordEntries = detectedWords.split(", ");
            for (String entry : wordEntries) {
                if (entry.contains(":")) {
                    String word = entry.split(":")[1];
                    if (!word.isEmpty()) {
                        String marker = "";
                        switch (level) {
                            case "P0":
                                marker = "【🔴" + word + "🔴】";
                                break;
                            case "P1":
                                marker = "【🟡" + word + "🟡】";
                                break;
                            case "P2":
                                marker = "【🔵" + word + "🔵】";
                                break;
                            default:
                                marker = "【" + word + "】";
                        }
                        markedComment = markedComment.replace(word, marker);
                    }
                }
            }
            return markedComment;
        }

        private String getColorfulLevel(String level) {
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
    }
}