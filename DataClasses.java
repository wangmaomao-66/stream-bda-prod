// DataClasses.java
package com.stream.realtime.lululemon3;

import java.util.Date;
import java.sql.Timestamp;

/**
 * 数据类定义 - 包含所有数据实体类
 */
public class DataClasses {

    // 用户完整信息类
    public static class UserCompleteInfo {
        public String userId;
        public String username;
        public String phone;
        public String birthday;
        public String gender;
        public String address;
        public String deviceType;
        public String os;
        public String osVersion;
        public String ip;
        public String networkType;
        public Timestamp lastLoginTime;
        public int age = 0; // 准确年龄
        public String ageGroup;
        public String decade;
        public String zodiacSign;
        public String weight;
        public String height;
    }

    // 用户画像结果类
    public static class UserProfileResult {
        private String id;
        private String userID;
        private String orderID;
        private String productID;
        private String productName;
        private String comment;
        private String markedComment;
        private String commentLevel;
        private boolean isBlack;
        private int blackDays;
        private String detectedSensitiveWords;
        private String existingSensitiveWords;
        private String operationType;
        private Date processTime;
        private String createdDate;
        private UserCompleteInfo userCompleteInfo;
        private String ageLabel;
        private String genderLabel;
        private String zodiacSign;
        private String decade;

        // getter和setter方法
        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getUserID() { return userID; }
        public void setUserID(String userID) { this.userID = userID; }
        public String getOrderID() { return orderID; }
        public void setOrderID(String orderID) { this.orderID = orderID; }
        public String getProductID() { return productID; }
        public void setProductID(String productID) { this.productID = productID; }
        public String getProductName() { return productName; }
        public void setProductName(String productName) { this.productName = productName; }
        public String getComment() { return comment; }
        public void setComment(String comment) { this.comment = comment; }
        public String getMarkedComment() { return markedComment; }
        public void setMarkedComment(String markedComment) { this.markedComment = markedComment; }
        public String getCommentLevel() { return commentLevel; }
        public void setCommentLevel(String commentLevel) { this.commentLevel = commentLevel; }
        public boolean getIsBlack() { return isBlack; }
        public void setIsBlack(boolean isBlack) { this.isBlack = isBlack; }
        public int getBlackDays() { return blackDays; }
        public void setBlackDays(int blackDays) { this.blackDays = blackDays; }
        public String getDetectedSensitiveWords() { return detectedSensitiveWords; }
        public void setDetectedSensitiveWords(String detectedSensitiveWords) { this.detectedSensitiveWords = detectedSensitiveWords; }
        public String getExistingSensitiveWords() { return existingSensitiveWords; }
        public void setExistingSensitiveWords(String existingSensitiveWords) { this.existingSensitiveWords = existingSensitiveWords; }
        public String getOperationType() { return operationType; }
        public void setOperationType(String operationType) { this.operationType = operationType; }
        public Date getProcessTime() { return processTime; }
        public void setProcessTime(Date processTime) { this.processTime = processTime; }
        public String getCreatedDate() { return createdDate; }
        public void setCreatedDate(String createdDate) { this.createdDate = createdDate; }
        public UserCompleteInfo getUserCompleteInfo() { return userCompleteInfo; }
        public void setUserCompleteInfo(UserCompleteInfo userCompleteInfo) { this.userCompleteInfo = userCompleteInfo; }
        public String getAgeLabel() { return ageLabel; }
        public void setAgeLabel(String ageLabel) { this.ageLabel = ageLabel; }
        public String getGenderLabel() { return genderLabel; }
        public void setGenderLabel(String genderLabel) { this.genderLabel = genderLabel; }
        public String getZodiacSign() { return zodiacSign; }
        public void setZodiacSign(String zodiacSign) { this.zodiacSign = zodiacSign; }
        public String getDecade() { return decade; }
        public void setDecade(String decade) { this.decade = decade; }

        @Override
        public String toString() {
            int exactAge = userCompleteInfo != null ? userCompleteInfo.age : 0;
            return String.format(
                    "UserProfileResult{" +
                            "userID='%s', " +
                            "orderID='%s', " +
                            "productName='%s', " +
                            "commentLevel='%s', " +
                            "isBlack=%s, " +
                            "blackDays=%d, " +
                            "exactAge=%d, " +
                            "ageLabel='%s', " +
                            "genderLabel='%s', " +
                            "zodiacSign='%s', " +
                            "decade='%s', " +
                            "detectedWords='%s'" +
                            "}",
                    userID, orderID,
                    productName != null && productName.length() > 20 ? productName.substring(0, 20) + "..." : productName,
                    commentLevel, isBlack, blackDays, exactAge, ageLabel, genderLabel, zodiacSign, decade,
                    detectedSensitiveWords != null && detectedSensitiveWords.length() > 30 ?
                            detectedSensitiveWords.substring(0, 30) + "..." : detectedSensitiveWords
            );
        }
    }

    // 评论数据类
    public static class CommentData {
        private String id;
        private String orderID;
        private String userID;
        private String productID;
        private String productName;
        private String comment;
        private String productNameFromComment;
        private String existingSensitiveWords;
        private String createdDate;
        private String operationType;

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getOrderID() { return orderID; }
        public void setOrderID(String orderID) { this.orderID = orderID; }
        public String getUserID() { return userID; }
        public void setUserID(String userID) { this.userID = userID; }
        public String getProductID() { return productID; }
        public void setProductID(String productID) { this.productID = productID; }
        public String getProductName() { return productName; }
        public void setProductName(String productName) { this.productName = productName; }
        public String getComment() { return comment; }
        public void setComment(String comment) { this.comment = comment; }
        public String getProductNameFromComment() { return productNameFromComment; }
        public void setProductNameFromComment(String productNameFromComment) { this.productNameFromComment = productNameFromComment; }
        public String getExistingSensitiveWords() { return existingSensitiveWords; }
        public void setExistingSensitiveWords(String existingSensitiveWords) { this.existingSensitiveWords = existingSensitiveWords; }
        public String getCreatedDate() { return createdDate; }
        public void setCreatedDate(String createdDate) { this.createdDate = createdDate; }
        public String getOperationType() { return operationType; }
        public void setOperationType(String operationType) { this.operationType = operationType; }
    }
}