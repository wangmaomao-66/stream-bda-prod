// SensitiveCommentDetectionJob.java
package com.stream.realtime.lululemon3;

import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * 敏感评论检测与用户画像基础特征计算作业 - 主类
 */
public class SensitiveCommentDetectionJob {

    public static void main(String[] args) throws Exception {
        try {
            // 初始化工具类
            UserProfileUtils.initialize();

            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            env.enableCheckpointing(10000);

            // 初始化动态计算的系数和权重
            UserProfileUtils.initializePreferenceCoefficients();

            DebeziumSourceFunction<String> source = SqlServerSource.<String>builder()
                    .hostname("localhost")
                    .port(1433)
                    .database("realtime_v3")
                    .tableList("dbo.ProductComments")
                    .username("sa")
                    .password("wyh123.0")
                    .deserializer(new JsonDebeziumDeserializationSchema())
                    .build();

            DataStream<String> cdcStream = env.addSource(source)
                    .name("SQL Server CDC Source");

            SingleOutputStreamOperator<DataClasses.UserProfileResult> processedStream = cdcStream
                    .flatMap(new FlinkProcessors.CommentDataParser())
                    .flatMap(new FlinkProcessors.UserProfileProcessor())
                    .name("用户画像处理");

            processedStream.print().name("结果输出");

            // 创建Doris Sink - 带重试和诊断
            DorisSink<String> dorisSink = createDorisSinkWithRetry();

            // 无论Doris是否连接成功，都添加增强控制台输出
            processedStream
                    .map(userProfileResult -> {
                        String consoleOutput = UserProfileUtils.convertToConsoleFormat(userProfileResult);
                        System.out.println(consoleOutput);

                        if (UserProfileUtils.getTotalProcessed() % 10 == 0) {
                            System.out.println(UserProfileUtils.generateStatisticsReport());
                        }

                        return userProfileResult;
                    })
                    .name("增强控制台输出");

            // 只有Doris Sink创建成功时才添加sink
            if (UserProfileUtils.isDorisConnected() && dorisSink != null) {
                DataStream<String> dorisStream = processedStream
                        .map(userProfileResult -> UserProfileUtils.convertToDorisJsonFormat(userProfileResult))
                        .name("转换为Doris JSON格式");

                dorisStream.sinkTo(dorisSink).name("写入Doris");
                System.out.println("✅ Doris Sink已添加，数据将写入Doris表: " + UserProfileUtils.DORIS_DATABASE + "." + UserProfileUtils.DORIS_TABLE);
            } else {
                System.out.println("⚠️ Doris Sink未添加，数据仅输出到控制台");
                System.out.println("📝 重要: 所有处理结果将在控制台显示，但不会持久化到Doris");
            }

            System.out.println("🚀 开始执行敏感评论检测与用户画像计算作业...");
            System.out.println("📊 运行模式: " + (UserProfileUtils.isDorisConnected() ? "完整模式(控制台+Doris)" : "控制台模式(仅控制台输出)"));

            env.execute("敏感评论检测与用户画像计算作业");

        } catch (Exception e) {
            System.err.println("❌ 作业执行失败: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * 创建Doris Sink（带重试机制）
     */
    private static DorisSink<String> createDorisSinkWithRetry() {
        int maxRetries = 2;  // 减少重试次数，快速失败
        int retryCount = 0;

        while (retryCount < maxRetries) {
            try {
                System.out.println("🔄 尝试创建Doris Sink (尝试 " + (retryCount + 1) + "/" + maxRetries + ")...");

                // 先进行连接测试
                if (!testDorisConnection()) {
                    throw new RuntimeException("Doris连接测试失败");
                }

                DorisSink<String> sink = createDorisSink();
                UserProfileUtils.setDorisConnected(true);
                System.out.println("✅ Doris Sink创建成功");
                return sink;

            } catch (Exception e) {
                retryCount++;
                System.err.println("❌ 创建Doris Sink失败 (尝试 " + retryCount + "/" + maxRetries + "): " + e.getMessage());

                if (retryCount >= maxRetries) {
                    System.err.println("❌ 重试次数用尽，无法连接到Doris数据库");
                    System.err.println("⚠️ 数据将仅输出到控制台，不会写入Doris");
                    UserProfileUtils.setDorisConnected(false);
                    return null;
                } else {
                    try {
                        System.out.println("⏳ 等待3秒后重试...");
                        TimeUnit.SECONDS.sleep(3);
                    } catch (InterruptedException ie) {
                        System.err.println("❌ 重试被中断");
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        return null;
    }

    /**
     * 测试Doris连接
     */
    private static boolean testDorisConnection() {
        System.out.println("🔍 测试Doris连接...");

        // 测试HTTP连接
        try {
            java.net.URL url = new java.net.URL("http://127.0.0.1:8030/api/health");
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);

            int responseCode = conn.getResponseCode();
            System.out.println("✅ HTTP连接测试: 成功 (响应码: " + responseCode + ")");
            return true;
        } catch (Exception e) {
            System.err.println("❌ HTTP连接测试: 失败 - " + e.getMessage());
            return false;
        }
    }

    /**
     * 创建Doris Sink（使用正确的配置）
     */
    private static DorisSink<String> createDorisSink() {
        System.out.println("📊 Doris配置信息:");
        System.out.println("   - FE节点: " + UserProfileUtils.DORIS_FENODES);
        System.out.println("   - 数据库: " + UserProfileUtils.DORIS_DATABASE);
        System.out.println("   - 表: " + UserProfileUtils.DORIS_TABLE);
        System.out.println("   - 用户名: " + UserProfileUtils.DORIS_USERNAME);

        try {
            DorisOptions dorisOptions = DorisOptions.builder()
                    .setFenodes(UserProfileUtils.DORIS_FENODES)
                    .setTableIdentifier(UserProfileUtils.DORIS_DATABASE + "." + UserProfileUtils.DORIS_TABLE)
                    .setUsername(UserProfileUtils.DORIS_USERNAME)
                    .setPassword(UserProfileUtils.DORIS_PASSWORD)
                    .build();

            Properties properties = UserProfileUtils.getDorisConnectionProperties();

            DorisExecutionOptions executionOptions = DorisExecutionOptions.builder()
                    .setBufferSize(1024 * 1024)
                    .setBufferCount(3)
                    .setMaxRetries(3)
                    .setStreamLoadProp(properties)
                    .build();

            DorisSink<String> sink = DorisSink.<String>builder()
                    .setDorisReadOptions(DorisReadOptions.builder().build())
                    .setDorisExecutionOptions(executionOptions)
                    .setDorisOptions(dorisOptions)
                    .setSerializer(new SimpleStringSerializer())
                    .build();

            System.out.println("✅ Doris Sink配置创建成功");
            return sink;

        } catch (Exception e) {
            throw new RuntimeException("创建Doris Sink失败: " + e.getMessage(), e);
        }
    }
}