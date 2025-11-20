package com.stream.realtime.lululemon3;

import com.alibaba.fastjson2.JSONObject;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

public class PostgresToHBaseJob {

    public static void main(String[] args) throws Exception {
        // 1. 设置 Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);  // 启用 checkpoint，每 5 秒一个 checkpoint

        // 2. 配置 PostgreSQL CDC 数据源
        SourceFunction<String> source = createPostgresSource(); // 使用方法提取来简化主方法的代码

        // 3. 处理数据并写入 HBase
        env.addSource(source)
                .addSink(new PostgresToHBaseSink()); // 使用自定义 Sink 函数

        // 4. 执行作业
        env.execute("Postgres to HBase CDC Job");
    }

    /**
     * 创建 PostgreSQL CDC 数据源
     * 优化点：将数据源配置逻辑提取成独立方法，简化主方法，增强可维护性。
     */
    private static SourceFunction<String> createPostgresSource() {
        return PostgreSQLSource.<String>builder()
                .hostname("localhost")
                .port(5432)
                .database("spider_db")
                .schemaList("public")
                .tableList("public.user_info_base")
                .username("postgres")
                .password("wyh123.0")
                .deserializer(new JsonDebeziumDeserializationSchema())  // 使用 JSON 反序列化
                .build();
    }

    /**
     * 自定义 Sink 函数，将数据写入 HBase
     * 优化点：通过自定义 Sink 实现，将数据从 PostgreSQL CDC 写入 HBase
     */
    public static class PostgresToHBaseSink extends RichSinkFunction<String> {

        private transient Connection connection;  // HBase 连接对象
        private transient Table table;  // HBase 表对象

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            // 3.1 初始化 HBase 连接
            // 优化点：将 HBase 连接初始化逻辑提取到单独的方法，避免重复代码
            connection = createHBaseConnection();
            table = connection.getTable(TableName.valueOf("users"));
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            try {
                // 3.2 解析 CDC 数据
                JSONObject json = JSONObject.parseObject(value);
                JSONObject after = json.getJSONObject("after"); // 获取 "after" 字段，包含实际数据

                if (after != null) {
                    // 3.3 提取数据并构造 Put 对象
                    String rowKey = after.getString("id"); // 使用 ID 作为 RowKey
                    Put put = new Put(rowKey.getBytes());
                    put.addColumn("cf".getBytes(), "name".getBytes(), after.getString("name").getBytes()); // 存储 name 字段
                    put.addColumn("cf".getBytes(), "age".getBytes(), after.getString("age").getBytes()); // 存储 age 字段

                    // 3.4 执行写入操作
                    table.put(put);  // 将数据写入 HBase
                }
            } catch (Exception e) {
                // 3.5 异常处理
                // 优化点：添加异常捕获，记录日志，避免异常导致作业失败
                System.err.println("Error processing record: " + value);
                e.printStackTrace();  // 打印异常堆栈，方便排查问题
            }
        }

        @Override
        public void close() throws Exception {
            // 4. 关闭 HBase 连接
            if (table != null) {
                table.close();  // 关闭表连接
            }
            if (connection != null) {
                connection.close();  // 关闭 HBase 连接
            }
        }

        /**
         * 创建 HBase 连接
         * 优化点：将 HBase 连接创建逻辑提取到单独的方法，增强代码的可复用性和可维护性
         */
        private Connection createHBaseConnection() throws Exception {
            org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "localhost");  // Zookeeper 地址
            conf.set("hbase.zookeeper.property.clientPort", "2181");  // Zookeeper 端口
            return ConnectionFactory.createConnection(conf);  // 返回 HBase 连接
        }
    }
}
