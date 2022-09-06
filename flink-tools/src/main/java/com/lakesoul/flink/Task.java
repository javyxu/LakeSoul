package com.lakesoul.flink;

import org.apache.flink.lakeSoul.metaData.LakeSoulCatalog;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Task {
    public static void main(String[] args) {

        String hostname = "127.0.0.1";
        String mysqlDb = "test";
        String mysqlTb = "test_1";
        int port = 3306;
        String mysqlUserName = "root";
        String mysqlPassword = "123456";

        String tableSchema = "id bigint, name string, dt string, primary key(id) NOT ENFORCED";

        String lakeSoulDataBase = "test_lakesoul_meta";
        String lakeSoulTaBle = "nan_hang";
        String lakeSoulTablePath = "/Users/dudongfeng/work/zehy/";
        String lakeSoulTablePartition = "dt";
        String hashBucketNum = "1";

        String flinkCheckPointPath = "file:///Users/dudongfeng/work/zehy/checkpoint";
        int flinkParallelism = 1;
        long flinkCheckPointInterval = 5000;
        long flinkCinCheckPointInterval = 4000;

        if (args.length < 1) {
            System.out.println("请输入配置文件路径！");
            System.exit(1);
        }

        String filePath = args[0];
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(filePath));
        } catch (IOException e) {
            e.printStackTrace();
        }

        hostname = properties.getProperty("hostname");
        mysqlDb = properties.getProperty("mysqlDb");
        mysqlTb = properties.getProperty("mysqlTb");
        port = Integer.getInteger(properties.getProperty("port"));
        mysqlUserName = properties.getProperty("hostname");
        mysqlPassword = properties.getProperty("hostname");

        tableSchema = properties.getProperty("tableSchema");

        lakeSoulDataBase = properties.getProperty("lakeSoulDataBase");
        lakeSoulTaBle = properties.getProperty("lakeSoulTaBle");

        lakeSoulTablePath = properties.getProperty("lakeSoulTablePath");
        if (!lakeSoulTablePath.endsWith("/")) {
            lakeSoulTablePath += lakeSoulTablePath + "/";
        }
        lakeSoulTablePartition = properties.getProperty("lakeSoulTablePartition");
        hashBucketNum = properties.getProperty("hashBucketNum");

        flinkCheckPointPath = properties.getProperty("flinkCheckPointPath");
        flinkParallelism = Integer.parseInt(properties.getProperty("flinkParallelism"));
        flinkCheckPointInterval = Long.parseLong(properties.getProperty("checkPointInterval"));
        flinkCinCheckPointInterval = Long.parseLong(properties.getProperty("minCheckPointInterval"));

        String flinkCreateTableSql = String.format("create table mysql_binlog( %s )" +
                        " with ('connector'='mysql-cdc', 'hostname'='%s', 'port'='%s'," +
                        "'username'='%s', 'password'='%s', 'database-name'='%s', 'table-name'='%s')",
                tableSchema, hostname, port, mysqlUserName, mysqlPassword, mysqlDb, mysqlTb);

        String lakeSoulCreateTableSQL = String.format("CREATE TABLE %s (%s) PARTITIONED BY (%s) " +
                        "with ('connector' = 'lakeSoul', 'format'='parquet','path'='%s%s', 'useCDC'='true', 'bucket_num'='%s')",
                lakeSoulTaBle, tableSchema, lakeSoulTablePartition, lakeSoulTablePath, lakeSoulTaBle, hashBucketNum);

        StreamTableEnvironment tEnvs;
        StreamExecutionEnvironment env;
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(flinkParallelism);
        env.enableCheckpointing(flinkCheckPointInterval);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(flinkCinCheckPointInterval);
        env.getCheckpointConfig().setCheckpointStorage(flinkCheckPointPath);
        tEnvs = StreamTableEnvironment.create(env);
        tEnvs.getConfig().getConfiguration().set(
                ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);


        tEnvs.executeSql(flinkCreateTableSql);

        Catalog lakeSoulCatalog = new LakeSoulCatalog();
        tEnvs.registerCatalog("lakeSoul", lakeSoulCatalog);
        tEnvs.useCatalog("lakeSoul");

        //target
        tEnvs.executeSql(lakeSoulCreateTableSQL);

        tEnvs.useCatalog("default_catalog");
        tEnvs.executeSql("insert into lakeSoul." + lakeSoulDataBase + "." + lakeSoulTaBle + " select * from mysql_binlog ");
    }
}
