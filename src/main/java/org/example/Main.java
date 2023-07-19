package org.example;

import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;

import java.util.Iterator;
import java.util.List;


public class Main {
    public static void main(String[] args) {
        // 创建 SparkSession
        SparkSession spark = SparkSession.builder()
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .appName("Create Delta Table")
                .master("local")
                .getOrCreate();

        // 设置 Delta Lake 表的存储路径
        String deltaPath = "../data";

        // 创建示例数据帧
        Dataset<Row> data = spark.range(1, 100000000)
                .selectExpr("id", "id * 2 as value");


        // 写入 Delta Lake 表
        org.apache.spark.sql.DataFrameWriter<Row> dataFrameWriter = data.write()
                .format("delta");
        dataFrameWriter.mode("overwrite").save(deltaPath);;

        // 读取 Delta Lake 表数据
        Dataset<Row> deltaData = spark.read()
                .format("delta")
                .load(deltaPath);

        // 显示 Delta Lake 表数据
        deltaData.show();
        int n =deltaData.rdd().getNumPartitions();
        System.out.println(n);

        if (deltaData != null) {
            // 将 Delta Lake 表转换为 RDD
            // 注意：需要导入 org.apache.spark.api.java.JavaRDD;
            JavaRDD<Row> deltaRDD = deltaData.toJavaRDD();

            // 获取 Delta Lake 表的所有切片
            List<Partition> partitions = deltaRDD.partitions();
            int i =0;
            // 遍历每个切片
            for (Partition partition : partitions) {
                i++;
                System.out.println("第"+i+"个分片\n");
            }
        } else {
            System.out.println("Failed to load Delta Lake table.");
        }
        spark.stop();
    }
}



