package org.example;
import org.apache.spark.sql.*;

public class Main{
    public static void main(String[] args) {
        // 创建 SparkSession
        SparkSession spark = SparkSession.builder()
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .appName("Create Delta Table")
                .master("local")
                .getOrCreate();

        // 设置 Delta Lake 表的存储路径
        String deltaPath = "/path/to/delta/table";

        // 创建示例数据帧
        Dataset<Row> data = spark.range(1, 10)
                .selectExpr("id", "id * 2 as value");

        // 写入 Delta Lake 表
        data.write()
                .format("delta")
                .mode("overwrite")
                .save(deltaPath);

        // 读取 Delta Lake 表数据
        Dataset<Row> deltaData = spark.read()
                .format("delta")
                .load(deltaPath);

        // 显示 Delta Lake 表数据
        deltaData.show();
    }
}



