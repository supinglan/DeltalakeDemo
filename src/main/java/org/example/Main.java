package org.example;

import lombok.val;
import lombok.var;
import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame;
import org.apache.spark.sql.*;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    public static void main(String[] args) {
        // 创建 SparkSession
        SparkSession spark = SparkSession.builder()
                .enableHiveSupport()
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .appName("Create Delta Table")
                .master("local")
                .getOrCreate();

        spark.sql("DROP DATABASE IF EXISTS data CASCADE;");
        spark.sql("CREATE DATABASE data");

        spark.sql("CREATE TABLE data.customer(id INT, name STRING) PARTITIONED BY (state STRING, city STRING);");
        spark.sql("INSERT INTO data.customer PARTITION (state = 'CA', city = 'Fremont') VALUES (100, 'John');");
        spark.sql("INSERT INTO data.customer PARTITION (state = 'CA', city = 'Fremont') VALUES (200, 'Marry');");
        spark.sql("INSERT INTO data.customer PARTITION (state = 'CA', city = 'Fremont') VALUES (100, 'Alan');");
        spark.sql("INSERT INTO data.customer PARTITION (state = 'AZ', city = 'Peoria') VALUES (200, 'Alex');");
        spark.sql("INSERT INTO data.customer PARTITION (state = 'AZ', city = 'Peoria') VALUES (300, 'Daniel');");

        Dataset<Row> customer=spark.read().table("data.customer");
        customer.write().format("delta")
                .partitionBy("state","city")
        .mode("overwrite").parquet("data.db\\customer");

        String basepath = "D:\\Desktop\\doris\\Demo\\";
        String dbname = "data";
        String tablename = "customer";

        Dataset<Row> splits=spark.sql("SHOW PARTITIONS data.customer;");
        List<Row> data=splits.collectAsList();
        int i = 0;
        for(Row row : data){
            System.out.println(row.getString(0));
            String path =basepath +dbname+".db\\"+tablename+"\\";
            String[] dir = row.getString(0).split("/");
            for (String s : dir) {
                path += s + "\\";
            }
            path+="*";
            Dataset<Row> dataframe = spark.read().parquet(path);
            dataframe.show();
            i++;
        };
        System.out.println("Total Partitions: " + i);
        splits.show();
        spark.stop();
    }
}



