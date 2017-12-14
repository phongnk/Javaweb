/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.vnpt.hive;

import com.google.gson.Gson;
import com.vnpt.hive.entity.BackupHiveOracle;
import static com.vnpt.hive.entity.BackupHiveOracle.HiveSql;
import static com.vnpt.hive.entity.BackupHiveOracle.writeDataToOracle;
import static com.vnpt.hive.entity.CassandraHive.processTac;
import static com.vnpt.hive.entity.CassandraHive.processdmh;
import static com.vnpt.hive.entity.CassandraHive.processCrm;
import com.vnpt.utils.ConfigClass;
import com.vnpt.utils.Util;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import scala.Tuple2;
import scala.Tuple3;

/**
 *
 * @author MSI
 */
public class Main {

    public static Logger logger = Logger.getLogger(Main.class);
    public static ConfigClass mConfig = null;
    public static JavaSparkContext jsc = null;
    public static SparkSession spark = null;

    public static void main(String[] args) throws IOException {
        init();

        spark = SparkSession
                .builder()
                //                .master("local[*]")
                .appName("Java Spark Hive Example")
                .enableHiveSupport()
                .config("spark.sql.sources.bucketing.enabled", true)
                .config("spark.sql.sources.bucketing.enabled", "true")
                .config("hive.exec.dynamic.partition", "true")
                .config("hive.exec.dynamic.partition.mode", "nonstrict")
                .config("spark.cassandra.connection.host", mConfig.cassandraHost)
                .config("spark.cassandra.auth.username", mConfig.cassandraUsername)
                .config("spark.cassandra.auth.password", mConfig.cassandraPassword)
                .config("spark.sql.warehouse.dir", mConfig.warehouseDir) //  warehouseLocation)                
                .getOrCreate();

        System.out.println("----------Đến lượt tạo bảng ----");

        jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());  // Hive dùng spark session 
       HiveSql();

        spark.stop();
        System.out.println("Process finished.");
        spark.stop();
    }

    private static void init() {

        // read file config    
        logger.info("load config file");
        String configJson = "";
        try (BufferedReader br = new BufferedReader(new FileReader("config/apps.conf"))) {
            String line = "";
            line = Util.readNextNonCommentNonEmpty(br);
            while (line != null) {
                configJson += line;
                line = Util.readNextNonCommentNonEmpty(br);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            return;
        }

        Gson gson = new Gson();
        mConfig = gson.fromJson(configJson, ConfigClass.class);
    }

}
