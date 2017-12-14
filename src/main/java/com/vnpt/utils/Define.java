/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.vnpt.utils;

/**
 *
 * @author MSI
 */
public class Define {
    //For db
    public static final String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
    public static final String CONNECTION_URL = "jdbc:oracle:thin:@10.33.66.226:1521:mytv";
    public static final String USER_NAME = "reportbi";
    public static final String PASS_WORD = "biai360";
    public static final String CONNECTION_URL_ORACLE = "jdbc:oracle:thin:" + USER_NAME + "/" + PASS_WORD +"@10.33.66.226:1521:mytv";
   
    //for db real
    public static final String ORACLE_DRIVER_REAL = "oracle.jdbc.driver.OracleDriver";
    public static final String CONNECTION_URL_REAL = "jdbc:oracle:thin:@192.168.103.98:1521:bigdata";
    public static final String USER_NAME_REAL = "segmentation";
    public static final String PASS_WORD_REAL = "customer123insight";
    public static final String CONNECTION_URL_ORACLE_REAL = "jdbc:oracle:thin:" + USER_NAME_REAL + "/" + PASS_WORD_REAL +"@192.168.103.98:1521:bigdata";
    
    //For SCHEMA
    public static String SPARK_CASSANDRA_USERNAME = "segment";
    public static String SPARK_CASSANDRA_PASSWORD = "tation123";
    public static String SPARK_CASSANDRA_SCHEMA_BACKUP = "stat_segment"; 
    public static String SPARK_CASSANDRA_TABLE_BACKUP = "customer_insight3"; 
    public static String[] ORACLE_MAP_COLUMN = new String[]{};
}
