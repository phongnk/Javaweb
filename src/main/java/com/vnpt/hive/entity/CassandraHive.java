/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.vnpt.hive.entity;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.column;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import com.datastax.spark.connector.japi.CassandraRow;
import static com.vnpt.hive.Main.jsc;
import static com.vnpt.hive.Main.spark;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 *
 * @author Boss
 */
public class CassandraHive {

    public static void processTac(String table) {

        JavaRDD<CassandraRow> rddGgsn = javaFunctions(jsc)
                .cassandraTable("vinaphone_crm_new", table)
                .select(
                        column("msisdn"),
                        column("update_time"),
                        column("2g_support"),
                        column("3g_support"),
                        column("brand"),
                        column("cam_resolution"),
                        column("data_support"),
                        column("device_category"),
                        column("display_resolution"),
                        column("imei"),
                        column("imsi"),
                        column("last_configuration"),
                        column("last_detection"),
                        column("model_device"),
                        column("msisdn_type"),
                        column("os_brand"),
                        column("os_name"),
                        column("os_version"),
                        column("ota_support"),
                        column("weight")
                );

        JavaRDD<Row> rowRDD = rddGgsn.map((t) -> {
            String date = "20170000";
            try {
                date = t.getString("update_time").trim().substring(0, 10).replace("-", "");
            } catch (Exception e) {
                e.getMessage();
            }
            return RowFactory.create(
                    t.getString("msisdn"),
                    t.getString("update_time"),
                    t.getString("2g_support"),
                    t.getString("3g_support"),
                    t.getString("brand"),
                    t.getString("cam_resolution"),
                    t.getString("data_support"),
                    t.getString("device_category"),
                    t.getString("display_resolution"),
                    t.getString("imei"),
                    t.getString("imsi"),
                    t.getString("last_configuration"),
                    t.getString("last_detection"),
                    t.getString("model_device"),
                    t.getString("msisdn_type"),
                    t.getString("os_brand"),
                    t.getString("os_name"),
                    t.getString("os_version"),
                    t.getString("ota_support"),
                    t.getString("weight"),
                    date
            );
        });

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("msisdn", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("update_time", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("2g_support", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("3g_support", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("brand", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("cam_resolution", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("data_support", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("device_category", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("display_resolution", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("imei", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("imsi", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("last_configuration", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("last_detection", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("model_device", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("msisdn_type", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("os_brand", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("os_name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("os_version", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("ota_support", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("weight", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("activity_date", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);
//        if (partitionType != null) {
//            fields.add(partitionType);
//        }
//        StructType schema = DataTypes.createStructType(fields);
        Dataset<Row> demoDF = spark.createDataFrame(rowRDD, schema);

        demoDF.write().mode(SaveMode.Append).insertInto("indb.crm_tac");

        System.out.println("================ process done =================");
    }

    public static void processdmh(String table) {

        JavaRDD<CassandraRow> rddGgsn = javaFunctions(jsc)
                .cassandraTable("vinaphone_in_new", table)
                .select(
                        column("a_subs"),
                        column("begin_date"),
                        column("in_type"),
                        column("activity_type"),
                        column("accumulators_info"),
                        column("b_subs"),
                        column("b_subs_org"),
                        column("b_subs_org_type"),
                        column("b_subs_type"),
                        column("balances_info"),
                        column("cell_id"),
                        column("charge_item_id"),
                        column("description"),
                        column("end_date"),
                        column("fund_usage_type"),
                        column("msg_type"),
                        column("ocs_comment"),
                        column("psa_subs_type"),
                        column("session_id"),
                        column("spending_limits_info"),
                        column("unit_type_id"),
                        column("usage")
                );

        JavaRDD<Row> rowRDD = rddGgsn.map((t) -> {
            String date = "20170000";
            try {
                date = t.getString("begin_date").trim().substring(0, 8);
            } catch (Exception e) {
                e.getMessage();
            }

            double TKC = 0;
            double KM = 0;
            double KM2 = 0;
            double DK1 = 0;
            double DK2 = 0;
            double VDK2 = 0;

            try {
                String lowb = t.getString("balances_info");
                String arr[] = lowb.split("]");
                for (String w : arr) {
                    String arrLow[] = w.trim().split("~");

                    if (arrLow[0].trim().replace("[", "").equals("1")) {
                        TKC += Double.parseDouble(arrLow[2].trim());
                    }
                    if (arrLow[0].trim().replace("[", "").equals("2")) {
                        KM += Double.parseDouble(arrLow[2].trim());
                    }
                    if (arrLow[0].trim().replace("[", "").equals("162")) {
                        KM2 += Double.parseDouble(arrLow[2].trim());
                    }
                    if (arrLow[0].trim().replace("[", "").equals("4")) {
                        DK1 += Double.parseDouble(arrLow[2].trim());
                    }
                    if (arrLow[0].trim().replace("[", "").equals("142")) {
                        DK2 += Double.parseDouble(arrLow[2].trim());
                    }
                    if (arrLow[0].trim().replace("[", "").equals("62")) {
                        VDK2 += Double.parseDouble(arrLow[2].trim());
                    }

                }

            } catch (Exception e) {
                e.getMessage();
            }

            return RowFactory.create(
                    t.getString("a_subs"),
                    t.getString("begin_date"),
                    t.getString("in_type"),
                    t.getString("activity_type"),
                    t.getString("accumulators_info"),
                    t.getString("b_subs"),
                    t.getString("b_subs_org"),
                    t.getString("b_subs_org_type"),
                    t.getString("b_subs_type"),
                    t.getString("balances_info"),
                    t.getString("cell_id"),
                    t.getString("charge_item_id"),
                    t.getString("description"),
                    t.getString("end_date"),
                    t.getString("fund_usage_type"),
                    t.getString("msg_type"),
                    t.getString("ocs_comment"),
                    t.getString("psa_subs_type"),
                    t.getString("session_id"),
                    t.getString("spending_limits_info"),
                    t.getString("unit_type_id"),
                    t.getString("usage"),
                    String.valueOf(TKC),
                    String.valueOf(KM),
                    String.valueOf(KM2),
                    String.valueOf(DK1),
                    String.valueOf(DK2),
                    String.valueOf(VDK2),
                    date
            );
        });

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("a_subs", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("begin_date", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("in_type", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("activity_type", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("accumulators_info", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("b_subs", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("b_subs_org", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("b_subs_org_type", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("b_subs_type", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("balances_info", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("cell_id", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("charge_item_id", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("description", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("end_date", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("fund_usage_type", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("msg_type", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("ocs_comment", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("psa_subs_type", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("session_id", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("spending_limits_info", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("unit_type_id", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("CORE_BALANCE", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("Voice_and_SMS_On_Net", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("On_net_VinaPhone", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("SMS_on_net_currency", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("3G_Currency", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("Voice_OnNet_VNPT_seconds", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("usage", DataTypes.StringType, true));

        fields.add(DataTypes.createStructField("activity_date", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);
//        if (partitionType != null) {
//            fields.add(partitionType);
//        }
//        StructType schema = DataTypes.createStructType(fields);
        Dataset<Row> demoDF = spark.createDataFrame(rowRDD, schema);

        demoDF.write().mode(SaveMode.Append).insertInto("indb.dmh");

        System.out.println("================ process done =================");
    }
    
    
     public static void processCrm(String table) {

        JavaRDD<CassandraRow> rddcrm = javaFunctions(jsc)
                .cassandraTable("vinaphone_crm_new", table)
                .select(
                        column("msisdn"),
                        column("update_time"),
                        column("address"),
                        column("birthday"),
                        column("idnumber"),
                        column("msisdn_type"),
                        column("name"),
                        column("placeofissue"),
                        column("registdate"),
                        column("sex"),
                        column("status"),
                        column("type")
//                     msisdn text,
//	update_time timestamp,
//	address text,
//	birthday text,
//	idnumber text,
//	msisdn_type text,
//	name text,
//	placeofissue text,
//	registdate text,
//	sex text,
//	status text,
//	type text,   
                        
                        
                        
      
                        
                );

        
        
        
        
        
        
        
        JavaRDD<Row> rowRDD =rddcrm.map((t) -> {
            String date = "20170000";
            try {
                date = t.getString("update_time").trim().substring(0, 10).replace("-", "");
            } catch (Exception e) {
                e.getMessage();
            }
            return RowFactory.create(
                    t.getString("msisdn"),
                    t.getString("update_time"),
                    t.getString("address"),
                    t.getString("birthday"),
                    t.getString("idnumber"),
                    t.getString("msisdn_type"),
                    t.getString("name"),
                    t.getString("placeofissue"),
                    t.getString("registdate"),
                    t.getString("sex"),
                    t.getString("status"),
                    t.getString("type"),
                    date
            );
        });

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("msisdn", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("update_time", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("address", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("birthday", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("idnumber", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("msisdn_type", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("placeofissue", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("registdate", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("sex", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("status", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("type", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("activity_date", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);
//        if (partitionType != null) {
//            fields.add(partitionType);
//        }
//        StructType schema = DataTypes.createStructType(fields);
        Dataset<Row> demoDF = spark.createDataFrame(rowRDD, schema);

        demoDF.write().mode(SaveMode.Append).insertInto("indb.crm");

        System.out.println("================ process done =================");
    }
}
