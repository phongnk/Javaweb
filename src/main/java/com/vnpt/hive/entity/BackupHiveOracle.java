/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.vnpt.hive.entity;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import java.util.Properties;
import org.apache.spark.sql.types.StructType;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import static org.apache.spark.sql.functions.log;
import com.datastax.spark.connector.japi.CassandraRow;
import org.apache.spark.api.java.JavaRDD;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.vnpt.hive.Main.jsc;
import static com.vnpt.hive.Main.spark;
import com.vnpt.utils.Define;
import java.io.IOException;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

/**
 *
 * @author Boss
 */
public class BackupHiveOracle {

    public static Dataset<Row> HiveSql() throws IOException {
        System.out.println("vào query hive rồi nạ");
        String sqlthdd = "select \n"
                + "c.last_updated_file as last_updated_file,a.registdate as registdate,\n"
                + "sum(c.tien) as tien,\n"
                + "sum(case when c.tien >0 then 1 else 0 end ) as sum,\n"
                + "sum(case when c.tien >0 and c.tien <=50000 then 1 else 0 end ) as sum50K,\n"
                + "sum(case when c.tien >50000 and c.tien <=100000 then 1 else 0 end ) as sum50K100K,\n"
                + "sum(case when c.tien >100000 and c.tien <=200000 then 1 else 0 end ) as sum100K200K,\n"
                + "sum(case when c.tien >200000 and c.tien <=500000 then 1 else 0 end ) as sum200K500K,\n"
                + "sum(case when c.tien >500000 then 1 else 0 end ) as sum500K\n"
                + "from \n"
                + "(select sum(b.tien) as tien,b.ma_tb,concat(substring(last_updated_file,3,4),substring(last_updated_file,0,2) ) as last_updated_file from indb.thdd b group by b.ma_tb,b.last_updated_file)c ,\n"
                + "(select msisdn,Max(cast(concat(substring(registdate,7,4),substring(registdate,4,2) ) as int)) as registdate \n"
                + " from indb.crm where substring(registdate,7,4) = '2017' group by msisdn) a\n"
                + "where a.msisdn = c.ma_tb group by c.last_updated_file,a.registdate order by a.registdate,c.last_updated_file";
        String sqlThuebao = "select dateUpdate as updateTime,registdate as regis,sum(huy) as huy,sum(2C) as 2C,sum(1C) as 1C from \n"
                + "(select msisdn,Max(cast(concat(substring(registdate,7,4),substring(registdate,4,2) ) as int))\n"
                + " as registdate from indb.crm where substring(registdate,7,4) = '2017' group by msisdn) a\n"
                + ",\n"
                + "(select so_tb,\n"
                + "(case when substring(last_updated_file,13,2)= '20' then concat(substring(last_updated_file,13,4) , substring(last_updated_file,11,2))\n"
                + "															  else substring(last_updated_file,9,6) end ) as dateUpdate,\n"
                + "(case when trang_thai = 'SP' then 1 else 0 end) as huy,\n"
                + "(case when goi_di = 'D' and goi_den ='D'then 1 else 0 end) as 2C,\n"
                + "(case when goi_di = 'D' or goi_den ='D' then 1 else 0 end) as 1C\n"
                + "  from indb.thuebao) b \n"
                + "  where a.msisdn = b.so_tb group by registdate,dateUpdate order by registdate,dateUpdate";

        String sqlMscGgsn = "select t.regis,t.datemonth,h.countmsisdn ,(h.countmsisdn-t.Countmsisdn) as notCountMsisdn,\n"
                + "t.Countmsisdn countMsisdnPsc,t.sms ,t.smsnoimang ,t.smsngoaimang,t.goi,t.goinoimang,t.goingoaimang, t.sumdownlink, t.sumuplink\n"
                + "from \n"
                + " \n"
                + "(select g.regis,g.dateM as datemonth,count(DISTINCT(g.sdt)) as Countmsisdn , sum(g.sms) as sms ,sum(g.smsnoimang) as smsnoimang,\n"
                + "sum(g.smsngoaimang) as smsngoaimang,sum(g.goi) as goi,sum(g.goinoimang) as goinoimang ,sum(g.goingoaimang) as goingoaimang,\n"
                + "sum(g.sumuplink) as sumuplink,sum(sumdownlink) as sumdownlink from \n"
                + "(\n"
                + " select (case when f.msisdinMsc  is null then f.msisdinGgsn else f.msisdinMsc end) sdt ,\n"
                + "(case when f.registdateMsc is null then f.registdateGGsn else  f.registdateMsc end) as regis ,\n"
                + "(case when f.datemonthMsc is null then f.datemonthGgsn else  f.datemonthMsc end) as dateM ,\n"
                + "f.* from  (select * from \n"
                + "(SELECT *\n"
                + "FROM (select msisdn as msisdinMsc,Max(cast(concat(substring(registdate,7,4),substring(registdate,4,2) ) as int))\n"
                + " as registdateMsc from indb.crm where substring(registdate,7,4) = '2017' group by msisdn) a\n"
                + "INNER JOIN\n"
                + " (select a_subs as a_subsMsc,substring(activity_date,0,6) as datemonthMsc, \n"
                + "sum(case when call_type = 'SMO' then 1 else NULL end) AS sms,\n"
                + "sum(case when call_type = 'SMO' and b_subs_type in ('8491','8494','8488','84123','84124','84125','84127','84129') \n"
                + "then 1 else NULL end) AS smsnoimang,\n"
                + "sum(case when call_type = 'SMO' and b_subs_type not in ('8491','8494','8488','84123','84124','84125','84127','84129') \n"
                + "then 1 else NULL end) AS smsngoaimang,\n"
                + "sum (case when call_type = 'MOC'  then call_duration else NULL end) as goi,\n"
                + "sum (case when call_type = 'MOC' and b_subs_type in ('8491','8494','8488','84123','84124','84125','84127','84129') \n"
                + "   then call_duration else NULL end) as goinoimang,\n"
                + "     sum (case when call_type = 'MOC' and b_subs_type not in ('8491','8494','8488','84123','84124','84125','84127','84129') \n"
                + "     then call_duration else NULL end) as goingoaimang\n"
                + "from voice_msc.msc_bi where  substring(activity_date,0,4)= '2017' and call_type in('MOC','SMO') group by a_subs,substring(activity_date,0,6))\n"
                + " b ON a.msisdinMsc = b.a_subsMsc) d\n"
                + "FULL OUTER JOIN \n"
                + "(SELECT *\n"
                + "FROM  (select msisdn as msisdinGgsn,Max(cast(concat(substring(registdate,7,4),substring(registdate,4,2) ) as int))\n"
                + " as registdateGGsn from indb.crm where substring(registdate,7,4) = '2017' group by msisdn) a\n"
                + "inner JOIN \n"
                + "(select  a_subs as a_subsGgsn,sum(downlink) as sumdownlink,sum(uplink) as sumuplink,substr(activity_date,0,6) as datemonthGgsn\n"
                + "from ggsn.ggsn where substring(activity_date,0,4)= '2017' group by activity_date, a_subs, substr(activity_date,0,6) )c \n"
                + " ON a.msisdinGgsn = c.a_subsGGsn) e on  d.datemonthMsc= e.datemonthGgsn and d.msisdinMsc=e.msisdinGgsn and d.registdateMsc = e.registdateGGsn  )f )g group by g.regis,g.dateM) t\n"
                + " ,\n"
                + " (select count(n.msisdn) as countmsisdn ,n.regisdate from \n"
                + " (select  msisdn,Max(cast(concat(substring(registdate,7,4),substring(registdate,4,2) ) as int)) as regisdate from indb.crm where substring(registdate,7,10) = '2017' group by msisdn )n\n"
                + "group by n.regisdate order by n.regisdate) h \n"
                + "where h.regisdate = t.regis order by t.regis,t.datemonth";

        String sqlinchadmh = "select ma_tinh,regis,datemonth,core_id,count(sdt) countsdt,\n" +
"(coalesce(cast(sum(sumchacore) as double),0.00) + coalesce(cast(sum(sumchacore) as double),0.00)) coretotal,\n" +
"sum(sumchacore) chacore, sum(suminchacallcore) chacallcore,sum(suminchasmscore) chasmscore,\n" +
"sum(sumchaothercore) chaothercore,sum(sumcallkm) chacallkm,sum(sumsmskm) chasmskm,\n" +
"sum(sumchacore) dmhcore,sum(sumdmhothercore) dmhothercore,sum(othertotal) othertoal\n" +
"from \n" +
"(select * from (select \n" +
"(case when t.cha_asubs  is null then t.dmh_asubs else t.cha_asubs end) sdt ,\n" +
"(case when t.chamonth is null then t.dmhmonth else  t.chamonth end) as datemonth ,\n" +
"\n" +
"(case\n" +
" when(coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00))  >0      and (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00)) <=5000    then 1 \n" +
" when(coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00))  >5000   and (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00)) <=10000   then 2\n" +
"when (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00))  >10000  and (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00)) <=20000   then 3\n" +
"when (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00))  >20000  and (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00)) <=30000   then 4\n" +
"when (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00))  >30000  and (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00)) <=40000   then 5\n" +
"when (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00))  >40000  and (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00)) <=50000   then 6\n" +
"when (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00))  >50000  and (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00)) <=60000   then 7\n" +
"when (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00))  >60000  and (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00)) <=70000   then 8\n" +
"when (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00))  >70000  and (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00)) <=80000   then 9\n" +
"when (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00))  >80000  and (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00)) <=90000   then 10\n" +
"when (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00))  >90000  and (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00)) <=100000  then 11\n" +
"when (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00))  >100000 and (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00)) <=200000  then 12\n" +
"when (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00))  >200000 and (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00)) <=300000  then 13\n" +
"when (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00))  >300000 and (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00)) <=400000  then 14\n" +
"when (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00))  >400000 and (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00)) <=500000  then 15\n" +
"when (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00))  >500000 and (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00)) <=600000  then 16\n" +
"when (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00))  >600000 and (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00)) <=700000  then 17\n" +
"when (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00))  >700000 and (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00)) <=800000  then 18\n" +
"when (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00))  >800000 and (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00)) <=900000  then 19\n" +
"when (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00))  >900000 and (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00)) <=1000000 then 20\n" +
"when (coalesce(cast(t.sumchacore as double),0.00) + coalesce(cast(t.sumdmhcore as double),0.00))  >1000000   then 21\n" +
"else 0 end) as core_id,\n" +
"t.sumchacore,t.suminchacallcore,t.suminchasmscore,t.sumchaothercore,t.sumsmskm,t.sumcallkm,\n" +
"t.sumdmhcore,t.sumdmhothercore, (coalesce(cast(t.sumchaothercore as double),0.00) + coalesce(cast(t.sumdmhothercore as double),0.00)) othertotal\n" +
"from (\n" +
"		 select * from (\n" +
"		select sum(core_balance) sumchacore,sum(callcore)  suminchacallcore,sum(smscore) suminchasmscore,sum(othercore)sumchaothercore ,\n" +
"		sum(callkm) sumcallkm,sum(smskm) sumsmskm,a_subs cha_asubs,month chamonth from\n" +
"		(\n" +
"		select \n" +
"		  a_subs,substring(activity_date,5,2) as month,core_balance,\n" +
"		 (case when activity_type ='OutgoingCallAttempt' then Voice_and_SMS_On_Net else 0 end)as callkm,\n" +
"		(case when activity_type ='Cap3OriginatingSms' then Voice_and_SMS_On_Net else 0 end)as smskm,\n" +
"		(case when activity_type ='OutgoingCallAttempt' then core_balance else 0 end)as callcore,\n" +
"		(case when activity_type ='Cap3OriginatingSms' then core_balance else 0 end)as smscore,\n" +
"		(case when activity_type  not in ('OutgoingCallAttempt','Cap3OriginatingSms') then core_balance else 0 end)as othercore\n" +
"		from indb.in_cha ) a group by a_subs,month ) m\n" +
"FULL OUTER JOIN  \n" +
"		(select sum(core_balance) sumdmhcore, sum(otherdatacore) sumdmhothercore,a_subs dmh_asubs,month dmhmonth from \n" +
"		(select a_subs ,substring(activity_date,5,2) as month,core_balance,\n" +
"		(case when activity_type <> 'unitBasedCharge' then core_balance else 0 end)as otherdatacore\n" +
"		from indb.dmh ) a group by a_subs,month)n\n" +
"	on m.cha_asubs=n.dmh_asubs and m.chamonth= n.dmhmonth )t \n" +
")g\n" +
"inner join \n" +
"	(select b.msisdn,a.ma_tinh,substring(b.registdate,7,4) regis from indb.thuebao a inner join \n" +
"	indb.crm b on cast(substring(b.registdate,7,4) as double) >1970 and cast(substring(b.registdate,7,4) as double) <2018 and a.so_tb=b.msisdn) h \n" +
"on h.msisdn=g.sdt)o \n" +
"group by o.ma_tinh,o.datemonth,o.regis,o.core_id order by o.datemonth,o.ma_tinh,o.core_id";

   

        Dataset<Row> dataSet = spark.sql(sqlinchadmh);

//        System.out.println("dataSet.count() :" + dataSet.count());
        writeDataToOracle(dataSet, "report_charge");

        return dataSet;

    }

    public static void writeDataToOracle(Dataset<Row> dataset, String table) {

        dropTableIfExits(table);
        Properties properties = new Properties();
        properties.setProperty("user", Define.USER_NAME);
        properties.setProperty("password", Define.PASS_WORD);
        properties.setProperty("driver", Define.ORACLE_DRIVER);

        dataset.write().mode(SaveMode.Overwrite).option("batchsize", 500).jdbc(Define.CONNECTION_URL, table, properties);

        log("End write oracle dev db");
        log("Begin write real oracle dev db");
//        Properties propertiesReal = new Properties();
//        propertiesReal.setProperty("user", Define.USER_NAME_REAL);
//        propertiesReal.setProperty("password", Define.PASS_WORD_REAL);
//        propertiesReal.setProperty("driver", Define.ORACLE_DRIVER_REAL);
//
//        dataset.write().mode(SaveMode.Overwrite).option("batchsize", 500).jdbc(Define.CONNECTION_URL_REAL, table, propertiesReal);

//        log("End write real oracle db");
    }

    public static void dropTableIfExits(String tableName) {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException e) {
            System.err.println("Failed to load driver class " + e.toString());
        }

//        Properties properties = new Properties();
//        properties.setProperty("user", Define.USER_NAME);
//        properties.setProperty("password", Define.PASS_WORD);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:oracle:thin:@10.33.66.226:1521:mytv", "reportbi", "biai360");
        } catch (SQLException e) {
            System.err.println("Connection failed " + e.toString());
        }

        try {
            PreparedStatement delete = connection.prepareStatement(String.format("DROP TABLE %s PURGE", tableName));
            delete.execute();
            delete.close();
            log("Delete table exit success :" + tableName);
        } catch (SQLException e) {
            System.err.println("SQLException :" + e.toString());
            try {
                connection.close();
            } catch (SQLException ex) {

            }
        } finally {
            try {
                connection.close();
            } catch (SQLException ex) {

            }
        }
    }

    public static void backupFromCassandraTable(String[] ipAddress, String usename, String pass, String schema, String table, int num_partitions) throws Exception {
        //connect cluster cassandra
        /*  JavaRDD<CassandraRow> dataCassandra = javaFunctions(jsc)
                .cassandraTable(schema, table)
                .select("msisdn",
                        "birthday",
                        "device",
                        "fullname",
                        "home_accurate")
                .coalesce(num_partitions);
        ///dataCassandra = jsc.parallelize(dataCassandra.take(10000));
        log("Begin count");
        log(dataCassandra.count() + ";");
        
        return;*/

        log("begin backup");
        Cluster cluster = new Cluster.Builder().addContactPoints(ipAddress)
                .withCredentials(usename, pass).build();
        Session session = cluster.connect();
        com.datastax.driver.core.Metadata m = session.getCluster().getMetadata();
        KeyspaceMetadata km = m.getKeyspace(schema);
        TableMetadata tm = km.getTable(table);
        //get all metadata from table
        ArrayList<String> columnName = new ArrayList<String>();
        ArrayList<String> columnType = new ArrayList<String>();
        ArrayList<String> columnMap = new ArrayList<String>();
        if (Define.ORACLE_MAP_COLUMN.length == 0) {
            // columnMap = new String[tm.getColumns().size()];
        } else {
            if (Define.ORACLE_MAP_COLUMN.length == columnName.size()) {
                List<String> list = Arrays.asList(Define.ORACLE_MAP_COLUMN);
                columnMap = new ArrayList<String>(list);
            } else {
                throw new Exception("please check data colum map oracble");
            }
        }
        //create map column oracle
        for (int i = 0; i < tm.getColumns().size(); i++) {
            ColumnMetadata cm = tm.getColumns().get(i);
            String colName = cm.getName().trim();
            if (!colName.contains(" ")) {
                columnName.add(cm.getName().trim());
                columnType.add(cm.getType().toString());
                if (colName.length() > 30) {
                    String[] colNames = colName.split("_");
                    if (colNames.length == 0) {
                        colName = colNames[0].substring(0, 30);
                        columnMap.add(colName);
                    } else {
                        String colNameFinal = "";
                        for (int j = 0; j < colNames.length; j++) {
                            if (j == 0) {
                                colNameFinal += colNames[j];
                            } else {
                                colNameFinal += colNames[j].substring(0, 1);
                            }
                        }
                        if (colNameFinal.length() > 30) {
                            colNameFinal = colNameFinal.substring(0, 30);
                        }
                        columnMap.add(colNameFinal);
                    }
                } else {
                    columnMap.add(colName);
                }
            }
        }
        log(tm.getColumns().size() + ":" + columnMap.size());
        session.close();
        cluster.close();

        if (columnMap.size() != columnName.size()) {
            throw new Exception("columnMap.size() != columnName.size()");
        } else {
            for (int i = 0; i < columnName.size(); i++) {
                log(columnName.get(i) + ":" + columnMap.get(i));
            }
        }
        String[] selectColumn = new String[columnName.size()];
        for (int i = 0; i < columnName.size(); i++) {
            selectColumn[i] = columnName.get(i);
        }
        JavaRDD<CassandraRow> dataCassandra = javaFunctions(jsc)
                .cassandraTable(schema, table)
                .select(selectColumn)
                .coalesce(num_partitions);
        ///dataCassandra = jsc.parallelize(dataCassandra.take(10000));
        log("Begin count");
        log(dataCassandra.count() + ";");
        //create fields oracle
        List<StructField> fields = new ArrayList<>();
        for (int i = 0; i < columnType.size(); i++) {
            if (columnType.get(i).equals("ascii") || columnType.get(i).equals("date") || columnType.get(i).equals("inet") || columnType.get(i).equals("text")
                    || columnType.get(i).equals("time") || columnType.get(i).equals("varchar") || columnType.get(i).equals("timestamp")) {
                StructField field = DataTypes.createStructField(columnMap.get(i), DataTypes.StringType, true);
                fields.add(field);
            } else if (columnType.get(i).equals("counter") || columnType.get(i).equals("int") || columnType.get(i).equals("smallint")
                    || columnType.get(i).equals("tinyint") || columnType.get(i).equals("varint")) {
                StructField field = DataTypes.createStructField(columnMap.get(i), DataTypes.IntegerType, true);
                fields.add(field);
            } else if (columnType.get(i).equals("float")) {
                StructField field = DataTypes.createStructField(columnMap.get(i), DataTypes.FloatType, true);
                fields.add(field);
            } else if (columnType.get(i).equals("double")) {
                StructField field = DataTypes.createStructField(columnMap.get(i), DataTypes.DoubleType, true);
                fields.add(field);
            } else if (columnType.get(i).equals("bigint")) {
                StructField field = DataTypes.createStructField(columnMap.get(i), DataTypes.LongType, true);
                fields.add(field);
            } else if (columnType.get(i).equals("boolean")) {
                StructField field = DataTypes.createStructField(columnMap.get(i), DataTypes.BooleanType, true);
                fields.add(field);
            }
        }
        if (fields.size() != columnType.size()) {
            log(fields.size() + ":" + columnType.size());
            throw new Exception("please check data type");
        }
        StructType schemaField = DataTypes.createStructType(fields);

        JavaRDD<Row> rowRDD = dataCassandra.map(new Function<CassandraRow, Row>() {
            @Override
            public Row call(CassandraRow v1) throws Exception {
                Object[] rowData = new Object[columnType.size()];
                for (int i = 0; i < fields.size(); i++) {
                    StructField field = fields.get(i);
                    if (field.dataType() == DataTypes.StringType) {
                        String data = v1.getString(i);
                        String value = "";
                        if (data != null && !data.isEmpty() && data.length() > 255) {
                            value = data.substring(0, 254);
                        } else {
                            value = (data == null ? "" : data);
                        }
                        rowData[i] = value;
                    } else if (field.dataType() == DataTypes.IntegerType) {
                        rowData[i] = new Integer(v1.getInt(i) == null ? 0 : v1.getInt(i));
                    } else if (field.dataType() == DataTypes.FloatType) {
                        rowData[i] = new Float(v1.getFloat(i) == null ? 0 : v1.getFloat(i));
                    } else if (field.dataType() == DataTypes.DoubleType) {
                        rowData[i] = new Double(v1.getDouble(i) == null ? 0 : v1.getDouble(i));
                    } else if (field.dataType() == DataTypes.LongType) {
                        rowData[i] = new Long(v1.getLong(i) == null ? 0 : v1.getLong(i));
                    } else if (field.dataType() == DataTypes.BooleanType) {
                        rowData[i] = new Boolean(v1.getBoolean(i) == null ? false : v1.getBoolean(i));
                    }
                }
                return RowFactory.create(rowData);
            }
        });

        Dataset<Row> dataset = spark.createDataFrame(rowRDD, schemaField);
        log("Begin write real oracle dev db");
        Properties propertiesReal = new Properties();
        propertiesReal.setProperty("user", Define.USER_NAME_REAL);
        propertiesReal.setProperty("password", Define.PASS_WORD_REAL);
        propertiesReal.setProperty("driver", Define.ORACLE_DRIVER_REAL);

        dataset.write().mode(SaveMode.Overwrite).option("batchsize", 500).jdbc(Define.CONNECTION_URL_REAL, table, propertiesReal);

        log("End write real oracle db");
    }

}
