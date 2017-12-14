/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.vnpt.utils;

import java.util.ArrayList;
import java.util.List;
import scala.Serializable;

import java.util.List;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang.builder.ToStringBuilder;

public class ConfigClass implements Serializable {

    @SerializedName("sparkMaster")
    @Expose
    public String sparkMaster;

    @SerializedName("executor_mem")
    @Expose
    public String executorMem;
    @SerializedName("driver_mem")
    @Expose
    public String driverMem;
    @SerializedName("heartbeatInterval")
    @Expose
    public String heartbeatInterval;
    @SerializedName("coalescePart")
    @Expose
    public Integer coalescePart;
    @SerializedName("appName")
    @Expose
    public String appName;
    @SerializedName("addressFile")
    @Expose
    public String addressFile;
    
    @SerializedName("lacCol")
    @Expose
    public Integer lacCol;
    
    @SerializedName("cellCol")        
    @Expose
    public Integer cellCol;    
    
    @SerializedName("provinceCol")
    @Expose
    public Integer provinceCol;            
    
    @SerializedName("longCol")
    @Expose
    public Integer longCol;
    @SerializedName("latCol")
    @Expose
    public Integer latCol;
    @SerializedName("debugMode")
    @Expose
    public Boolean debugMode;
    @SerializedName("verbose")
    @Expose
    public Boolean verbose;
    @SerializedName("outputPath")
    @Expose
    public String outputPath;
    @SerializedName("hbaseMaster")
    @Expose
    public String hbaseMaster;
    @SerializedName("zookeeperPort")
    @Expose
    public String zookeeperPort;
    @SerializedName("zookeeperQuorum")
    @Expose
    public String zookeeperQuorum;
    @SerializedName("tableName")
    @Expose
    public String tableName;
    @SerializedName("dateBegin")
    @Expose
    public String dateBegin;
    @SerializedName("jars")
    @Expose
    public List<String> jars = null;
    @SerializedName("dateEnd")
    @Expose
    public String dateEnd;
    //
    @SerializedName("cassandraHost")
    @Expose
    public String cassandraHost;
    
    @SerializedName("cassandraPort")
    @Expose
    public String cassandraPort;
    
    @SerializedName("cassandraUsername")
    @Expose
    public String cassandraUsername;
    @SerializedName("cassandraPassword")
    @Expose
    public String cassandraPassword;
    
    @SerializedName("cassandraSchema")
    @Expose
    public String cassandraSchema;
    
    @SerializedName("cassandraTable")  
    @Expose
    public String cassandraTable;
    
    @SerializedName("cassandraTableDay")
    @Expose
    public String cassandraTableDay;
    
    @SerializedName("mappingPath")
    @Expose
    public String mappingPath;
    
    
    @SerializedName("env")
    @Expose
    public String env;
    
    @SerializedName("isProcessDataMonth")
    @Expose
    public Boolean isProcessDataMonth;
    
    @SerializedName("isProcessDataDay")
    @Expose
    public Boolean isProcessDataDay;
    
    @SerializedName("startDateOfMonth")
    @Expose
    public String startDateOfMonth;
    
    @SerializedName("endDateOfMonth")
    @Expose
    public String endDateOfMonth;
    
    @SerializedName("isProcessFinalData")
    @Expose
    public boolean isProcessFinalData;
    
    @SerializedName("monthDataFinal")
    @Expose
    public String monthDataFinal;
    
    @SerializedName("dataFinalTable")
    @Expose
    public String dataFinalTable;
    
    @SerializedName("isUseOldData")
    @Expose
    public boolean isUseOldData;
    
    @SerializedName("pathFileData")
    @Expose
    public List<String> pathFileData;
    
    @SerializedName("warhouseDir")
    @Expose
    public String warehouseDir;
    
    @SerializedName("dataFolder")
    @Expose
    public String dataFolder;
    
    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
