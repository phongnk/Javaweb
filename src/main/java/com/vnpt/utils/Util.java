/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.vnpt.utils;

import com.google.gson.Gson;
//import com.vnpt.spark.hbase.definition.Coordinate;
//import com.vnpt.spark.hbase.definition.TappingMsg;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import scala.Serializable;

/**
 *
 * @author user001
 */
public class Util implements Serializable
{

    static Pattern numberPattern = Pattern.compile(".*[^0-9].*");

    public static String readNextNonCommentNonEmpty(BufferedReader br) throws IOException
    {
        String line = "";
        while (true)
        {
            line = br.readLine();
            if (line == null)
            {
                return null;
            }
            else if (line.length() < 1)
            {
                continue;
            }
            else if (line.charAt(0) == '/' || line.charAt(0) == '\\' || line.charAt(0) == '#')
            {
                continue;
            }
            else
            {
                return line;
            }
        }
    }

    public static void writeToLocalFile(String file, List list)
    {

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(file, false)))
        {

            for (Object obj : list)
            {
                bw.write(obj.toString() + "\r\n");
            }

            // no need to close it.
            //bw.close();
            System.out.println("Done");

        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public static boolean isNumber(String value)
    {
        boolean result = false;
        if (value == null || value.trim().equals(""))
        {
            return false;
        }
        else
        {
            result = !numberPattern.matcher(value).matches();
        }
        return result;
    }

    public static boolean isWeekend(String ts)
    {
        int year = Integer.parseInt(ts.substring(4, 8));
        int month = Integer.parseInt(ts.substring(2, 4));
        int day = Integer.parseInt(ts.substring(0, 2));
        Calendar cal = new GregorianCalendar(year, month - 1, day);
        int dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
        return (Calendar.SUNDAY == dayOfWeek || Calendar.SATURDAY == dayOfWeek);

    }

    public static boolean isWeekend(Calendar cal)
    {
        int dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
        return (Calendar.SUNDAY == dayOfWeek || Calendar.SATURDAY == dayOfWeek);
    }

    public static String formatMsisdn(String msisdn)
    {
        if ((msisdn != null) && (!msisdn.isEmpty()))
        {
            String sOut;
            msisdn = msisdn.replace("+", "");
            if (msisdn.startsWith("0"))
            {
                sOut = "84" + msisdn.substring(1);
            }
            else if (msisdn.startsWith("84"))
            {
                sOut = msisdn;
            }
            else
            {
                sOut = "84" + msisdn;
            }
            return sOut;
        }
        else
        {
            return null;
        }
    }

    public static double getDistanceFromLatLonInKm(double lat1, double lon1, double lat2, double lon2)
    {

        int R = 6371; // Radius of the earth in km
        double dLat = deg2rad(lat2 - lat1);  // deg2rad below
        double dLon = deg2rad(lon2 - lon1);
        double a
               = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                 + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2))
                   * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double d = R * c; // Distance in km
        return d;
    }

//    public static double getDistanceFromLatLonInKm(TappingMsg add1, TappingMsg add2)
//    {
//        double lat1 = Double.parseDouble(add1.getLat().trim());
//        double lon1 = Double.parseDouble(add1.getLon().trim());
//        double lat2 = Double.parseDouble(add2.getLat().trim());
//        double lon2 = Double.parseDouble(add2.getLon().trim());
//        return getDistanceFromLatLonInKm(lat1, lon1, lat2, lon2);
//    }
//
//    public static double getDistanceFromLatLonInKm(Coordinate add1, Coordinate add2)
//    {
//        double lat1 = add1.getLatitude();
//        double lon1 = add1.getLongitude();
//        double lat2 = add2.getLatitude();
//        double lon2 = add2.getLongitude();
//        return getDistanceFromLatLonInKm(lat1, lon1, lat2, lon2);
//    }

    public static double deg2rad(double deg)
    {
        return deg * (Math.PI / 180);
    }

//    public static void filterError(List<TappingMsg> location_imsi)
//    {
//
//        if (location_imsi != null)
//        {
//
////                for (int i = 0; i < location_imsi.size(); i++)
////                {
////                    try
////                    {
////                        TappingMsg lTemp = location_imsi.get(i);
////                        if (lTemp == null || lTemp.getTimestamp() == null || lTemp.getTimestamp().trim() == "")
////                        {
////                            location_imsi.remove(i);
////                            i--;
////                        }
////                    }
////                    catch (Exception ex)
////                    {
////                        ex.printStackTrace();
////                    }
////
////                }
//            //Collections.sort(location_imsi);
//            System.out.println("Before Filter: " + location_imsi.size());
//            // remove wrong data
//            List<TappingMsg> tmp_imsi = new ArrayList<>();
//            for (int i = 0; i < location_imsi.size(); i++)
//            {
//                TappingMsg obj = location_imsi.get(i);
//                if (obj != null)
//                {
//                    tmp_imsi.add(obj);
//                    String type = obj.getMessage().trim();
//                    if ((type.equals("BSSAP_SRV_REQ")
//                         || type.equals("RANAP_SRV_REQ")
//                         || type.equals("BSSAP_LU_REQ")
//                         || type.equals("RANAP_LU_REQ")
//                         || type.equals("BSSAP_IMSI_DETACH")
//                         || type.equals("BSSAP_IMSI_ATTACH")
//                         || type.equals("RANAP_IMSI_DETACH")
//                         || type.equals("RANAP_IMSI_ATTACH"))
//                        && (!obj.getTmsi().trim().equals("null")))
//                    {
//                        if (tmp_imsi.size() > 1)
//                        {
//                            int size = tmp_imsi.size();
//                            TappingMsg infoTmp = tmp_imsi.get(size - 2);
//                            if (infoTmp != null)
//                            {
//                                if (!infoTmp.getTmsi().equals(obj.getTmsi()))
//                                {
//                                    location_imsi.remove(i);
//                                    i--;
//                                }
//                                else if (i > 0)
//                                {
//
//                                    double distance = Util.getDistanceFromLatLonInKm(obj, location_imsi.get(i - 1));
//
//                                    double period = 0;
//                                    try
//                                    {
//                                        period = Double.parseDouble(obj.getTimestamp()) - Double.parseDouble(location_imsi.get(i - 1).getTimestamp());
//                                        //period = df.parse(obj.getDateTime()).getTime() - df.parse(location_imsi.get(i - 1).getDateTime()).getTime();
//                                    }
//                                    catch (Exception ex)
//                                    {
//                                        System.out.println("Exception: " + ex.getMessage());
//                                    }
//
//                                    period = period / 60000; //minutes
//                                    double velocity = distance * 60 / period; // van toc km/h
//                                    //alert("time1: "+obj.time+",time2: "+ location_imsi[i-1].time+", distance: "+ distance +", period: "+period+", velocity: "+ velocity);
//                                    if (((distance > 10) && (velocity > 50)) || (distance > 20))
//                                    { // khoang cach lon hon 10km, van toc > 100km/h, khoang thoi gian nho hon 60min thi la nghi ngo
//                                        location_imsi.remove(i);
//                                        i--;
//                                    }
//                                    else //caculate distance, time, velocity
//                                    {
//                                        int leng = tmp_imsi.size() - 1;
//                                        if (leng > 0)
//                                        {
//                                            //double distance1 = Common.getDistanceFromLatLonInKm(obj.getAddress(), tmp_imsi.get(leng - 1).getAddress());
//                                            double distance1 = Util.getDistanceFromLatLonInKm(obj, tmp_imsi.get(leng - 1));
//                                            double period1 = 0;
//                                            try
//                                            {
//                                                //period1 = df.parse(obj.getDateTime()).getTime() - df.parse(tmp_imsi.get(leng - 1).getDateTime()).getTime();
//                                                period1 = Double.parseDouble(obj.getTimestamp()) - Double.parseDouble(tmp_imsi.get(leng - 1).getTimestamp());
//                                            }
//                                            catch (Exception ex)
//                                            {
//                                                System.out.println("Exception: " + ex.getMessage());
//                                            }
//
//                                            period1 = period1 / 60000; //minutes
//                                            double velocity1 = distance1 * 60 / period1; // van toc km/h
//
//                                            if (((distance1 > 10) && (velocity1 > 50)) || (distance1 > 20))
//                                            { // khoang cach lon hon 10km, van toc > 100km/h, khoang thoi gian nho hon 60min thi la nghi ngo
//                                                location_imsi.remove(i);
//                                                i--;
//                                            }
//                                        }
//                                    }
//                                }
//                            }
//                            else
//                            {
//
//                            }
//                        }
//                        else
//                        {
//                            location_imsi.remove(i);
//                            i--;
//                        }
//                    }
//                }
//
//            }
//        }
//
//        System.out.println("After Filter: " + location_imsi.size());
//    }

    public static String formatCellLac(String cellLac)
    {
        String out = "";
        int len = cellLac.length();
        int sub = 5 - len;
        for (int i = 0; i < sub; i++)
        {
            out = out + "0";
        }
        out = out + cellLac;
        return out;
    }

    public static double round(double value)
    {
        value = Math.round(value * 100);
        value = value / 100;
        return value;
    }

    public static String getDate()
    {
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
        return df.format(new Date());

    }

    public static void main(String[] args)
    {
        long time = 1498458984699L;
        Calendar cal = new GregorianCalendar();
        cal.setTimeInMillis(time);
        System.out.println(isWeekend(cal));
    }

    public static double findMin(double[] arr)
    {
        double min = 1000000000;
        for (int i = 0; i < arr.length; i++)
        {
            if (arr[i] < min)
            {
                min = arr[i];
            }
        }
        return min;
    }

    public static double findMax(double[] arr)
    {
        double max = -1000000000;
        for (int i = 0; i < arr.length; i++)
        {
            if (arr[i] > max)
            {
                max = arr[i];
            }
        }
        return max;
    }
    
    
    public static int computeCodeData3G(String msisdn, String date){
        String data = "msisdn:" + msisdn + ",date:" + date;
        return data.hashCode();
    }
    
   
}
