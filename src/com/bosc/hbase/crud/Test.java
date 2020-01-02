package com.bosc.hbase.crud;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.commons.cli.*;

public class Test {

    public String transferLongToDate(String dateFormat, Long millSec) {
        SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
        Date date = new Date(millSec);
        return sdf.format(date);
    }

    public boolean isYesterday(String yyyyMMdd) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Calendar current = Calendar.getInstance();
        current.add(Calendar.DAY_OF_YEAR,-1);
        String yesterday = sdf.format(current.getTime());
        return yyyyMMdd.equals(yesterday);
    }

    public static void main(String[] args) throws IOException, ParseException {
//        CRUDUtils u = new CRUDUtils("127.0.0.1");
//        u.createNamespace("buffering_tables");
//        String[] c = {"rows"};
//        u.putByRow("test:supertest", "iamgood", "rows", "col1", "neverhappen");
//        u.scanAll("buffering_tables:xianzhi");
//        u.getByKey("test:supertest", "iamgood");
//        u.printInfo("success");
        //2019-12-27 13:57:25 - HBase: rowKey: iamgood, timestamp: 1577426139960, family: rows, qualifier: iamgood, value: ifyoucould
        //2019-12-27 14:00:58 - HBase: rowKey: iamgood, timestamp: 1577426458045, family: rows, qualifier: iamgood, value: neverhappen

//        String a = "col5";
//        String pattern = "[0-9]";
//        Pattern p = Pattern.compile(pattern);
//        Matcher m = p.matcher(a);
//
//        System.out.println(m.group());

//        try {
//            CommandLineParser parser = new BasicParser();
//            Options options = new Options();
//            options.addOption("h", "help", false, "Print this usage information.");
//            options.addOption("f", "full", false, "Whether export full or zengliang");
//            CommandLine commandLine = parser.parse(options, args);
//            if(commandLine.hasOption("f")) {
//                System.out.println("zengliang");
//            }
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }


//        Test t = new Test();
//        System.out.println(t.transferLongToDate("yyyyMMdd", System.currentTimeMillis()));
//        System.out.println(t.transferLongToDate("yyyyMMdd", Long.valueOf("1577693311634")));
//        System.out.println(System.currentTimeMillis());
//
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
//        Calendar current = Calendar.getInstance();
////        rightNow.setTime(new Date(System.currentTimeMillis()));
//        current.add(Calendar.DAY_OF_YEAR,-1);//日期加1天
//        Date dt1 = current.getTime();
//        String reStr = sdf.format(dt1);
//        System.out.println(reStr);

        int[] indices = {};
        System.out.println(indices.length);
        indices[1] = 2;
        System.out.println(indices.length);
    }
}
