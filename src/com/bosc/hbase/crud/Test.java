package com.bosc.hbase.crud;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.commons.cli.*;

public class Test {
    public static void main(String[] args) throws IOException {
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

        try {
            CommandLineParser parser = new BasicParser();
            Options options = new Options();
            options.addOption("h", "help", false, "Print this usage information.");
            options.addOption("f", "full", false, "Whether export full or zengliang");
            CommandLine commandLine = parser.parse(options, args);
            if(options.hasOption("f")) {
                System.out.println("zengliang");
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }


    }
}
