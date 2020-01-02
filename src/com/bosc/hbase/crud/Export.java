package com.bosc.hbase.crud;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import com.bosc.hbase.crud.conf.Config;
import com.bosc.hbase.crud.utils.CLParser;
import com.bosc.hbase.crud.utils.CRUDUtil;
import org.apache.commons.cli.*;

public class Export {

    private String seperator;
    private String outputPath;
    private String bufferingNamespace;
    private String configPath;
    private Config config;
    private CRUDUtil u;

    public Export(String hostAddress, String bufferingNamespace, String seperator, String outputPath, String configPath) throws IOException {
        this.seperator = seperator;
        this.bufferingNamespace = bufferingNamespace;
        this.u = new CRUDUtil(hostAddress);
        this.outputPath = outputPath;
        this.configPath = configPath;
        this.config = new Config(this.configPath);
        if(!u.isNamespaceExists(this.bufferingNamespace)) {
            u.createNamespace(this.bufferingNamespace);
        }
    }

    public String join(String[] strings, int primaryKeyIndex, String primaryKey) {
        StringBuilder sb = new StringBuilder();
        if(strings.length == 1) {
            return strings[0];
        }
        for(int i=0; i<strings.length; i++) {
            sb.append(strings[i]);
            if(i != (strings.length - 1)) {
                sb.append(this.seperator);
            }
        }
        return sb.toString();
    }

    private String[] insert(String[] original, int insertIndex, String value) {
        String[] inserted = new String[original.length + 1];
        int i = 0;
        for(String o: original) {
            if(i == insertIndex) {
                inserted[i] = value;
                i ++;
            }
            inserted[i] = o;
            i ++;
        }
        return inserted;
    }

    public void export(String tableName, String seperator) throws IOException{
        String fileName = this.outputPath + "/" + tableName + ".txt";
        File file = new File(fileName);
        if(!file.exists()) {
            if(file.getParentFile().mkdirs()) {
                System.out.println("New directory created.");
            }
        }
        FileOutputStream fos = new FileOutputStream(file);
        String nsTableName = this.bufferingNamespace + ":" + tableName;
        HashMap<String, List<String>> results = this.u.scanAll(nsTableName);
//        int[] primaryKeyIndex = this.config.getTablePrimaryKeyIndex(tableName);
        for(String rowKey: results.keySet()) {
            String[] row = new String[results.get(rowKey).size()];
            for(String e: results.get(rowKey)) {
                String[] valueSplit = e.split(",");
                String qualifier = valueSplit[valueSplit.length - 2];
                String value = valueSplit[valueSplit.length - 1];
                int index = Integer.parseInt(qualifier.split("col")[qualifier.split("col").length - 1]);
                row[index] = value;
            }
//            row = insert(row, primaryKeyIndex, rowKey);
            String line = this.u.join(row, seperator) + "\n";
            fos.write(line.getBytes());
//            System.out.print(line);
        }
        fos.close();
        generateFlagFile(tableName);
        printInfo("Table " + tableName + " export completed.");
    }

    public void exportIncreasingly(String tableName, String seperator) throws IOException{
        String fileName = this.outputPath + "/" + tableName + ".txt";
        File file = new File(fileName);
        if(!file.exists()) {
            if(file.getParentFile().mkdirs()) {
                System.out.println("New directory created.");
            }
        }
        FileOutputStream fos = new FileOutputStream(file);
        String nsTableName = this.bufferingNamespace + ":" + tableName;
        HashMap<String, List<String>> results = this.u.scanAll(nsTableName);
//        int primaryKeyIndex = this.config.getTablePrimaryKeyIndex(tableName);

//        System.out.println(primaryKeyIndex);
        for(String rowKey: results.keySet()) {
            String[] row = new String[results.get(rowKey).size()];
            boolean isYesterdayUpdated = false;
            for(String e: results.get(rowKey)) {
                String[] valueSplit = e.split(",");
                String ts = valueSplit[0];
                if(isYesterday(ts)) {
                    isYesterdayUpdated = true;
                }
                String qualifier = valueSplit[2];
                String value = valueSplit[3];
                int index = Integer.parseInt(qualifier.split("col")[qualifier.split("col").length - 1]);
                row[index] = value;
            }
            if(isYesterdayUpdated) {
//                row = insert(row, primaryKeyIndex, rowKey);
                String line = this.u.join(row, seperator) + "\n";
                fos.write(line.getBytes());
                System.out.print(line);
            }
        }
        fos.close();
        generateFlagFile(tableName);
        printInfo("Table " + tableName + " increasing export completed.");
    }

    public void printInfo(String info) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date(System.currentTimeMillis());
        String ts = formatter.format(date);
        System.out.println(ts + " - HBase Export: " + info);
    }

    private void generateFlagFile(String tableName) throws IOException{
        //flg文件需要系统名称
        //进hbase需要主键索引号
        String fileName = this.outputPath + "/" + tableName + ".flg";
        File file = new File(fileName);
        if(!file.exists()) {
            if(file.getParentFile().mkdirs()) {
                printInfo("New directory created.");
            }
        } else {
            printInfo("Flag file already exists.");
        }
        FileOutputStream fos = new FileOutputStream(file);
        String[] flgs = {"WIND", tableName, "20191231", "WIND", tableName};
        String line = this.u.join(flgs, "~");
        fos.write(line.getBytes());
        fos.close();
    }

    public boolean isYesterday(String dt) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Calendar current = Calendar.getInstance();
        current.add(Calendar.DAY_OF_YEAR,-1);
        String yesterday = sdf.format(current.getTime());
        return sdf.format(new Date(Long.parseLong(dt))).equals(yesterday);
    }

    public static void main(String[] args) throws IOException{
        try {
            CLParser clParser = new CLParser();
            CommandLine commandLine = clParser.parse(args);
            String hostAddress = commandLine.hasOption("H")? commandLine.getOptionValue("H"): "127.0.0.1";
            String bufferingNamespace = commandLine.hasOption("n")? commandLine.getOptionValue("n"): "buffering_tables";
            String seperator = commandLine.hasOption("s")? commandLine.getOptionValue("s"): "@!@";
            String outputPath = commandLine.hasOption("o")? commandLine.getOptionValue("o"): "output/";
            String configPath = commandLine.hasOption("c")? commandLine.getOptionValue("c"): "file2hbase.conf";

            if(commandLine.hasOption("t")) {
                String tableName = commandLine.getOptionValue("t");
                Export e = new Export(hostAddress, bufferingNamespace, seperator, outputPath, configPath);
                if (commandLine.hasOption("i")) {
                    System.out.println("increasingly");
                    e.exportIncreasingly(tableName, seperator);
                } else {
                    e.export(tableName, seperator);
                }
            } else {
                System.out.println("Table name must be specified.");
            }

        } catch (ParseException e) {
            e.printStackTrace();
        }
//        e.export("xianzhi", "@!@");
//        e.exportIncreasingly("xianzhi", "@!@");
    }
}