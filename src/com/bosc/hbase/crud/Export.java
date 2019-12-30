package com.bosc.hbase.crud;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.cli.*;

public class Export {

    private String seperator;
    private String outputPath;
    private String bufferingNamespace;
    private CRUDUtils u;

    public Export(String hostAddress, String bufferingNamespace, String seperator, String outputPath) throws IOException {
        this.seperator = seperator;
        this.bufferingNamespace = bufferingNamespace;
        this.u = new CRUDUtils(hostAddress);
        this.outputPath = outputPath;
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

    private void export(String tableName, String seperator) throws IOException{
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
        int primaryKeyIndex = 0;
        try {
            primaryKeyIndex = Integer.parseInt(this.u.getByKey(nsTableName, "primary_key_index", "cols", "primary_key_index").get("value"));
        } catch (NullPointerException e) {
            System.out.println("Primary key index does not specified when load into HBase, get primary key index failed.");
        }
//        System.out.println(primaryKeyIndex);
        for(String rowKey: results.keySet()) {
            if(rowKey.equals("primary_key_index")) {
                continue;
            }
            String[] row = new String[results.get(rowKey).size()];
            for(String e: results.get(rowKey)) {
                String[] valueSplit = e.split(",");
                String qualifier = valueSplit[valueSplit.length - 2];
                String value = valueSplit[valueSplit.length - 1];
                int index = Integer.parseInt(qualifier.split("col")[qualifier.split("col").length - 1]);
                row[index] = value;
            }
            row = insert(row, primaryKeyIndex, rowKey);
            String line = this.u.join(row, seperator) + "\n";
            fos.write(line.getBytes());
            System.out.print(line);
        }
        fos.close();
        generateFlagFile(tableName);
        printInfo("Table " + tableName + " export completed.");
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
        System.out.println(line);
        fos.write(line.getBytes());
        fos.close();
    }

    public static void main(String[] args) throws IOException{
        try {
            CommandLineParser parser = new BasicParser();
            Options options = new Options();
            options.addOption("h", "help", false, "Print this usage information.");
            options.addOption("-i", "increment", true, "Whether export full or increasingly");
            CommandLine commandLine = parser.parse(options, args);
            if(commandLine.hasOption("f")) {
                System.out.println("increasingly");
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Export e = new Export("127.0.0.1", "buffering_tables", "@!@", "/Users/tianzhuoli/IdeaProjects/HBaseCRUD/src/com/bosc/hbase/crud/output");
        e.export("xianzhi", "@!@");
    }
}