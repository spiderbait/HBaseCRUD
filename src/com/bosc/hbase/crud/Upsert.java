package com.bosc.hbase.crud;

import com.bosc.hbase.crud.conf.Config;
import com.bosc.hbase.crud.utils.CLParser;
import com.bosc.hbase.crud.utils.CRUDUtil;
import org.apache.commons.cli.*;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class Upsert {

    private String seperator;
    private String[] familyColumns = {"cols"};
    private String bufferingNamespace;
    private String configPath;
    private Config config;
    private CRUDUtil u;

    public Upsert(String hostAddress, String bufferingNamespace, String seperator, String configPath) throws IOException {
        this.seperator = seperator;
        this.bufferingNamespace = bufferingNamespace;
        this.configPath = configPath;
        this.u = new CRUDUtil(hostAddress);
        this.config = new Config(this.configPath);
        if(!u.isNamespaceExists(this.bufferingNamespace)) {
            u.createNamespace(this.bufferingNamespace);
        }
    }

    public List<String> getFileList(String path) {
        File dir = new File((path));
        List<String> fileList = new ArrayList<String>();
        if(!dir.exists() || !dir.isDirectory()) {
            System.out.println("Specified path does not exist or is not an directory.");
        } else {
            String[] files = dir.list();
            if(files == null) {
                System.out.println("Folder is empty.");
            } else {
                for(int i = 0; i<files.length; i++) {
                    fileList.add(path + "/" + files[i]);
                }
            }
        }
        return fileList;
    }

    public boolean isFileNameValid(String fileName) {
        String[] fileNameSplit = fileName.split("_");

        return true;
    }

    public HashMap<String, String> parsePath(String path) {
        HashMap<String, String> fieldMapping = new HashMap<String, String>();
        String[] pathSplit = path.split("/");
        String fileName = pathSplit[pathSplit.length - 1];
        String[] fileNameSplit = fileName.split("_");
        fieldMapping.put("type", fileNameSplit[0]);
        fieldMapping.put("number", fileNameSplit[1]);
        fieldMapping.put("table", fileNameSplit[2]);
        fieldMapping.put("fixed", fileNameSplit[3]);
        return fieldMapping;
    }

    private void upsertFromFile(String path) {
        InputStream inputStream = null;
        Reader reader = null;
        BufferedReader bufferedReader = null;
        HashMap<String, String> fieldMapping = parsePath(path);
        try{
            File file = new File(path);
            inputStream = new FileInputStream(file);
            reader = new InputStreamReader(inputStream);
            bufferedReader = new BufferedReader(reader);
            String line = null;
            int[] primaryKeyIndices = this.config.getTablePrimaryKeyIndex(fieldMapping.get("table"));
            while ((line = bufferedReader.readLine()) != null) {
                putByLine(fieldMapping.get("table"), line, primaryKeyIndices);
            }
            inputStream.close();
            file.delete();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    private void putByLine(String tableName, String line, int[] primaryKeyIndices) throws IOException{
        String[] lineSplit = line.split(this.seperator);
        String nsTableName = this.bufferingNamespace + ":" + tableName;
        if(!u.isTableExists(nsTableName)) {
            u.createTable(nsTableName, this.familyColumns);
        }
        int i = 0;
        boolean flag = true;
        for(String col: lineSplit) {
            if(this.u.isContains(primaryKeyIndices, i) && flag) {
                flag = false;
                continue;
            }
            String columnName = "col" + i;
            u.putByRow(nsTableName, lineSplit[primaryKeyIndices], this.familyColumns[0], columnName, col);
            i ++;
        }
//        u.putByRow(nsTableName, "primary_key_index", this.familyColumns[0], "primary_key_index", String.valueOf(primaryKeyIndex));
    }

    public void printInfo(String info) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date(System.currentTimeMillis());
        String ts = formatter.format(date);
        System.out.println(ts + " - HBase Upsert: " + info);
    }

    public void process(String path) {
        List<String> fileList = getFileList(path);
        for(String filePath: fileList) {
            upsertFromFile(filePath);
            System.out.println(filePath + " loaded.");
        }
    }

    public static void main(String[] args) throws IOException{
        try {
            CLParser clParser = new CLParser();
            CommandLine commandLine = clParser.parse(args);
            String hostAddress = commandLine.hasOption("H")? commandLine.getOptionValue("H"): "127.0.0.1";
            String bufferingNamespace = commandLine.hasOption("n")? commandLine.getOptionValue("n"): "buffering_tables";
            String seperator = commandLine.hasOption("s")? commandLine.getOptionValue("s"): "@!@";
            String configPath = commandLine.hasOption("c")? commandLine.getOptionValue("c"): "file2hbase.conf";
            if(commandLine.hasOption("p")) {
                String path = commandLine.getOptionValue("p");
                Upsert upst = new Upsert(hostAddress, bufferingNamespace, seperator, configPath);
                upst.process(path);
            } else {
                System.out.println("Please specify file input path: ");
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}