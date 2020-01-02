package com.bosc.hbase.crud.conf;

import java.io.*;

public class Config {
    private String configPath;

    public Config(String configPath) {
        this.configPath = configPath;
    }

    public int[] getTablePrimaryKeyIndex(String tableName) {
        InputStream inputStream = null;
        Reader reader = null;
        BufferedReader bufferedReader = null;
        int[] primaryKeyIndices = new int[0];
        try{
            File file = new File(this.configPath);
            inputStream = new FileInputStream(file);
            reader = new InputStreamReader(inputStream);
            bufferedReader = new BufferedReader(reader);
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                String[] lineSplit = line.split("~");
                String tn = lineSplit[0];
                String i = lineSplit[1];

                if(tn.equals(tableName)) {
                    String[] indices = i.split(",");
                    primaryKeyIndices = new int[indices.length];
                    for(int n=0; n<indices.length; n++) {
                        primaryKeyIndices[n] = Integer.parseInt(indices[n]);
                    }
                    break;
                }
            }
            inputStream.close();
        } catch (IOException e) {
            System.out.println("Configuration file does not exists.");
        }
        return primaryKeyIndices;
    }
}
