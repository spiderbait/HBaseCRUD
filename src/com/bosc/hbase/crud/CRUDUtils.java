package com.bosc.hbase.crud;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class CRUDUtils {

    private Admin admin = null;
    private Connection connection = null;
    private boolean debugFlag;

    public CRUDUtils(String hostAddress, boolean debugFlag) {
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", hostAddress);
            this.connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
            this.debugFlag = debugFlag;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public CRUDUtils(String hostAddress) {
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", hostAddress);
            this.connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
            this.debugFlag = false;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void printInfo(String info) {
        if(this.debugFlag) {
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = new Date(System.currentTimeMillis());
            String ts = formatter.format(date);
            System.out.println(ts + " - HBase Util: " + info);
        }
    }

    public void createNamespace(String namespace) throws IOException{
        try {
            admin.getNamespaceDescriptor(namespace);
        } catch (NamespaceNotFoundException e){
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(namespaceDescriptor);
        }
    }

    public void createTable(String tableName, String[] familyColumns) throws IOException{
        TableName tn = TableName.valueOf(tableName);
        if (admin.tableExists(tn)) {
            printInfo("Table " + tableName + " already exists.");
        } else {
            HTableDescriptor tableDescriptor = new HTableDescriptor(tn);
            for (String familyName: familyColumns) {
                tableDescriptor.addFamily(new HColumnDescriptor(familyName));
            }
            this.admin.createTable(tableDescriptor);
            printInfo("Table " + tableName + " created.");
        }
    }

    public String join(String[] arrays, String seperator) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < arrays.length; i++) {
            sb.append(arrays[i]);
            if (i != arrays.length - 1) {
                sb.append(seperator);
            }
        }
        return sb.toString();
    }

    public HashMap<String, List<String>> scanAll(String tableName) throws IOException{
        HashMap<String, List<String>> resultSet = new HashMap<>();

        Scan scan = new Scan();
        Table table = this.connection.getTable(TableName.valueOf(tableName));
        ResultScanner rs = table.getScanner(scan);
        List<Cell> cells = null;

        for (Result result: rs) {
            cells = result.listCells();
            for (Cell cell: cells) {
                String rowKey = Bytes.toString(CellUtil.cloneRow(cell));  //取行键
                long timestamp = cell.getTimestamp();  //取到时间戳
                String family = Bytes.toString(CellUtil.cloneFamily(cell));  //取到族列
                String qualifier  = Bytes.toString(CellUtil.cloneQualifier(cell));  //取到修饰名
                String value = Bytes.toString(CellUtil.cloneValue(cell));  //取到值

                String values = join(new String[]{String.valueOf(timestamp), family, qualifier, value}, ",");

                if(resultSet.containsKey(rowKey)) {
                    resultSet.get(rowKey).add(values);
                } else {
                    resultSet.put(rowKey, new ArrayList<String>(){{
                        add(values);
                    }});
                }

                printInfo("scanAll - rowKey: " + rowKey + ", timestamp: " +
                        timestamp + ", family: " + family + ", qualifier: " + qualifier + ", value: " + value);
            }
        }
        return resultSet;
    }

    public boolean isKeyExists(String tableName, String keyName) {
        boolean r = false;
        try {
            Table table = this.connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(keyName));
            r = table.exists(get);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return r;
    }

    public boolean isTableExists(String tableName) {
        TableName tn = TableName.valueOf(tableName);
        boolean r = false;
        try {
            r = admin.tableExists(tn);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return r;
    }

    public boolean isNamespaceExists(String namespace) throws IOException{
        try {
            admin.getNamespaceDescriptor(namespace);
            return true;
        } catch (NamespaceNotFoundException e){
            return false;
        }
    }

    public List<HashMap<String, String>> getByKey(String tableName, String keyName) throws IOException{
        List<HashMap<String, String>> returnSet = new ArrayList<HashMap<String, String>>();

        Table table = this.connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(keyName));
        Result r = table.get(get);

        List<Cell> cells = r.listCells();
        if (cells != null) {
            for (Cell cell : cells) {
                String rowKey = Bytes.toString(CellUtil.cloneRow(cell));  //取行键
                long timestamp = cell.getTimestamp();  //取到时间戳
                String family = Bytes.toString(CellUtil.cloneFamily(cell));  //取到族列
                String qualifier  = Bytes.toString(CellUtil.cloneQualifier(cell));  //取到修饰名
                String value = Bytes.toString(CellUtil.cloneValue(cell));  //取到值

                returnSet.add(new HashMap<String, String>(){{
                    put("timestamp", String.valueOf(timestamp));
                    put("family", family);
                    put("qualifier", qualifier);
                    put("value", value);
                }});

                printInfo("rowKey: " + rowKey + ", timestamp: " +
                        timestamp + ", family: " + family + ", qualifier: " + qualifier + ", value: " + value);
            }
        } else {
            printInfo("No data found by key " + keyName + " in table " + tableName);
        }
        return returnSet;
    }

    public HashMap<String, String> getByKey(String tableName, String keyName, String familyName, String qualifierName) throws IOException{
        HashMap<String, String> returnSet = new HashMap<>();
        Table table = this.connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(keyName));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(qualifierName));
        Result r = table.get(get);

        List<Cell> cells = r.listCells();
        for (Cell cell : cells) {
            String rowKey = Bytes.toString(CellUtil.cloneRow(cell));  //取行键
            long timestamp = cell.getTimestamp();  //取到时间戳
            String family = Bytes.toString(CellUtil.cloneFamily(cell));  //取到族列
            String qualifier  = Bytes.toString(CellUtil.cloneQualifier(cell));  //取到修饰名
            String value = Bytes.toString(CellUtil.cloneValue(cell));  //取到值

            returnSet.put("timestamp", String.valueOf(timestamp));
            returnSet.put("family", family);
            returnSet.put("qualifier", qualifier);
            returnSet.put("value", value);

            printInfo("getByKey - rowKey: " + rowKey + ", timestamp: " +
                    timestamp + ", family: " + family + ", qualifier: " + qualifier + ", value: " + value);
        }

        return returnSet;
    }

    public void putByRow(String tableName, String keyName, String familyName, String qualifierName, String value) throws IOException{
        TableName tn = TableName.valueOf(tableName);
        if (this.admin.tableExists(tn)) {
            Put put = new Put(Bytes.toBytes(keyName));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(qualifierName), Bytes.toBytes(value));
            Table table = this.connection.getTable(tn);
            table.put(put);
            printInfo("PUT - rowKey: " + keyName +  ", family: " + familyName + ", qualifier: " + qualifierName + ", value: " + value);
        } else {
            printInfo("Table " + tableName + " not exists");
        }
    }

    public void deleteByKey(String tableName, String keyName, String familyName, String qualifierName) {
        try {
            Delete delete = new Delete(Bytes.toBytes(keyName));
            delete.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(qualifierName));
            Table table = this.connection.getTable(TableName.valueOf(tableName));
            table.delete(delete);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void deleteByKey(String tableName, String keyName) {
        try {
            Delete delete = new Delete(Bytes.toBytes(keyName));
            Table table = this.connection.getTable(TableName.valueOf(tableName));
            table.delete(delete);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void deleteTable(String tableName) {
        TableName tn = TableName.valueOf(tableName);
        try {
            if (admin.tableExists(tn)) {
                admin.disableTable(tn);
                admin.deleteTable(tn);
            } else {
                printInfo("Table " + tableName + "not exists");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
