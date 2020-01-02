package com.bosc.hbase.crud;

import com.bosc.hbase.crud.utils.CLParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws ParseException {
        CLParser clParser = new CLParser();
        CommandLine commandLine = clParser.parse(args);
        String hostAddress = commandLine.hasOption("H")? commandLine.getOptionValue("H"): "127.0.0.1";
        String bufferingNamespace = commandLine.hasOption("n")? commandLine.getOptionValue("n"): "buffering_tables";
        String seperator = commandLine.hasOption("s")? commandLine.getOptionValue("s"): "@!@";
        String configPath = commandLine.hasOption("c")? commandLine.getOptionValue("c"): "file2hbase.conf";
        if(commandLine.hasOption("u")) {
            try {
                if (commandLine.hasOption("p")) {
                    String path = commandLine.getOptionValue("p");
                    Upsert upst = new Upsert(hostAddress, bufferingNamespace, seperator, configPath);
                    upst.process(path);
                } else {
                    System.out.println("Please specify file input path.");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if(commandLine.hasOption("e")) {
            try {
                String outputPath = commandLine.hasOption("o")? commandLine.getOptionValue("o"): "output";
                if (commandLine.hasOption("t")) {
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
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("Please specify whether upsert or export.");
        }
        System.out.println("Process completed.");
    }
}
