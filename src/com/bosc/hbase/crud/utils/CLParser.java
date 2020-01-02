package com.bosc.hbase.crud.utils;

import org.apache.commons.cli.*;

public class CLParser {

    private Options options;

    public CLParser() {
        this.options = new Options();
        options.addOption("h", "help", false, "Print this usage information.");
        options.addOption("H", "host", true, "HBase host address");
        options.addOption("p", "path", true, "Specify table name to export.");
        options.addOption("t", "table", true, "Specify table name to export.");
        options.addOption("i", "increment", false, "Whether export full or increasingly");
        options.addOption("n", "namespace", true, "Which namespace to load the data");
        options.addOption("s", "seperator", true, "Output seperator.");
        options.addOption("o", "output", true, "Where to save output files.");
        options.addOption("c", "config", true, "Configuration file path.");
        options.addOption("u", "upsert", false, "Upsert data from file.");
        options.addOption("e", "export", false, "Export data to file.");
    }

    public CommandLine parse(String[] args) throws ParseException{
        CommandLineParser parser = new BasicParser();
        return parser.parse(this.options, args);
    }
}
