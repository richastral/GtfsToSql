package com.transitfeeds.gtfs;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

public class GtfsToSql {

	public static void main(String[] args) throws Exception {
		Options options = new Options();

		options.addOption("g", true, "GTFS Path");
		options.addOption("s", true, "Sqlite Path");

		CommandLineParser parser = new GnuParser();
		CommandLine line = parser.parse(options, args);

		if (!line.hasOption("g")) {
			System.out.println("GTFS path must be specified");
			showHelp(options);
			System.exit(1);
		}
		
		if (!line.hasOption("s")) {
			System.out.println("Sqlite path must be specified");
			showHelp(options);
			System.exit(2);
		}

		String gtfsPath = line.getOptionValue("g");
		String sqlitePath = line.getOptionValue("s");

		File gtfsFile = new File(gtfsPath);
		File sqliteFile = new File(sqlitePath);
		
		GtfsParser gtfs = new GtfsParser(gtfsFile, sqliteFile);
		gtfs.parse();
	}

	public static void showHelp(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("GtfsToSql", options);
	}

}
