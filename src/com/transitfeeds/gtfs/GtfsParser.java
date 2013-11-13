package com.transitfeeds.gtfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import org.mozilla.universalchardet.UniversalDetector;

import com.csvreader.CsvReader;

public class GtfsParser {

	private File mGtfsFile;
	private Connection mConnection;

	public GtfsParser(File gtfsFile, Connection connection) throws FileNotFoundException, SQLException, Exception {
		if (!gtfsFile.exists()) {
			throw new FileNotFoundException("GTFS file not found");
		}

		if (!gtfsFile.isDirectory()) {
			throw new Exception("GTFS path must be a directory");
		}

		mGtfsFile = gtfsFile;

		mConnection = connection;
		mConnection.setAutoCommit(false);
	}

	private static String[] SCHEMA = {
			"agency", "agency_id,agency_name,agency_url,agency_timezone,agency_lang,agency_phone,agency_fare_url",
			"stops", "stop_id,stop_code,stop_name,stop_desc,stop_lat,stop_lon,zone_id,stop_url,location_type,parent_station,stop_timezone,wheelchair_boarding",
			"routes", "route_id,agency_id,route_short_name,route_long_name,route_desc,route_type,route_url,route_color,route_text_color",
			"trips", "route_id,service_id,trip_id,trip_headsign,trip_short_name,direction_id,block_id,shape_id,wheelchair_accessible",
			"stop_times", "trip_id,arrival_time,departure_time,stop_id,stop_sequence,stop_headsign,pickup_type,drop_off_type,shape_dist_traveled",
			"calendar", "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date",
			"calendar_dates", "service_id,date,exception_type", 
			"fare_attributes", "fare_id,price,currency_type,payment_method,transfers,transfer_duration", 
			"fare_rules", "fare_id,route_id,origin_id,destination_id,contains_id,", "shapes",
			"shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence,shape_dist_traveled", 
			"frequencies", "trip_id,start_time,end_time,headway_secs,exact_times", 
			"transfers", "from_stop_id,to_stop_id,transfer_type,min_transfer_time", 
			"feed_info", "feed_publisher_name,feed_publisher_url,feed_lang,feed_start_date,feed_end_date,feed_version" 
	};

	private static String[] INDEXES = { 
			"agency", "agency_id", 
			"stops", "stop_id", 
			"routes", "route_id,agency_id",
			"trips", "route_id,service_id,trip_id,shape_id",
			"stop_times", "trip_id,stop_id",
			"calendar", "service_id", 
			"calendar_dates", "service_id", 
			"fare_attributes", "fare_id", 
			"fare_rules", "fare_id", 
			"shapes", "shape_id", 
			"frequencies", "trip_id", 
			"transfers", "from_stop_id,to_stop_id" 
	};

	public void parse() throws SQLException {
		createGtfsTables();

		parseFiles();
		createIndexes();
	}

	private void createGtfsTables() throws SQLException {
		ResultSet tables = mConnection.getMetaData().getTables(null, null, null, null);
		
		Set<String> tableNames = new HashSet<String>();
		
		while (tables.next()) {
			tableNames.add(tables.getString("TABLE_NAME"));
		}
		
		for (int i = 0; i < SCHEMA.length; i += 2) {
			String tableName = SCHEMA[i];
			
			if (tableNames.contains(tableName)) {
				Statement stmt = mConnection.createStatement();
				stmt.execute("DROP TABLE " + tableName);
				stmt.close();

			}
			
			String[] fields = SCHEMA[i + 1].split(",");

			String query = "CREATE TABLE " + tableName + " (";

			for (int j = 0; j < fields.length; j++) {
				if (j > 0) {
					query += ", ";
				}
				query += fields[j] + " TEXT";
			}

			query += ")";

			System.err.println(query);

			Statement stmt = mConnection.createStatement();
			stmt.execute(query);
			stmt.close();
		}

		mConnection.commit();
	}

	private void createIndexes() throws SQLException {
		for (int i = 0; i < INDEXES.length; i += 2) {
			String table = INDEXES[i];
			String[] fields = INDEXES[i + 1].split(",");

			for (int j = 0; j < fields.length; j++) {
				String query = String.format("CREATE INDEX %s_%s ON %s (%s)", table, fields[j], table, fields[j]);
				System.err.println(query);

				Statement stmt = mConnection.createStatement();
				stmt.execute(query);
				stmt.close();
			}
		}

		mConnection.commit();
	}

	private File getFile(String filename) {
		return new File(mGtfsFile.getAbsolutePath() + System.getProperty("file.separator") + filename);
	}

	private CsvReader getCsv(File f) throws FileNotFoundException, IOException {
		byte[] buf = new byte[4096];
	    java.io.FileInputStream fis = new java.io.FileInputStream(f);

	    UniversalDetector detector = new UniversalDetector(null);

	    int nread;
	    while ((nread = fis.read(buf)) > 0 && !detector.isDone()) {
	      detector.handleData(buf, 0, nread);
	    }

	    detector.dataEnd();
	    fis.close();

	    String encoding = detector.getDetectedCharset();
	    Charset charset;
	    if (encoding != null) {
	    	charset = Charset.forName(encoding);
	    } else {
	    	charset = Charset.forName("ISO-8859-1");
	    }

	    detector.reset();
	    
		return new CsvReader(f.getAbsolutePath(), ',', charset);
	}

	private void parseFiles() throws SQLException {
		for (int i = 0; i < SCHEMA.length; i += 2) {
			String[] fields = SCHEMA[i + 1].split(",");

			File f = getFile(SCHEMA[i] + ".txt");

			parseFile(f, SCHEMA[i], fields);
		}
	}

	private String getList(String[] strs) {
		String ret = "";

		for (int i = 0; i < strs.length; i++) {
			if (i > 0) {
				ret += ", ";
			}

			ret += strs[i];
		}

		return ret;
	}

	private String getPlaceholders(int len) {
		String ret = "";

		for (int i = 0; i < len; i++) {
			if (i > 0) {
				ret += ", ";
			}
			ret += "?";
		}

		return ret;
	}

	private void parseFile(File f, String table, String[] fields) {
		System.err.println("Parsing " + f.getAbsolutePath());

		try {
			CsvReader csv = getCsv(f);

			csv.readHeaders();

			String query = String.format("INSERT INTO %s (%s) VALUES (%s)", table, getList(fields),
					getPlaceholders(fields.length));

			PreparedStatement insert = mConnection.prepareStatement(query);

			int numFields = fields.length;

			int[] indexes = new int[numFields];

			String[] headers = csv.getHeaders();
			
			

			for (int i = 0; i < numFields; i++) {
				String col = fields[i];
				indexes[i] = -1;
				
				// contains is used in case there's an invalid byte sequence at the start of a file
				for (int j = 0; j < headers.length; j++) {
					if (headers[j].contains(col)) {
						indexes[i] = j;
						break;
					}
				}
			}

			int row = 0;

			while (csv.readRecord()) {
				int i;

				for (i = 0; i < numFields; i++) {
					insert.setString(i + 1, csv.get(indexes[i]));
				}

				insert.addBatch();

				if ((row % 1000) == 0) {
					insert.executeBatch();
				}
				
				row++;
			}

			insert.executeBatch();

			mConnection.commit();
			insert.close();
		} catch (SQLException se) {
		} catch (FileNotFoundException fnfe) {
		} catch (IOException ioe) {
		}
	}
}
