package com.transitfeeds.gtfs;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class GtfsOptimizer {
    private Connection mConnection;

    public GtfsOptimizer(Connection connection) {
        mConnection = connection;
    }

    public void optimize() throws SQLException {
        updateStopSequence();
        finalize();
    }

    public void finalize() throws SQLException {
        mConnection.setAutoCommit(true);
        
        String[] queries = {
                "DELETE FROM stops WHERE stop_index NOT IN (SELECT DISTINCT stop_index FROM stop_times)",
                "VACUUM",
                "ANALYZE"
        };
        
        Statement st = mConnection.createStatement();

        for (int i = 0; i < queries.length; i++) {
            String query = queries[i];

            System.err.println(query);
            st.executeUpdate(query);
        }
        
        st.close();
        
        System.err.println("DONE");
    }
    
    private void updateStopSequence() throws SQLException {
        Statement st = mConnection.createStatement();
        ResultSet result = st.executeQuery("SELECT trip_index FROM trips");

        PreparedStatement update = mConnection.prepareStatement("UPDATE stop_times SET last_stop = 1 WHERE trip_index = ? AND stop_sequence = (SELECT max(stop_sequence) FROM stop_times WHERE trip_index = ?)");

        int row = 0;
        
        while (result.next()) {
            int tripIndex = result.getInt(1);

            update.setInt(1, tripIndex);
            update.setInt(2, tripIndex);
            
            update.addBatch();
            
            if ((row % 1000) == 0) {
                update.executeBatch();
                System.err.println(String.format("%d", row));
            }
            
            row++;
        }
        
        update.executeBatch();
        mConnection.commit();
        
        st.close();
        update.close();
    }

}
