package databasetest;

import java.sql.*;
import java.util.LinkedHashMap;
import org.json.simple.*;

public class DatabaseTest {

    public static void main(String[] args) {
        
        DatabaseTest d = new DatabaseTest();        
        d.getJSONData();

    }


    public JSONArray getJSONData(){  

        JSONArray results = null;
        
        Connection conn = null;
        PreparedStatement pstSelect = null, pstUpdate = null;
        ResultSet resultset = null;
        ResultSetMetaData metadata = null;
        
        String query,value;
        String[] headers;
        
        JSONArray records = new JSONArray();
        
        boolean hasResults;
        int resultCount, columnCount;
        
        try {
            
            /* Identify the Server */
            
            String server = ("jdbc:mysql://localhost/p2_test");
            String username = "root";
            String password = "cs310";
            System.out.println("Connecting to " + server + "...");
            
            /* Load the MySQL JDBC Driver */
            
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            
            /* Open Connection */

            conn = DriverManager.getConnection(server, username, password);

            /* Test Connection */
            
            if (conn.isValid(0)) {
                
                /* Connection Open! */
                
                System.out.println("Connected Successfully!");
                
                // Prepare Update Query
                
                query = "SELECT * FROM people";

                pstSelect = conn.prepareStatement(query);
                
                
                
                // Execute Update Query
                System.out.println("Submitting Query..");
                
                hasResults = pstSelect.execute();
                
                System.out.println("Getting Results..");
                
                
                
                while (hasResults || pstSelect.getUpdateCount() != -1){
                
                    if (hasResults){

                        /* Get Meta data */

                        resultset = pstSelect.getResultSet();
                        metadata = resultset.getMetaData();
                        columnCount = metadata.getColumnCount();

                        headers = new String[columnCount - 1];

                        
                        for(int i = 0; i < headers.length; i++){
                            headers[i] = metadata.getColumnLabel(i + 2);

                        }

                        /* Get data, print it */

                        LinkedHashMap data = new LinkedHashMap();
                        
                       
                        while(resultset.next()){

                            /* Loop through columns, print */

                            data = new LinkedHashMap();
                          
                            for(int i = 0; i < headers.length; i++){
                                value = resultset.getString(i + 2);

                                if(resultset.wasNull()){
                                    data.put(headers[i], "");

                                } else {
                                    data.put(headers[i], value);
                                }
                            }
                            records.add(data);
                        }
                        
                    } else {                       
                        resultCount = pstSelect.getUpdateCount();
                        
                        if (resultCount == -1){
                            break;
                        }
                    }
                   
                    /* Check for more data */
                   
                    hasResults = pstSelect.getMoreResults();
                }                
                results = records;           
            }
           
            /* Close connection */
            
            System.out.println();          
            conn.close();           

        } catch (Exception e){
            System.err.println(e.toString());
        }
        
        /* close other databases */
        
        finally {
            
            if (resultset != null) { try { resultset.close(); resultset = null; } catch (Exception e) {} }
            
            if (pstSelect != null) { try { pstSelect.close(); pstSelect = null; } catch (Exception e) {} }
            
            if (pstUpdate != null) { try { pstUpdate.close(); pstUpdate = null; } catch (Exception e) {} }
        }
        
        return results;
    } 
}