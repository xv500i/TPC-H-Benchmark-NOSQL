/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.database.test;

/**
 *
 * @author Alex
 */
public class Main {
    
    public static void main (String args[]) {
        oracleExecution();
    }
    
    public static void oracleExecution() {
        IDBAdapter adapter = new DBAdapterOracle();
        adapter.connect();
        
        // INSERT FIRST BULK
        float time;
        time = adapter.insertFirstBulk();
        System.out.println("First bulk insert time = " + time);
        
        /*
        // OBTAIN REQUIRED PARAMETERS FOR THE QUERIES
        ((DBAdapterOracle)adapter).obtainQueryParameters();
        
        // QUERY 1
        time = Float.MAX_VALUE;
        for (int i = 0; i < 5; i++) time = Math.min(time, adapter.executeQuery1());
        System.out.println("Query1 time = " + time);
        
        // QUERY 2
        time = Float.MAX_VALUE;
        for (int i = 0; i < 5; i++) time = Math.min(time, adapter.executeQuery2());
        System.out.println("Query2 time = " + time);
        
        // QUERY 3
        time = Float.MAX_VALUE;
        for (int i = 0; i < 5; i++) time = Math.min(time, adapter.executeQuery3());
        System.out.println("Query3 time = " + time);
        
        // QUERY 4
        time = Float.MAX_VALUE;
        for (int i = 0; i < 5; i++) time = Math.min(time, adapter.executeQuery4());
        System.out.println("Query4 time = " + time);*/
        
        // INSERT SECOND BULK
        time = adapter.insertSecondBulk();
        System.out.println("Second bulk insert time = " + time);
    }
    
    public static void mongoDBExecution() {
        IDBAdapter adapter = new DBAdapterMongo();
        adapter.connect();
        float time;
        
        // FIRST BULK
        time = adapter.insertFirstBulk();
        System.out.println("First bulk insert time = " + time);
        int i;
        time = Float.MAX_VALUE;
        for (i = 0; i < 5; i++) {
            time = Math.min(time, adapter.executeQuery1());
        }
        System.out.println("Query1 time = " + time);
        time = Float.MAX_VALUE;
        for (i = 0; i < 5; i++) {
            time = Math.min(time, adapter.executeQuery2());
        }
        System.out.println("Query2 time = " + time);
        time = Float.MAX_VALUE;
        for (i = 0; i < 5; i++) {
            time = Math.min(time, adapter.executeQuery3());
        }
        System.out.println("Query3 time = " + time);
        time = Float.MAX_VALUE;
        for (i = 0; i < 5; i++) {
            time = Math.min(time, adapter.executeQuery4());
        }
        System.out.println("Query4 time = " + time);
        
        // SECOND BULK
        time = adapter.insertSecondBulk();
        System.out.println("Second bulk insert time = " + time);
        time = Float.MAX_VALUE;
        for (i = 0; i < 5; i++) {
            time = Math.min(time, adapter.executeQuery1());
        }
        System.out.println("Query1 time = " + time);
        time = Float.MAX_VALUE;
        for (i = 0; i < 5; i++) {
            time = Math.min(time, adapter.executeQuery2());
        }
        System.out.println("Query2 time = " + time);
        time = Float.MAX_VALUE;
        for (i = 0; i < 5; i++) {
            time = Math.min(time, adapter.executeQuery3());
        }
        System.out.println("Query3 time = " + time);
        time = Float.MAX_VALUE;
        for (i = 0; i < 5; i++) {
            time = Math.min(time, adapter.executeQuery4());
        }
        System.out.println("Query4 time = " + time);
    }
}
