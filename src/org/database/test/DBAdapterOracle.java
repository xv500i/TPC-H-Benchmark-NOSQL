/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.database.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Alex
 */
public class DBAdapterOracle implements IDBAdapter {

    private Connection connection;
    
    @Override
    public void connect() {
        //URL of Oracle database server
        // driver@machineName:port:SID
        String url = "jdbc:oracle:thin:@localhost:1632:DEVROOT32";
     
        //properties for creating connection to Oracle database
        Properties props = new Properties();
        props.setProperty("user", "scott");
        props.setProperty("password", "tiger");
     
        //creating connection to Oracle database using JDBC
        try {
            connection = DriverManager.getConnection(url,props);
        } catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(-1);
        }

        String sql ="select sysdate as current_day from dual";

        //creating PreparedStatement object to execute query
        
        try {
            PreparedStatement preStatement = connection.prepareStatement(sql);
            ResultSet result = preStatement.executeQuery();
            while(result.next()){
                System.out.println("Current Date from Oracle : " +         result.getString("current_day"));
            }
            System.out.println("done");
        } catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(-1);
        }
    }

    @Override
    public void disconnect() {
        try {
            connection.close();
        } catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(-1);
        }
    }

    @Override
    public float insertFirstBulk() {
        long initialTime = System.currentTimeMillis();
        // 666 part
        try {
            String insert = "insert into part values (?,?,?,?);";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 0; i < 666; i++) {
                ps.setString(1, "relol");
                ps.setString(2, "relol");
                ps.setString(3, "relol");
                ps.setString(4, "relol");
                ps.executeQuery();
            }
            ps.close();
        } catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
        // 2666 partsupps
        // 20000 lineitems
        // 5000 orders
        // 500 customers
        // 33 supplier
        // 25 nations
        // 5 regions
        long endTime = System.currentTimeMillis();
        return (float)(endTime-initialTime)/1000.0f;
    }

    @Override
    public float insertSecondBulk() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public float executeQuery1() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public float executeQuery2() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public float executeQuery3() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public float executeQuery4() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}
