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
public class DBAdapterOracle extends AbstractDBAdapter {

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
                System.out.println("Current Date from Oracle : " + result.getString("current_day"));
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
    protected void firstInsertOperation() {
        // 666 part
        try {
            String insert = "insert into part values (?,?,?,?,?,?,?,?,?,?);";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 1; i <= 666; i++) {
                ps.setInt(1, i); // [1-666]
                ps.setString(2, GenerationUtility.generateString(32));
                ps.setString(3, GenerationUtility.generateString(32));
                ps.setString(4, GenerationUtility.generateString(32));
                ps.setString(5, GenerationUtility.generateString(32));
                ps.setInt(6, GenerationUtility.generateInteger());
                ps.setString(7, GenerationUtility.generateString(32));
                ps.setDouble(8, GenerationUtility.generateNumber(6,2));
                ps.setString(9, GenerationUtility.generateString(32));
                ps.setString(10, GenerationUtility.generateString(32));
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
    }

    @Override
    protected void secondInsertOperation() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void doQuery1() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void doQuery2() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void doQuery3() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void doQuery4() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}
