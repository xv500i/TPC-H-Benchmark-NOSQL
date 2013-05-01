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
import java.sql.Statement;
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
        // 5 regions
        try {
            String insert = "insert into region values (?,?,?);";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 1; i <= 5; i++) {
                ps.setInt(1, i); // [1-5]
                ps.setString(2, GenerationUtility.generateString(32));
                ps.setString(3, GenerationUtility.generateString(80));
                ps.executeQuery();
            }
            ps.close();
        } catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
        // 25 nations
        try {
            String insert = "insert into nation values (?,?,?,?);";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 1; i <= 25; i++) {
                ps.setInt(1, i); // [1-25]
                ps.setString(2, GenerationUtility.generateString(32));
                ps.setInt(3, (i%5)+1);
                ps.setString(4, GenerationUtility.generateString(32));
                ps.executeQuery();
            }
            ps.close();
        } catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
        // 666 part
        try {
            String insert = "insert into part values (?,?,?,?,?,?,?,?,?);";
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
                ps.executeQuery();
            }
            ps.close();
        } catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
        // 33 supplier
        try {
            String insert = "insert into supplier values (?,?,?,?,?,?,?);";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 1; i <= 33; i++) {
                ps.setInt(1, i); // [1-33]
                ps.setString(2, GenerationUtility.generateString(32));
                ps.setString(3, GenerationUtility.generateString(32));
                ps.setInt(4, (i%25)+1);
                ps.setString(5, GenerationUtility.generateString(32));
                ps.setDouble(6, GenerationUtility.generateNumber(6, 2));
                ps.setString(7, GenerationUtility.generateString(52));
                ps.executeQuery();
            }
            ps.close();
        } catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
        // 500 customers
        try {
            String insert = "insert into customer values (?,?,?,?,?,?,?,?);";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 1; i <= 500; i++) {
                ps.setInt(1, i); // [1-500]
                ps.setString(2, GenerationUtility.generateString(32));
                ps.setString(3, GenerationUtility.generateString(32));
                ps.setInt(4, (i%25)+1);
                ps.setString(5, GenerationUtility.generateString(32));
                ps.setDouble(6, GenerationUtility.generateNumber(6, 2));
                ps.setString(7, GenerationUtility.generateString(32));
                ps.setString(8, GenerationUtility.generateString(60));
                ps.executeQuery();
            }
            ps.close();
        } catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
        // 2666 partsupps
        try {
            String insert = "insert into partsups values (?,?,?,?,?);";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 1; i <= 2666; i++) {
                ps.setInt(1, (i%666)+1);
                ps.setInt(2, (i%33)+1);
                ps.setInt(3, GenerationUtility.generateInteger());
                ps.setDouble(4, GenerationUtility.generateNumber(6, 2));
                ps.setString(5, GenerationUtility.generateString(100));
                ps.executeQuery();
            }
            ps.close();
        } catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
        // 5000 orders
        try {
            String insert = "insert into part values (?,?,?,?,?,?,?,?,?);";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 1; i <= 5000; i++) {
                ps.setInt(1, i); // [1-5000]
                ps.setInt(2, (i%500)+1);
                ps.setString(3, GenerationUtility.generateString(32));
                ps.setDouble(4, GenerationUtility.generateNumber(6, 2));
                ps.setDate(5, GenerationUtility.generateDate());
                ps.setString(6, GenerationUtility.generateString(32));
                ps.setString(7, GenerationUtility.generateString(32));
                ps.setInt(8, GenerationUtility.generateInteger());
                ps.setString(9, GenerationUtility.generateString(40));
                ps.executeQuery();
            }
            ps.close();
        } catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
        // 20000 lineitems
        try {
            String insert = "insert into part values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 1; i <= 20000; i++) {
                ps.setInt(1, (i%5000)+1);
                ps.setInt(2, (i%666)+1);
                ps.setInt(3, (i%33)+1);
                ps.setInt(4, GenerationUtility.generateInteger());
                ps.setInt(5, GenerationUtility.generateInteger());
                ps.setDouble(6, GenerationUtility.generateNumber(6, 2));
                ps.setDouble(7, GenerationUtility.generateNumber(6, 2));
                ps.setDouble(8, GenerationUtility.generateNumber(6, 2));
                ps.setString(9, GenerationUtility.generateString(32));
                ps.setString(10, GenerationUtility.generateString(32));
                ps.setDate(11, GenerationUtility.generateDate());
                ps.setDate(12, GenerationUtility.generateDate());
                ps.setDate(13, GenerationUtility.generateDate());
                ps.setString(14, GenerationUtility.generateString(32));
                ps.setString(15, GenerationUtility.generateString(32));
                ps.setString(16, GenerationUtility.generateString(32));
                ps.executeQuery();
            }
            ps.close();
        } catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    protected void secondInsertOperation() {
        // C&P del 1 pero sumant +20000 als indexos
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void doQuery1() {
        try {
            Statement st = connection.createStatement();
            boolean execute = st.execute("");
            if (execute) {
                ResultSet resultSet = st.getResultSet();
                System.out.println("Q1 returned " + resultSet.getFetchSize() + " rows.");
            } else {
                System.out.println("Q1 returned " + 0 + " rows.");
            }
        } catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void doQuery2() {
        try {
            Statement st = connection.createStatement();
            boolean execute = st.execute("");
            if (execute) {
                ResultSet resultSet = st.getResultSet();
                System.out.println("Q1 returned " + resultSet.getFetchSize() + " rows.");
            } else {
                System.out.println("Q1 returned " + 0 + " rows.");
            }
        } catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void doQuery3() {
        try {
            Statement st = connection.createStatement();
            boolean execute = st.execute("");
            if (execute) {
                ResultSet resultSet = st.getResultSet();
                System.out.println("Q1 returned " + resultSet.getFetchSize() + " rows.");
            } else {
                System.out.println("Q1 returned " + 0 + " rows.");
            }
        } catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void doQuery4() {
        try {
            Statement st = connection.createStatement();
            boolean execute = st.execute("");
            if (execute) {
                ResultSet resultSet = st.getResultSet();
                System.out.println("Q1 returned " + resultSet.getFetchSize() + " rows.");
            } else {
                System.out.println("Q1 returned " + 0 + " rows.");
            }
        } catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
