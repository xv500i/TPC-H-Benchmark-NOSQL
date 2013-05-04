
package org.database.test;

import java.sql.*;
import java.math.*;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * @author Alex Soms Batalla
 * @author Natxo Raga Llorens
 */
public class DBAdapterOracle extends AbstractDBAdapter {

    /* Tuples numbers */
    private final static int NUM_REGIONS = 5;
    private final static int NUM_NATIONS = 25;
    private final static int NUM_PARTS = 666;
    private final static int NUM_SUPPLIERS = 33;
    private final static int NUM_CUSTOMERS = 500;
    private final static int NUM_PARTSUPPS = 2666;
    private final static int NUM_ORDERS = 5000;
    private final static int NUM_LINEITEM = 20000;
    
    private static Random r = new Random(System.currentTimeMillis());
    private Connection connection;
    
    @Override
    public void connect() {
        // Database connection data
        String prefix = "jdbc:oracle:thin";
        String hostname = "oraclefib.fib.upc.es";
        String port = "1521";
        String SID = "ORABD";
        String URL = prefix + ":@" + hostname + ":" + port + ":" + SID;
     
        // Properties (user and password) for creating connection to Oracle database
        Properties props = new Properties();
        props.setProperty("user", "natxo.raga");
        props.setProperty("password", "DB040991");
        
        // Create CONNECTION to Oracle database using JDBC
        try {
            connection = DriverManager.getConnection(URL, props);
        } 
        catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(-1);
        }

        // Connection TEST
        try {
            String sql = "SELECT sysdate AS current_day FROM dual";
            PreparedStatement preStatement = connection.prepareStatement(sql);
            ResultSet result = preStatement.executeQuery();
            while (result.next()){
                System.out.println("Current Date from Oracle : " + result.getString("current_day"));
            }
            System.out.println("done");
        } 
        catch (SQLException ex) {
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
        String str;
        ArrayList<Integer> regions = new ArrayList<Integer>(NUM_REGIONS);
        ArrayList<Integer> nations = new ArrayList<Integer>(NUM_NATIONS);
        ArrayList<Integer> parts = new ArrayList<Integer>(NUM_PARTS);
        // 5 Regions
        try {
            String insert = "INSERT INTO region VALUES(?,?,?,?)";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 0; i < NUM_REGIONS; i++) {
                regions.add(GenerationUtility.generateInteger(false));
                ps.setInt(1, regions.get(i));
                if ((str = GenerationUtility.generateString(64/2)) != null) ps.setString(2, str);
                if ((str = GenerationUtility.generateString(160/2)) != null) ps.setString(3, str);
                if ((str = GenerationUtility.generateString(64/2)) != null) ps.setString(4, str);
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();
        } catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
        // 25 Nations
        try {
            String insert = "INSERT INTO nation VALUES(?,?,?,?,?)";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 0; i < NUM_NATIONS; i++) {
                nations.add(GenerationUtility.generateInteger(false));
                ps.setInt(1, nations.get(i));
                if ((str = GenerationUtility.generateString(64/2)) != null) ps.setString(2, str);
                ps.setInt(3, regions.get(r.nextInt(regions.size())));
                if ((str = GenerationUtility.generateString(160/2)) != null) ps.setString(4, str);
                if ((str = GenerationUtility.generateString(64/2)) != null) ps.setString(5, str);
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();
        } catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
        // 666 Parts
        try {
            String insert = "INSERT INTO part VALUES(?,?,?,?,?,?,?,?,?)";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 0; i < NUM_PARTS; i++) {
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
        // 33 Suppliers
        /*try {
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
        // 500 Customers
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
        // 2666 Partsupps
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
        // 5000 Orders
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
        // 20000 Lineitems
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
        }*/
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
            String date = "500-05-27"; // FIXME: must exist
            boolean execute = st.execute(
                "SELECT l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order"
                +"FROM lineitem"
                +"WHERE l_shipdate <= '" + date + "'" 
                +"GROUP BY l_returnflag, l_linestatus"
                +"ORDER BY l_returnflag, l_linestatus;");
            if (execute) {
                ResultSet resultSet = st.getResultSet();
                System.out.println("Q1 returned " + resultSet.getFetchSize() + " rows.");
                resultSet.close();
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
            String size = "2000"; // MUST EXIST
            String type = "A";
            String region = "Qaoxp"; 
            boolean execute = st.execute("SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment"
                +"FROM part, supplier, partsupp, nation, region"
                +"WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = " + size + " AND p_type like '%" + type + "' AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = '" + region + "' AND ps_supplycost = (SELECT min(ps_supplycost) FROM partsupp, supplier, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = '" + region + "')"
                +"ORDER BY s_acctbal desc, n_name, s_name, p_partkey;");
            if (execute) {
                ResultSet resultSet = st.getResultSet();
                System.out.println("Q2 returned " + resultSet.getFetchSize() + " rows.");
                resultSet.close();
            } else {
                System.out.println("Q2 returned " + 0 + " rows.");
            }
        } catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void doQuery3() {
        try {
            Statement st = connection.createStatement();
            String segment = "QpsoiE";
            String date1 = "300-00-00";
            String date2 = "500-6-27";
            boolean execute = st.execute("SELECT l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority"
                +"FROM customer, orders, lineitem"
                +"WHERE c_mktsegment = '" + segment + "' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < '" + date1 + "' AND l_shipdate > '" + date2 + "'"
                +"GROUP BY l_orderkey, o_orderdate, o_shippriority"
                +"ORDER BY revenue desc, o_orderdate;");
            if (execute) {
                ResultSet resultSet = st.getResultSet();
                System.out.println("Q3 returned " + resultSet.getFetchSize() + " rows.");
                resultSet.close();
            } else {
                System.out.println("Q3 returned " + 0 + " rows.");
            }
        } catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void doQuery4() {
        try {
            Statement st = connection.createStatement();
            String region = "QpsoiE";
            String date1 = "300-00-00";
            String date2 = "500-6-27";
            boolean execute = st.execute("SELECT n_name, sum(l_extendedprice * (1 - l_discount)) as revenue"
                +"FROM customer, orders, lineitem, supplier, nation, region"
                +"WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = '" + region + "' AND o_orderdate >= date '" + date1 + "' AND o_orderdate < date '" + date2 + "' + interval '1' year"
                +"GROUP BY n_name"
                +"ORDER BY revenue desc;");
            if (execute) {
                ResultSet resultSet = st.getResultSet();
                System.out.println("Q4 returned " + resultSet.getFetchSize() + " rows.");
                resultSet.close();
            } else {
                System.out.println("Q4 returned " + 0 + " rows.");
            }
        } catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
