
package org.database.test;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
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
    private final static int NUM_LINEITEMS = 20000;
    
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
        Integer itg;
        Double dbl;
        Date dt;
        // 5 Regions
        try {
            String insert = "INSERT INTO region VALUES(?,?,?,?)";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 0; i < NUM_REGIONS; i++) {
                ps.setInt(1, i + 1); // [1,NUM_REGIONS]
                if ((str = GenerationUtility.generateString(64/2)) != null) ps.setString(2, str);
                if ((str = GenerationUtility.generateString(160/2)) != null) ps.setString(3, str);
                if ((str = GenerationUtility.generateString(64/2)) != null) ps.setString(4, str);
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();
        } 
        catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
        // 25 Nations
        try {
            String insert = "INSERT INTO nation VALUES(?,?,?,?,?)";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 0; i < NUM_NATIONS; i++) {
                ps.setInt(1, i + 1); // [1,NUM_NATIONS]
                if ((str = GenerationUtility.generateString(64/2)) != null) ps.setString(2, str);
                ps.setInt(3, 1 + r.nextInt(NUM_REGIONS));
                if ((str = GenerationUtility.generateString(160/2)) != null) ps.setString(4, str);
                if ((str = GenerationUtility.generateString(64/2)) != null) ps.setString(5, str);
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();
        } 
        catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
        // 666 Parts
        try {
            String insert = "INSERT INTO part VALUES(?,?,?,?,?,?,?,?,?,?)";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 0; i < NUM_PARTS; i++) {
                ps.setInt(1, i + 1); // [1,NUM_PARTS]
                if ((str = GenerationUtility.generateString(64/2)) != null) ps.setString(2, str);
                if ((str = GenerationUtility.generateString(64/2)) != null) ps.setString(3, str);
                if ((str = GenerationUtility.generateString(64/2)) != null) ps.setString(4, str);
                if ((str = GenerationUtility.generateString(64/2)) != null) ps.setString(5, str);
                if ((itg = GenerationUtility.generateInteger()) != null) ps.setInt(6, itg);
                if ((str = GenerationUtility.generateString(64/2)) != null) ps.setString(7, str);
                if ((dbl = GenerationUtility.generateNumber(13/2, 2)) != null) ps.setDouble(8, dbl);
                if ((str = GenerationUtility.generateString(64/2)) != null) ps.setString(9, str);
                if ((str = GenerationUtility.generateString(64/2)) != null) ps.setString(10, str);
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();
        } 
        catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
        // 33 Suppliers
        try {
            String insert = "INSERT INTO supplier VALUES(?,?,?,?,?,?,?,?)";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 0; i < NUM_SUPPLIERS; i++) {
                ps.setInt(1, i + 1); // [1,NUM_SUPPLIERS]
                if ((str = GenerationUtility.generateString(64/2)) != null) ps.setString(2, str);
                if ((str = GenerationUtility.generateString(64/2)) != null) ps.setString(3, str);
                ps.setInt(4, 1 + r.nextInt(NUM_NATIONS));
                if ((str = GenerationUtility.generateString(18/2)) != null) ps.setString(5, str);
                if ((dbl = GenerationUtility.generateNumber(13/2, 2)) != null) ps.setDouble(6, dbl);
                if ((str = GenerationUtility.generateString(105/2)) != null) ps.setString(7, str);
                if ((str = GenerationUtility.generateString(64/2)) != null) ps.setString(8, str);
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();
        } 
        catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
        // 500 Customers
        try {
            String insert = "INSERT INTO customer VALUES(?,?,?,?,?,?,?,?,?)";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 0; i < NUM_CUSTOMERS; i++) {
                ps.setInt(1, i + 1); // [1,NUM_CUSTOMERS]
                if ((str = GenerationUtility.generateString(64/2)) != null) ps.setString(2, str);
                if ((str = GenerationUtility.generateString(64/2)) != null) ps.setString(3, str);
                ps.setInt(4, 1 + r.nextInt(NUM_NATIONS));
                if ((str = GenerationUtility.generateString(64/2)) != null) ps.setString(5, str);
                if ((dbl = GenerationUtility.generateNumber(13/2, 2)) != null) ps.setDouble(6, dbl);
                if ((str = GenerationUtility.generateString(64/2)) != null) ps.setString(7, str);
                if ((str = GenerationUtility.generateString(120/2)) != null) ps.setString(8, str);
                if ((str = GenerationUtility.generateString(64/2)) != null) ps.setString(9, str);
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();
        } 
        catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
        // 2666 Partsupps
        ArrayList<ArrayList<Integer>> partsuppPK = new ArrayList<ArrayList<Integer>>(NUM_PARTSUPPS);
        try {
            int part, supplier;
            ArrayList<Integer> pk = new ArrayList<Integer>(2);
            String insert = "INSERT INTO partsupp VALUES(?,?,?,?,?,?)";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 0; i < NUM_PARTSUPPS; i++) {
                do {
                    pk.clear();
                    part = 1 + r.nextInt(NUM_PARTS);
                    supplier = 1 + r.nextInt(NUM_SUPPLIERS);
                    pk.add(part);
                    pk.add(supplier);
                } while (partsuppPK.contains(pk));
                partsuppPK.add(pk);
                ps.setInt(1, part);
                ps.setInt(2, supplier);
                if ((itg = GenerationUtility.generateInteger()) != null) ps.setInt(3, itg);
                if ((dbl = GenerationUtility.generateNumber(13/2, 2)) != null) ps.setDouble(4, dbl);
                if ((str = GenerationUtility.generateString(200/2)) != null) ps.setString(5, str);
                if ((str = GenerationUtility.generateString(64/2)) != null) ps.setString(6, str);
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();
        } 
        catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
        // 5000 Orders
        try {
            String insert = "INSERT INTO order VALUES(?,?,?,?,?,?,?,?,?,?)";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 0; i < NUM_ORDERS; i++) {
                ps.setInt(1, i + 1); // [1,NUM_ORDERS]
                ps.setInt(2, 1 + r.nextInt(NUM_CUSTOMERS));
                if ((str = GenerationUtility.generateString(64/2)) != null) ps.setString(3, str);
                if ((dbl = GenerationUtility.generateNumber(13/2, 2)) != null) ps.setDouble(4, dbl);
                if ((dt = GenerationUtility.generateDate()) != null) ps.setDate(5, dt);
                if ((str = GenerationUtility.generateString(15/2)) != null) ps.setString(6, str);
                if ((str = GenerationUtility.generateString(64/2)) != null) ps.setString(7, str);
                if ((itg = GenerationUtility.generateInteger()) != null) ps.setInt(8, itg);
                if ((str = GenerationUtility.generateString(80/2)) != null) ps.setString(9, str);
                if ((str = GenerationUtility.generateString(64/2)) != null) ps.setString(10, str);
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();
        } 
        catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
        // 20000 Lineitems
        HashSet<ArrayList<Integer>> lineitemPK = new HashSet<ArrayList<Integer>>(NUM_LINEITEMS);
        try {
            int order, linenumber;
            ArrayList<Integer> pk = new ArrayList<Integer>(2);
            String insert = "INSERT INTO lineitem VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 0; i < NUM_LINEITEMS; i++) {
                do {
                    pk.clear();
                    order = 1 + r.nextInt(NUM_ORDERS);
                    linenumber = r.nextInt();
                    pk.add(order);
                    pk.add(linenumber);
                } while (lineitemPK.contains(pk));
                lineitemPK.add(pk);
                ps.setInt(1, order);
                int index = r.nextInt(NUM_PARTSUPPS);
                ps.setInt(2, partsuppPK.get(index).get(0));
                ps.setInt(3, partsuppPK.get(index).get(1));
                ps.setInt(4, linenumber);
                if ((itg = GenerationUtility.generateInteger()) != null) ps.setInt(5, itg);
                if ((dbl = GenerationUtility.generateNumber(13/2, 2)) != null) ps.setDouble(6, dbl);
                if ((dbl = GenerationUtility.generateNumber(13/2, 2)) != null) ps.setDouble(7, dbl);
                if ((dbl = GenerationUtility.generateNumber(13/2, 2)) != null) ps.setDouble(8, dbl);
                if ((str = GenerationUtility.generateString(64/2)) != null) ps.setString(9, str);
                if ((str = GenerationUtility.generateString(64/2)) != null) ps.setString(10, str);
                if ((dt = GenerationUtility.generateDate()) != null) ps.setDate(11, dt);
                if ((dt = GenerationUtility.generateDate()) != null) ps.setDate(12, dt);
                if ((dt = GenerationUtility.generateDate()) != null) ps.setDate(13, dt);
                if ((str = GenerationUtility.generateString(64/2)) != null) ps.setString(14, str);
                if ((str = GenerationUtility.generateString(64/2)) != null) ps.setString(15, str);
                if ((str = GenerationUtility.generateString(64/2)) != null) ps.setString(16, str);
                if ((str = GenerationUtility.generateString(64/2)) != null) ps.setString(17, str);
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();
        } 
        catch (SQLException ex) {
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
