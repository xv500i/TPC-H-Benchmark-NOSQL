
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
            System.out.println("Done");
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

    private void setPreparedStatementString(PreparedStatement ps, int index, int stringLength) throws SQLException {
        String str = GenerationUtility.generateString(stringLength);
        if (str != null) ps.setString(index, str);
        else ps.setNull(index, java.sql.Types.VARCHAR);
    }
    
    private void setPreparedStatementInteger(PreparedStatement ps, int index) throws SQLException {
        Integer itg = GenerationUtility.generateInteger();
        if (itg != null) ps.setInt(index, itg);
        else ps.setNull(index, java.sql.Types.INTEGER);
    }
    
    private void setPreparedStatementNumber(PreparedStatement ps, int index, int numDigits, int numDecimals) throws SQLException {
        Double dbl = GenerationUtility.generateNumber(numDigits, numDecimals);
        if (dbl != null) ps.setDouble(index, dbl);
        else ps.setNull(index, java.sql.Types.DECIMAL);
    }
    
    private void setPreparedStatementDate(PreparedStatement ps, int index) throws SQLException {
        Date dt = GenerationUtility.generateDate();
        if (dt != null) ps.setDate(index, dt);
        else ps.setNull(index, java.sql.Types.DATE);
    }
    
    @Override
    protected void firstInsertOperation() {
        // 5 Regions
        try {
            String insert = "INSERT INTO region VALUES(?,?,?,?)";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 0; i < NUM_REGIONS; i++) {
                ps.setInt(1, i + 1); // [1,NUM_REGIONS]
                setPreparedStatementString(ps, 2, 64/2);
                setPreparedStatementString(ps, 3, 160/2);
                setPreparedStatementString(ps, 4, 64/2);
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();
        } 
        catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println(NUM_REGIONS + " inserts region acabats");
        // 25 Nations
        try {
            String insert = "INSERT INTO nation VALUES(?,?,?,?,?)";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 0; i < NUM_NATIONS; i++) {
                ps.setInt(1, i + 1); // [1,NUM_NATIONS]
                setPreparedStatementString(ps, 2, 64/2);
                ps.setInt(3, 1 + r.nextInt(NUM_REGIONS));   // FK Region
                setPreparedStatementString(ps, 4, 160/2);
                setPreparedStatementString(ps, 5, 64/2);
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();
        } 
        catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println(NUM_NATIONS + " inserts nation acabats");
        // 666 Parts
        try {
            String insert = "INSERT INTO part VALUES(?,?,?,?,?,?,?,?,?,?)";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 0; i < NUM_PARTS; i++) {
                ps.setInt(1, i + 1); // [1,NUM_PARTS]
                setPreparedStatementString(ps, 2, 64/2);
                setPreparedStatementString(ps, 3, 64/2);
                setPreparedStatementString(ps, 4, 64/2);
                setPreparedStatementString(ps, 5, 64/2);
                setPreparedStatementInteger(ps, 6);
                setPreparedStatementString(ps, 7, 64/2);
                setPreparedStatementNumber(ps, 8, 13/2, 2);
                setPreparedStatementString(ps, 9, 64/2);
                setPreparedStatementString(ps, 10, 64/2);
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();
        } 
        catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println(NUM_PARTS + " inserts part acabats");
        // 33 Suppliers
        try {
            String insert = "INSERT INTO supplier VALUES(?,?,?,?,?,?,?,?)";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 0; i < NUM_SUPPLIERS; i++) {
                ps.setInt(1, i + 1); // [1,NUM_SUPPLIERS]
                setPreparedStatementString(ps, 2, 64/2);
                setPreparedStatementString(ps, 3, 64/2);
                ps.setInt(4, 1 + r.nextInt(NUM_NATIONS));   // FK Nation
                setPreparedStatementString(ps, 5, 18/2);
                setPreparedStatementNumber(ps, 6, 13/2, 2);
                setPreparedStatementString(ps, 7, 105/2);
                setPreparedStatementString(ps, 8, 64/2);
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();
        } 
        catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println(NUM_SUPPLIERS + " inserts supplier acabats");
        // 500 Customers
        try {
            String insert = "INSERT INTO customer VALUES(?,?,?,?,?,?,?,?,?)";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 0; i < NUM_CUSTOMERS; i++) {
                ps.setInt(1, i + 1); // [1,NUM_CUSTOMERS]
                setPreparedStatementString(ps, 2, 64/2);
                setPreparedStatementString(ps, 3, 64/2);
                ps.setInt(4, 1 + r.nextInt(NUM_NATIONS));   // FK Nation
                setPreparedStatementString(ps, 5, 64/2);
                setPreparedStatementNumber(ps, 6, 13/2, 2);
                setPreparedStatementString(ps, 7, 64/2);
                setPreparedStatementString(ps, 8, 120/2);
                setPreparedStatementString(ps, 9, 64/2);
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();
        } 
        catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println(NUM_CUSTOMERS + " inserts customer acabats");
        // 2666 Partsupps
        ArrayList<ArrayList<Integer>> partsuppPK = new ArrayList<ArrayList<Integer>>(NUM_PARTSUPPS);
        try {
            ArrayList<Integer> pk;
            int part, supplier;
            String insert = "INSERT INTO partsupp VALUES(?,?,?,?,?,?)";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 0; i < NUM_PARTSUPPS; i++) {
                do {
                    pk = new ArrayList<Integer>(2);
                    part = 1 + r.nextInt(NUM_PARTS);            // FK Part
                    supplier = 1 + r.nextInt(NUM_SUPPLIERS);    // FK Supplier
                    pk.add(part);
                    pk.add(supplier);
                } while (partsuppPK.contains(pk));
                partsuppPK.add(pk);
                ps.setInt(1, part);
                ps.setInt(2, supplier);
                setPreparedStatementInteger(ps, 3);
                setPreparedStatementNumber(ps, 4, 13/2, 2);
                setPreparedStatementString(ps, 5, 200/2);
                setPreparedStatementString(ps, 6, 64/2);
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();
        } 
        catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println(NUM_PARTSUPPS + " inserts partsupp acabats");
        // 5000 Orders
        try {
            String insert = "INSERT INTO orders VALUES(?,?,?,?,?,?,?,?,?,?)";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 0; i < NUM_ORDERS; i++) {
                ps.setInt(1, i + 1); // [1,NUM_ORDERS]
                ps.setInt(2, 1 + r.nextInt(NUM_CUSTOMERS)); // FK Customer
                setPreparedStatementString(ps, 3, 64/2);
                setPreparedStatementNumber(ps, 4, 13/2, 2);
                setPreparedStatementDate(ps, 5);
                setPreparedStatementString(ps, 6, 15/2);
                setPreparedStatementString(ps, 7, 64/2);
                setPreparedStatementInteger(ps, 8);
                setPreparedStatementString(ps, 9, 80/2);
                setPreparedStatementString(ps, 10, 64/2);
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();
        } 
        catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println(NUM_ORDERS + " inserts order acabats");
        // 20000 Lineitems
        HashSet<ArrayList<Integer>> lineitemPK = new HashSet<ArrayList<Integer>>(NUM_LINEITEMS);
        try {
            ArrayList<Integer> pk;
            int order, linenumber;
            String insert = "INSERT INTO lineitem VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 0; i < NUM_LINEITEMS; i++) {
                do {
                    pk = new ArrayList<Integer>(2);
                    order = 1 + r.nextInt(NUM_ORDERS);  // FK Order
                    linenumber = r.nextInt();
                    pk.add(order);
                    pk.add(linenumber);
                } while (lineitemPK.contains(pk));
                lineitemPK.add(pk);
                ps.setInt(1, order);
                int index = r.nextInt(NUM_PARTSUPPS);   // FK Partsupp
                ps.setInt(2, partsuppPK.get(index).get(0));
                ps.setInt(3, partsuppPK.get(index).get(1));
                ps.setInt(4, linenumber);
                setPreparedStatementInteger(ps, 5);
                setPreparedStatementNumber(ps, 6, 13/2, 2);
                setPreparedStatementNumber(ps, 7, 13/2, 2);
                setPreparedStatementNumber(ps, 8, 13/2, 2);
                setPreparedStatementString(ps, 9, 64/2);
                setPreparedStatementString(ps, 10, 64/2);
                setPreparedStatementDate(ps, 11);
                setPreparedStatementDate(ps, 12);
                setPreparedStatementDate(ps, 13);
                setPreparedStatementString(ps, 14, 64/2);
                setPreparedStatementString(ps, 15, 64/2);
                setPreparedStatementString(ps, 16, 64/2);
                setPreparedStatementString(ps, 17, 64/2);
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();
        } 
        catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println(NUM_LINEITEMS + " inserts lineitem acabats");
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
