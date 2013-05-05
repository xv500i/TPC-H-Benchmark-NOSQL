
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

    /* Tables cardinality */
    private final static int REGION_CARDINALITY = 5;
    private final static int NATION_CARDINALITY = 25;
    private final static int PART_CARDINALITY = 200000;
    private final static int SUPPLIER_CARDINALITY = 10000;
    private final static int CUSTOMER_CARDINALITY = 150000;
    private final static int PARTSUPP_CARDINALITY = 800000;
    private final static int ORDER_CARDINALITY = 1500000;
    private final static int LINEITEM_CARDINALITY = 6000000;
    
    /* Num inserts */
    private final static int NUM_LINEITEMS = 20000;
    private final static double SF = NUM_LINEITEMS/LINEITEM_CARDINALITY;
    private final static int NUM_REGIONS = REGION_CARDINALITY;
    private final static int NUM_NATIONS = NATION_CARDINALITY;
    private final static int NUM_PARTS = (int)(PART_CARDINALITY*SF);
    private final static int NUM_SUPPLIERS = (int)(SUPPLIER_CARDINALITY*SF);
    private final static int NUM_CUSTOMERS = (int)(CUSTOMER_CARDINALITY*SF);
    private final static int NUM_PARTSUPPS = (int)(PARTSUPP_CARDINALITY*SF);
    private final static int NUM_ORDERS = (int)(ORDER_CARDINALITY*SF);    
    
    
    /* Partsupp PKs */
    ArrayList<ArrayList<Integer>> partsuppPK = new ArrayList<ArrayList<Integer>>(NUM_PARTSUPPS);
    /* Lineitem PKs */
    HashSet<ArrayList<Integer>> lineitemPK = new HashSet<ArrayList<Integer>>(NUM_LINEITEMS);
    
    /* Needed query parameters */
    Date lineitemShipdate1;
    Integer partSize;
    String partType;
    String regionName1;
    String customerMktsegment;
    Date orderOrderdate1;
    Date lineitemShipdate2;
    String regionName2;
    Date orderOrderdate2;
    Date orderOrderdate3;
    
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
        } 
        catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(-1);
        }
    }    
    
    @Override
    protected void firstInsertOperation() {
        insertRegions(NUM_REGIONS, 1);                              // 5 Regions
        System.out.println(NUM_REGIONS + " inserts region acabats");
        insertNations(NUM_NATIONS, 1, NUM_REGIONS);                 // 25 Nations
        System.out.println(NUM_NATIONS + " inserts nation acabats");
        insertParts(NUM_PARTS, 1);                                  // 666 Parts
        System.out.println(NUM_PARTS + " inserts part acabats");
        insertSuppliers(NUM_SUPPLIERS, 1, NUM_NATIONS);             // 33 Suppliers
        System.out.println(NUM_SUPPLIERS + " inserts supplier acabats");
        insertCustomers(NUM_CUSTOMERS, 1, NUM_NATIONS);             // 500 Customers
        System.out.println(NUM_CUSTOMERS + " inserts customer acabats");
        insertPartsupps(NUM_PARTSUPPS, NUM_PARTS, NUM_SUPPLIERS);   // 2666 Partsupps
        System.out.println(NUM_PARTSUPPS + " inserts partsupp acabats");
        insertOrders(NUM_ORDERS, 1, NUM_CUSTOMERS);                 // 5000 Orders
        System.out.println(NUM_ORDERS + " inserts order acabats");
        insertLineitems(NUM_LINEITEMS, NUM_ORDERS, NUM_PARTSUPPS);  // 20000 Lineitems
        System.out.println(NUM_LINEITEMS + " inserts lineitem acabats");
    }

    @Override
    protected void secondInsertOperation() {
        insertRegions(NUM_REGIONS, NUM_REGIONS + 1);                        // 5 Regions (10 Regions total)
        System.out.println(NUM_REGIONS + " inserts region acabats");
        insertNations(NUM_NATIONS, NUM_NATIONS + 1, NUM_REGIONS*2);         // 25 Nations (50 Nations total)
        System.out.println(NUM_NATIONS + " inserts nation acabats");
        insertParts(NUM_PARTS, 1);                                          // 666 Parts (1332 Parts total)
        System.out.println(NUM_PARTS + " inserts part acabats");
        insertSuppliers(NUM_SUPPLIERS, NUM_SUPPLIERS + 1, NUM_NATIONS*2);   // 33 Suppliers (66 Suppliers total)
        System.out.println(NUM_SUPPLIERS + " inserts supplier acabats");
        insertCustomers(NUM_CUSTOMERS, NUM_CUSTOMERS + 1, NUM_NATIONS*2);   // 500 Customers (1000 Customers total)
        System.out.println(NUM_CUSTOMERS + " inserts customer acabats");
        insertPartsupps(NUM_PARTSUPPS, NUM_PARTS*2, NUM_SUPPLIERS*2);       // 2666 Partsupps (5332 Partsupps total)
        System.out.println(NUM_PARTSUPPS + " inserts partsupp acabats");
        insertOrders(NUM_ORDERS, NUM_ORDERS + 1, NUM_CUSTOMERS*2);          // 5000 Orders (10000 Orders total)
        System.out.println(NUM_ORDERS + " inserts order acabats");
        insertLineitems(NUM_LINEITEMS, NUM_ORDERS*2, NUM_PARTSUPPS*2);      // 20000 Lineitems (40000 Lineitems total)
        System.out.println(NUM_LINEITEMS + " inserts lineitem acabats");
    }
    
    @Override
    public void doQuery1() {
        try {
            Statement st = connection.createStatement();
            st.execute(
                "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price, "
                        + "SUM(l_extendedprice*(1-l_discount)) AS sum_disc_price, SUM(l_extendedprice*(1-l_discount)*(1+l_tax)) AS sum_charge, "
                        + "AVG(l_quantity) AS avg_qty, AVG(l_extendedprice) AS avg_price, AVG(l_discount) AS avg_disc, COUNT(*) as count_order "
              + "FROM lineitem "
              + "WHERE l_shipdate <= '" + getDateString(lineitemShipdate1) + "' " 
              + "GROUP BY l_returnflag, l_linestatus "
              + "ORDER BY l_returnflag, l_linestatus");
        } 
        catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void doQuery2() {
        try {
            Statement st = connection.createStatement();
            st.execute(
                "SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment "
              + "FROM part, supplier, partsupp, nation, region "
              + "WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = " + partSize + " " 
              + "AND p_type like '%" + partType + "' AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey "
              + "AND r_name = '" + regionName1 + "' AND ps_supplycost = "
                  + "(SELECT min(ps_supplycost) "
                  + "FROM partsupp, supplier, nation, region "
                  + "WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey "
                  + "AND n_regionkey = r_regionkey AND r_name = '" + regionName1 + "') "
              + "ORDER BY s_acctbal desc, n_name, s_name, p_partkey");
        } 
        catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void doQuery3() {
        try {
            Statement st = connection.createStatement();
            st.execute(
                "SELECT l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority "
              + "FROM customer, orders, lineitem "
              + "WHERE c_mktsegment = '" + customerMktsegment + "' AND c_custkey = o_custkey AND l_orderkey = o_orderkey "
              + "AND o_orderdate < '" + getDateString(orderOrderdate1) + "' AND l_shipdate > '" + getDateString(lineitemShipdate2) + "' "
              + "GROUP BY l_orderkey, o_orderdate, o_shippriority "
              + "ORDER BY revenue desc, o_orderdate"); 
        } 
        catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void doQuery4() {
        try {
            Statement st = connection.createStatement();
            st.execute(
                "SELECT n_name, sum(l_extendedprice * (1 - l_discount)) as revenue "
              + "FROM customer, orders, lineitem, supplier, nation, region "
              + "WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey "
              + "AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = '" + regionName2 + "' "
              + "AND o_orderdate >= '" + getDateString(orderOrderdate2) + "' AND o_orderdate < '" + getDateString(orderOrderdate3) + "' + INTERVAL '1' YEAR "
              + "GROUP BY n_name "
              + "ORDER BY revenue DESC");
        } 
        catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
    /* Inserting methods */
    private void insertRegions(int numInserts, int firstInsertPK) {
        try {
            String insert = "INSERT INTO region VALUES(?,?,?,?)";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 0; i < numInserts; i++) {
                ps.setInt(1, firstInsertPK + i);            // PK
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
    }
    
    private void insertNations(int numInserts, int firstInsertPK, int numRegions) {
        try {
            String insert = "INSERT INTO nation VALUES(?,?,?,?,?)";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 0; i < numInserts; i++) {
                ps.setInt(1, firstInsertPK + i);            // PK
                setPreparedStatementString(ps, 2, 64/2);
                ps.setInt(3, 1 + r.nextInt(numRegions));    // FK Region
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
    }
    
    /**
     * Obtain randomly the needed parameters by the queries.
     */
    public void obtainQueryParameters() {
        try {
            Statement st;
            ResultSet rs;
            /* Query 1 */
            // LINEITEM SHIPDATE
            while (lineitemShipdate1 == null) {
                st = connection.createStatement();
                st.execute("SELECT l_shipdate FROM (SELECT l_shipdate FROM lineitem ORDER BY dbms_random.value) WHERE rownum = 1");
                rs = st.getResultSet();
                if (rs.next()) lineitemShipdate1 = rs.getDate("l_shipdate");
            }
            
            /* Query 2 */
            // PART SIZE
            while (partSize == null) {
                st = connection.createStatement();
                st.execute("SELECT p_size FROM (SELECT p_size FROM part ORDER BY dbms_random.value) WHERE rownum = 1");
                rs = st.getResultSet();
                if (rs.next()) partSize = rs.getInt("p_size");
            }
            
            // PART TYPE
            while (partType == null) {
                st = connection.createStatement();
                st.execute("SELECT p_type FROM (SELECT p_type FROM part ORDER BY dbms_random.value) WHERE rownum = 1");
                rs = st.getResultSet();
                if (rs.next()) partType = rs.getString("p_type");
            }
            
            // REGION NAME
            while (regionName1 == null) {
                st = connection.createStatement();
                st.execute("SELECT r_name FROM (SELECT r_name FROM region ORDER BY dbms_random.value) WHERE rownum = 1");
                rs = st.getResultSet();
                if (rs.next()) regionName1 = rs.getString("r_name");
            }
            
            /* Query 3 */
            // ORDER ORDERDATE
            while (orderOrderdate1 == null) {
                st = connection.createStatement();
                st.execute("SELECT o_orderdate FROM (SELECT o_orderdate FROM orders ORDER BY dbms_random.value) WHERE rownum = 1");
                rs = st.getResultSet();
                if (rs.next()) orderOrderdate1 = rs.getDate("o_orderdate");
            }
            
            // LINEITEM SHIPDATE
            while (lineitemShipdate2 == null) {
                st = connection.createStatement();
                st.execute("SELECT l_shipdate FROM (SELECT l_shipdate FROM lineitem ORDER BY dbms_random.value) WHERE rownum = 1");
                rs = st.getResultSet();
                if (rs.next()) lineitemShipdate2 = rs.getDate("l_shipdate");
            }
            
            // CUSTOMER MKTSEGMENT
            while (customerMktsegment == null) {
                st = connection.createStatement();
                st.execute("SELECT c_mktsegment FROM (SELECT c_mktsegment FROM customer ORDER BY dbms_random.value) WHERE rownum = 1");
                rs = st.getResultSet();
                if (rs.next()) customerMktsegment = rs.getString("c_mktsegment");
            }
            
            /* Query 4 */
            // REGION NAME
            while (regionName2 == null) {
                st = connection.createStatement();
                st.execute("SELECT r_name FROM (SELECT r_name FROM region ORDER BY dbms_random.value) WHERE rownum = 1");
                rs = st.getResultSet();
                if (rs.next()) regionName2 = rs.getString("r_name");
            }
            
            // ORDER ORDERDATE
            while (orderOrderdate2 == null || orderOrderdate3 == null) {
                st = connection.createStatement();
                st.execute("SELECT o_orderdate FROM (SELECT o_orderdate FROM orders ORDER BY dbms_random.value) WHERE rownum <= 2");
                rs = st.getResultSet();
                if (rs.next()) orderOrderdate2 = rs.getDate("o_orderdate");
                if (rs.next()) orderOrderdate3 = rs.getDate("o_orderdate");
            }
            if (orderOrderdate2.after(orderOrderdate3)) {
                Date auxDate = new Date(orderOrderdate2.getTime());
                orderOrderdate2 = orderOrderdate3;
                orderOrderdate3 = auxDate;
            }
        }
        catch (SQLException ex) {
            Logger.getLogger(DBAdapterOracle.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    private void insertParts(int numInserts, int firstInsertPK) {
        try {
            String insert = "INSERT INTO part VALUES(?,?,?,?,?,?,?,?,?,?)";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 0; i < numInserts; i++) {
                ps.setInt(1, firstInsertPK + i);            // PK
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
    }
    
    private void insertSuppliers(int numInserts, int firstInsertPK, int numNations) {
        try {
            String insert = "INSERT INTO supplier VALUES(?,?,?,?,?,?,?,?)";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 0; i < numInserts; i++) {
                ps.setInt(1, firstInsertPK + i);            // PK
                setPreparedStatementString(ps, 2, 64/2);
                setPreparedStatementString(ps, 3, 64/2);
                ps.setInt(4, 1 + r.nextInt(numNations));    // FK Nation
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
    }
    
    private void insertCustomers(int numInserts, int firstInsertPK, int numNations) {
        try {
            String insert = "INSERT INTO customer VALUES(?,?,?,?,?,?,?,?,?)";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 0; i < numInserts; i++) {
                ps.setInt(1, firstInsertPK + i);            // PK
                setPreparedStatementString(ps, 2, 64/2);
                setPreparedStatementString(ps, 3, 64/2);
                ps.setInt(4, 1 + r.nextInt(numNations));    // FK Nation
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
    }
    
    private void insertPartsupps(int numInserts, int numParts, int numSuppliers) {
        try {
            ArrayList<Integer> pk;
            int part, supplier;
            String insert = "INSERT INTO partsupp VALUES(?,?,?,?,?,?)";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 0; i < numInserts; i++) {
                do {
                    pk = new ArrayList<Integer>(2);
                    part = 1 + r.nextInt(numParts);             
                    supplier = 1 + r.nextInt(numSuppliers);     
                    pk.add(part);
                    pk.add(supplier);
                } while (partsuppPK.contains(pk));
                partsuppPK.add(pk);
                ps.setInt(1, part);                             // PK (and FK Part)
                ps.setInt(2, supplier);                         // PK (and FK Supplier)
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
    }
    
    private void insertOrders(int numInserts, int firstInsertPK, int numCustomers) {
        try {
            String insert = "INSERT INTO orders VALUES(?,?,?,?,?,?,?,?,?,?)";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 0; i < numInserts; i++) {
                ps.setInt(1, firstInsertPK + i);            // PK   
                ps.setInt(2, 1 + r.nextInt(numCustomers));  // FK Customer
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
    }
    
    private void insertLineitems(int numInserts, int numOrders, int numPartsupps) {
        try {
            ArrayList<Integer> pk;
            int order, linenumber;
            String insert = "INSERT INTO lineitem VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            PreparedStatement ps = connection.prepareStatement(insert);
            for (int i = 0; i < numInserts; i++) {
                do {
                    pk = new ArrayList<Integer>(2);
                    order = 1 + r.nextInt(numOrders);      
                    linenumber = r.nextInt(Integer.MAX_VALUE);
                    pk.add(order);
                    pk.add(linenumber);
                } while (lineitemPK.contains(pk));
                lineitemPK.add(pk);
                ps.setInt(1, order);                        // PK (and FK Order)
                int index = r.nextInt(numPartsupps);        // FK Partsupp
                ps.setInt(2, partsuppPK.get(index).get(0));
                ps.setInt(3, partsuppPK.get(index).get(1));
                ps.setInt(4, linenumber);                   // PK
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
    }
    
    /* Utility methods for adding randoms to preparedStatement */
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
    
    /* Utility method for converting a date to a Oracle-like format date string */
    private String getDateString(Date date) {
        String dateString = date.toString();
        String[] dateParts = dateString.split("-");
        return dateParts[2] + "/" + dateParts[1] + "/" + dateParts[0];
    }
    
}
