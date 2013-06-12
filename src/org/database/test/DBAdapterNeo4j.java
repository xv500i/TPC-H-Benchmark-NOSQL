
package org.database.test;

import java.util.Random;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;


/**
 * @author Natxo Raga Llorens
 */
public class DBAdapterNeo4j extends AbstractDBAdapter {

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
    private final static double SF = (double)NUM_LINEITEMS/(double)LINEITEM_CARDINALITY;
    private final static int NUM_REGIONS = REGION_CARDINALITY;
    private final static int NUM_NATIONS = NATION_CARDINALITY;
    private final static int NUM_PARTS = (int)(PART_CARDINALITY*SF);
    private final static int NUM_SUPPLIERS = (int)(SUPPLIER_CARDINALITY*SF);
    private final static int NUM_CUSTOMERS = (int)(CUSTOMER_CARDINALITY*SF);
    private final static int NUM_PARTSUPPS = (int)(PARTSUPP_CARDINALITY*SF);
    private final static int NUM_ORDERS = (int)(ORDER_CARDINALITY*SF);   
    
    private final static String DB_PATH = "neo4j";
    
    
    private GraphDatabaseService graphDB;
    private static Random r = new Random(System.currentTimeMillis());
    
    
    // Shutdown Hook
    private static void registerShutdownHook(final GraphDatabaseService graphDB) {
        // Registers a shutdown hook for the Neo4j instance so that it shuts down 
        // nicely when the VM exits (even if you "Ctrl-C" the running application).
        Runtime.getRuntime().addShutdownHook(new Thread()
            {
                @Override
                public void run()
                {
                    graphDB.shutdown();
                }
            } 
        );
    }
    
    @Override
    public void connect() {
         graphDB = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
         registerShutdownHook(graphDB);
    }

    @Override
    public void disconnect() {
        graphDB.shutdown();
    }

    @Override
    protected void firstInsertOperation() {
        Transaction tx = graphDB.beginTx();
        try
        {
            // INSERTS HERE
            
            tx.success();
        }
        finally
        {
            tx.finish();
        }
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
    
    
    /* Inserting methods */
    private void insertRegions(int numInserts, int firstInsertPK) {
        for (int i = 0; i < numInserts; i++) {
            Node node = graphDB.createNode();
            node.setProperty("R_RegionKey", firstInsertPK + i);
            node.setProperty("R_Name", GenerationUtility.generateString(64/2));
            node.setProperty("R_Comment", GenerationUtility.generateString(160/2));
            node.setProperty("skip", GenerationUtility.generateString(64/2));
        }
    }
    
    private void insertNations(int numInserts, int firstInsertPK, int numRegions) {
        for (int i = 0; i < numInserts; i++) {
            Node node = graphDB.createNode();
            node.setProperty("N_NationKey", firstInsertPK + i);
            node.setProperty("N_Name", GenerationUtility.generateString(64/2));
            node.setProperty("N_RegionKey", 1 + r.nextInt(numRegions));
            node.setProperty("N_Comment", GenerationUtility.generateString(160/2));
            node.setProperty("skip", GenerationUtility.generateString(64/2));
        }
    }
    
    private void insertParts(int numInserts, int firstInsertPK) {
        for (int i = 0; i < numInserts; i++) {
            Node node = graphDB.createNode();
            node.setProperty("P_PartKey", firstInsertPK + i);
            node.setProperty("P_Name", GenerationUtility.generateString(64/2));
            node.setProperty("P_Mfgr", GenerationUtility.generateString(64/2));
            node.setProperty("P_Brand", GenerationUtility.generateString(64/2));
            node.setProperty("P_Type", GenerationUtility.generateString(64/2));
            node.setProperty("P_Size", GenerationUtility.generateInteger());
            node.setProperty("P_Container", GenerationUtility.generateString(64/2));
            node.setProperty("P_RetailPrice", GenerationUtility.generateNumber(13/2, 2));
            node.setProperty("P_Comment", GenerationUtility.generateString(64/2));
            node.setProperty("skip", GenerationUtility.generateString(64/2));
        }   
    }
    
    private void insertSuppliers(int numInserts, int firstInsertPK, int numNations) {
        for (int i = 0; i < numInserts; i++) {
            Node node = graphDB.createNode();
            node.setProperty("S_SuppKey", firstInsertPK + i);
            node.setProperty("S_Name", GenerationUtility.generateString(64/2));
            node.setProperty("S_Address", GenerationUtility.generateString(64/2));
            node.setProperty("S_NationKey", 1 + r.nextInt(numNations));
            node.setProperty("S_Phone", GenerationUtility.generateString(18/2));
            node.setProperty("S_AcctBal", GenerationUtility.generateNumber(13/2, 2));
            node.setProperty("S_Comment", GenerationUtility.generateString(105/2));
            node.setProperty("skip", GenerationUtility.generateString(64/2)); 
        }
    }
    
    private void insertCustomers(int numInserts, int firstInsertPK, int numNations) {
        for (int i = 0; i < numInserts; i++) {
            Node node = graphDB.createNode();
            node.setProperty("C_CustKey", firstInsertPK + 1);
            node.setProperty("C_Name", GenerationUtility.generateString(64/2));
            node.setProperty("C_Address", GenerationUtility.generateString(64/2));
            node.setProperty("C_NationKey", 1 + r.nextInt(numNations));
            node.setProperty("C_Phone", GenerationUtility.generateString(64/2));
            node.setProperty("C_AcctBal", GenerationUtility.generateNumber(13/2, 2));
            node.setProperty("C_MktSegment", GenerationUtility.generateString(64/2));
            node.setProperty("C_Comment", GenerationUtility.generateString(120/2));
            node.setProperty("skip", GenerationUtility.generateString(64/2));
        }
    }
    
    /*
    private void insertPartsupps(int numInserts, int numParts, int numSuppliers) {
        for (int i = 0; i < numInserts; i++) {
            Node node = graphDB.createNode();
            node.setProperty("PS_PartKey", i);
            node.setProperty("PS_SuppKey", i);
            node.setProperty("PS_AvailQty", i);
            node.setProperty("PS_SupplyCost", i);
            node.setProperty("PS_Comment", i);
            node.setProperty("skip", i);
            
        }
        
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
    }*/
    
}