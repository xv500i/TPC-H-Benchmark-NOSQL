
package org.database.test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.IteratorUtil;


/**
 * @author Natxo Raga Llorens
 */
public class DBAdapterNeo4j extends AbstractDBAdapter {

    private static enum RelTypes implements RelationshipType {
        MEMBER_OF_REGION,
        FROM_NATION,
        IS_PART,
        FROM_SUPPLIER,
        ORDERED_BY_CUSTOMER
    }
    
    
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
    
    
    /* Partsupp PKs */
    ArrayList<ArrayList<Integer>> partsuppPK = new ArrayList<ArrayList<Integer>>(NUM_PARTSUPPS);
    /* Lineitem PKs */
    HashSet<ArrayList<Integer>> lineitemPK = new HashSet<ArrayList<Integer>>(NUM_LINEITEMS);
    
    private GraphDatabaseService graphDB;
    private ExecutionEngine engine;
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
         engine = new ExecutionEngine(graphDB);
    }

    @Override
    public void disconnect() {
        graphDB.shutdown();
    }

    @Override
    protected void firstInsertOperation() {
        Transaction tx = graphDB.beginTx();
        try {
            // INSERTS HERE
            
            tx.success();
        }
        finally {
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
            int regionKey = 1 + r.nextInt(numRegions);
            ExecutionResult result = engine.execute("START region=node(*) where region.R_RegionKey = '" + regionKey + "' RETURN region");
            Node region = null;
            Iterator<Node> region_column = result.columnAs("region");
            for (Node node : IteratorUtil.asIterable(region_column)) {
                // Only one iteration
                region = node;
            }
            
            Node node = graphDB.createNode();
            node.setProperty("N_NationKey", firstInsertPK + i);
            node.setProperty("N_Name", GenerationUtility.generateString(64/2));
            node.createRelationshipTo(region, RelTypes.MEMBER_OF_REGION);
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
            int nationKey = 1 + r.nextInt(numNations);
            ExecutionResult result = engine.execute("START nation=node(*) where nation.N_NationKey = '" + nationKey + "' RETURN nation");
            Node nation = null;
            Iterator<Node> nation_column = result.columnAs("nation");
            for (Node node : IteratorUtil.asIterable(nation_column)) {
                // Only one iteration
                nation = node;
            }
            
            Node node = graphDB.createNode();
            node.setProperty("S_SuppKey", firstInsertPK + i);
            node.setProperty("S_Name", GenerationUtility.generateString(64/2));
            node.setProperty("S_Address", GenerationUtility.generateString(64/2));
            node.createRelationshipTo(nation, RelTypes.FROM_NATION);
            node.setProperty("S_Phone", GenerationUtility.generateString(18/2));
            node.setProperty("S_AcctBal", GenerationUtility.generateNumber(13/2, 2));
            node.setProperty("S_Comment", GenerationUtility.generateString(105/2));
            node.setProperty("skip", GenerationUtility.generateString(64/2)); 
        }
    }
    
    private void insertCustomers(int numInserts, int firstInsertPK, int numNations) {
        for (int i = 0; i < numInserts; i++) {
            int nationKey = 1 + r.nextInt(numNations);
            ExecutionResult result = engine.execute("START nation=node(*) where nation.N_NationKey = '" + nationKey + "' RETURN nation");
            Node nation = null;
            Iterator<Node> nation_column = result.columnAs("nation");
            for (Node node : IteratorUtil.asIterable(nation_column)) {
                // Only one iteration
                nation = node;
            }
            
            Node node = graphDB.createNode();
            node.setProperty("C_CustKey", firstInsertPK + 1);
            node.setProperty("C_Name", GenerationUtility.generateString(64/2));
            node.setProperty("C_Address", GenerationUtility.generateString(64/2));
            node.createRelationshipTo(nation, RelTypes.FROM_NATION);
            node.setProperty("C_Phone", GenerationUtility.generateString(64/2));
            node.setProperty("C_AcctBal", GenerationUtility.generateNumber(13/2, 2));
            node.setProperty("C_MktSegment", GenerationUtility.generateString(64/2));
            node.setProperty("C_Comment", GenerationUtility.generateString(120/2));
            node.setProperty("skip", GenerationUtility.generateString(64/2));
        }
    }
    
    private void insertPartsupps(int numInserts, int numParts, int numSuppliers) {
        for (int i = 0; i < numInserts; i++) {
            int partKey = 1 + r.nextInt(numParts);
            ExecutionResult result = engine.execute("START part=node(*) where part.P_PartKey = '" + partKey + "' RETURN part");
            Node part = null;
            Iterator<Node> part_column = result.columnAs("part");
            for (Node node : IteratorUtil.asIterable(part_column)) {
                // Only one iteration
                part = node;
            }
            
            int supplierKey = 1 + r.nextInt(numSuppliers);
            result = engine.execute("START supplier=node(*) where supplier.S_SuppKey = '" + supplierKey + "' RETURN supplier");
            Node supplier = null;
            Iterator<Node> supplier_column = result.columnAs("supplier");
            for (Node node : IteratorUtil.asIterable(supplier_column)) {
                // Only one iteration
                supplier = node;
            }
            
            Node node = graphDB.createNode();
            node.createRelationshipTo(part, RelTypes.IS_PART);
            node.createRelationshipTo(supplier, RelTypes.FROM_SUPPLIER);
            node.setProperty("PS_AvailQty", GenerationUtility.generateInteger());
            node.setProperty("PS_SupplyCost", GenerationUtility.generateNumber(13/2, 2));
            node.setProperty("PS_Comment", GenerationUtility.generateString(200/2));
            node.setProperty("skip", GenerationUtility.generateString(64/2));
        }
    }
    
    private void insertOrders(int numInserts, int firstInsertPK, int numCustomers) {
        for (int i = 0; i < numInserts; i++) {
            int customerKey = 1 + r.nextInt(numCustomers);
            ExecutionResult result = engine.execute("START customer=node(*) where customer.C_CustKey = '" + customerKey + "' RETURN customer");
            Node customer = null;
            Iterator<Node> customer_column = result.columnAs("customer");
            for (Node node : IteratorUtil.asIterable(customer_column)) {
                // Only one iteration
                customer = node;
            }
            
            Node node = graphDB.createNode();
            node.setProperty("O_OrderKey", firstInsertPK + i);
            node.createRelationshipTo(customer, RelTypes.ORDERED_BY_CUSTOMER);
            node.setProperty("O_OrderStatus", GenerationUtility.generateString(64/2));
            node.setProperty("O_TotalPrice", GenerationUtility.generateNumber(13/2, 2));
            node.setProperty("O_OrderDate", GenerationUtility.generateDate());
            node.setProperty("O_OrderPriority", GenerationUtility.generateString(15/2));
            node.setProperty("O_Clerk", GenerationUtility.generateString(64/2));
            node.setProperty("O_ShipPriority", GenerationUtility.generateInteger());
            node.setProperty("O_Comment", GenerationUtility.generateString(80/2));
            node.setProperty("skip", GenerationUtility.generateString(64/2)); 
        }
    }
    
    private void insertLineitems(int numInserts, int numOrders, int numPartsupps) {
        for (int i = 0; i < numInserts; i++) {
            ArrayList<Integer> pk;
            int order, linenumber;
            do {
                pk = new ArrayList<Integer>(2);
                order = 1 + r.nextInt(numOrders);      
                linenumber = r.nextInt(Integer.MAX_VALUE);
                pk.add(order);
                pk.add(linenumber);
            } while (lineitemPK.contains(pk));
            lineitemPK.add(pk);
            int index = r.nextInt(numPartsupps);
            
            Node node = graphDB.createNode();
            node.setProperty("L_OrderKey", order);
            node.setProperty("L_PartKey", partsuppPK.get(index).get(0));
            node.setProperty("L_SuppKey", partsuppPK.get(index).get(1));
            node.setProperty("L_LineNumber", linenumber);
            node.setProperty("L_Quantity", GenerationUtility.generateInteger());
            node.setProperty("L_ExtendedPrice", GenerationUtility.generateNumber(13/2, 2));
            node.setProperty("L_Discount", GenerationUtility.generateNumber(13/2, 2));
            node.setProperty("L_Tax", GenerationUtility.generateNumber(13/2, 2));
            node.setProperty("L_ReturnFlag", GenerationUtility.generateString(64/2));
            node.setProperty("L_LineStatus", GenerationUtility.generateString(64/2));
            node.setProperty("L_ShipDate", GenerationUtility.generateDate());
            node.setProperty("L_CommitDate", GenerationUtility.generateDate());
            node.setProperty("L_ReceiptDate", GenerationUtility.generateDate());
            node.setProperty("L_ShipInstruct", GenerationUtility.generateString(64/2));
            node.setProperty("L_ShipMode", GenerationUtility.generateString(64/2));
            node.setProperty("L_Comment", GenerationUtility.generateString(64/2));
            node.setProperty("skip", GenerationUtility.generateString(64/2));
        }
    }
    
}