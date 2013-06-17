
package org.database.test;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
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
        ORDERED_BY_CUSTOMER,
        MEMBER_OF_ORDER,
        HAS_PARTSUPP
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
            insertRegions(NUM_REGIONS, 1);                              // 5 Regions
            System.out.println(NUM_REGIONS + " inserts region acabats");
            
            tx.success();
        }
        finally {
            tx.finish();
        }
        
        /* PROVA */
        //ExecutionResult result = engine.execute("START prova=node(0,1,2,3,4,5) RETURN prova");
        //System.out.println(result.dumpToString());
        /* FI PROVA */

        tx = graphDB.beginTx();
        try {
            insertNations(NUM_NATIONS, 1, NUM_REGIONS);                 // 25 Nations
            System.out.println(NUM_NATIONS + " inserts nation acabats");
            
            tx.success();
        }
        finally {
            tx.finish();
        }
        
        
        /*insertNations(NUM_NATIONS, 1, NUM_REGIONS);                 // 25 Nations
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
        System.out.println(NUM_LINEITEMS + " inserts lineitem acabats");*/
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
            node.setProperty("R_Name", getRandomString(64/2));
            node.setProperty("R_Comment", getRandomString(160/2));
            node.setProperty("skip", getRandomString(64/2));
        }
    }
    
    private void insertNations(int numInserts, int firstInsertPK, int numRegions) {
        for (int i = 0; i < numInserts; i++) {
            int regionKey = 1 + r.nextInt(numRegions);
            ExecutionResult result = engine.execute("START region=node(*) WHERE region.R_RegionKey! = " + regionKey + " RETURN region");
            Node region = null;
            Iterator<Node> region_column = result.columnAs("region");
            for (Node node : IteratorUtil.asIterable(region_column)) {
                // Only one iteration
                region = node;
            }
            
            Node node = graphDB.createNode();
            node.setProperty("N_NationKey", firstInsertPK + i);
            node.setProperty("N_Name", getRandomString(64/2));
            node.createRelationshipTo(region, RelTypes.MEMBER_OF_REGION);
            node.setProperty("N_Comment", getRandomString(160/2));
            node.setProperty("skip", getRandomString(64/2));
        }
    }
    
    private void insertParts(int numInserts, int firstInsertPK) {
        for (int i = 0; i < numInserts; i++) {
            Node node = graphDB.createNode();
            node.setProperty("P_PartKey", firstInsertPK + i);
            node.setProperty("P_Name", getRandomString(64/2));
            node.setProperty("P_Mfgr", getRandomString(64/2));
            node.setProperty("P_Brand", getRandomString(64/2));
            node.setProperty("P_Type", getRandomString(64/2));
            node.setProperty("P_Size", getRandomInteger());
            node.setProperty("P_Container", getRandomString(64/2));
            node.setProperty("P_RetailPrice", getRandomNumber(13/2, 2));
            node.setProperty("P_Comment", getRandomString(64/2));
            node.setProperty("skip", getRandomString(64/2));
        }   
    }
    
    private void insertSuppliers(int numInserts, int firstInsertPK, int numNations) {
        for (int i = 0; i < numInserts; i++) {
            int nationKey = 1 + r.nextInt(numNations);
            ExecutionResult result = engine.execute("START nation=node(*) WHERE nation.N_NationKey = '" + nationKey + "' RETURN nation");
            Node nation = null;
            Iterator<Node> nation_column = result.columnAs("nation");
            for (Node node : IteratorUtil.asIterable(nation_column)) {
                // Only one iteration
                nation = node;
            }
            
            Node node = graphDB.createNode();
            node.setProperty("S_SuppKey", firstInsertPK + i);
            node.setProperty("S_Name", getRandomString(64/2));
            node.setProperty("S_Address", getRandomString(64/2));
            node.createRelationshipTo(nation, RelTypes.FROM_NATION);
            node.setProperty("S_Phone", getRandomString(18/2));
            node.setProperty("S_AcctBal", getRandomNumber(13/2, 2));
            node.setProperty("S_Comment", getRandomString(105/2));
            node.setProperty("skip", getRandomString(64/2)); 
        }
    }
    
    private void insertCustomers(int numInserts, int firstInsertPK, int numNations) {
        for (int i = 0; i < numInserts; i++) {
            int nationKey = 1 + r.nextInt(numNations);
            ExecutionResult result = engine.execute("START nation=node(*) WHERE nation.N_NationKey = '" + nationKey + "' RETURN nation");
            Node nation = null;
            Iterator<Node> nation_column = result.columnAs("nation");
            for (Node node : IteratorUtil.asIterable(nation_column)) {
                // Only one iteration
                nation = node;
            }
            
            Node node = graphDB.createNode();
            node.setProperty("C_CustKey", firstInsertPK + 1);
            node.setProperty("C_Name", getRandomString(64/2));
            node.setProperty("C_Address", getRandomString(64/2));
            node.createRelationshipTo(nation, RelTypes.FROM_NATION);
            node.setProperty("C_Phone", getRandomString(64/2));
            node.setProperty("C_AcctBal", getRandomNumber(13/2, 2));
            node.setProperty("C_MktSegment", getRandomString(64/2));
            node.setProperty("C_Comment", getRandomString(120/2));
            node.setProperty("skip", getRandomString(64/2));
        }
    }
    
    private void insertPartsupps(int numInserts, int numParts, int numSuppliers) {
        for (int i = 0; i < numInserts; i++) {
            ArrayList<Integer> pk;
            int partKey, supplierKey;
            do {
                pk = new ArrayList<Integer>(2);
                partKey = 1 + r.nextInt(numParts);             
                supplierKey = 1 + r.nextInt(numSuppliers);     
                pk.add(partKey);
                pk.add(supplierKey);
            } while (partsuppPK.contains(pk));
            partsuppPK.add(pk);
            
            ExecutionResult result = engine.execute("START part=node(*) WHERE part.P_PartKey = '" + partKey + "' RETURN part");
            Node part = null;
            Iterator<Node> part_column = result.columnAs("part");
            for (Node node : IteratorUtil.asIterable(part_column)) {
                // Only one iteration
                part = node;
            }
            
            result = engine.execute("START supplier=node(*) WHERE supplier.S_SuppKey = '" + supplierKey + "' RETURN supplier");
            Node supplier = null;
            Iterator<Node> supplier_column = result.columnAs("supplier");
            for (Node node : IteratorUtil.asIterable(supplier_column)) {
                // Only one iteration
                supplier = node;
            }
            
            Node node = graphDB.createNode();
            node.createRelationshipTo(part, RelTypes.IS_PART);
            node.createRelationshipTo(supplier, RelTypes.FROM_SUPPLIER);
            node.setProperty("PS_AvailQty", getRandomInteger());
            node.setProperty("PS_SupplyCost", getRandomNumber(13/2, 2));
            node.setProperty("PS_Comment", getRandomString(200/2));
            node.setProperty("skip", getRandomString(64/2));
        }
    }
    
    private void insertOrders(int numInserts, int firstInsertPK, int numCustomers) {
        for (int i = 0; i < numInserts; i++) {
            int customerKey = 1 + r.nextInt(numCustomers);
            ExecutionResult result = engine.execute("START customer=node(*) WHERE customer.C_CustKey = '" + customerKey + "' RETURN customer");
            Node customer = null;
            Iterator<Node> customer_column = result.columnAs("customer");
            for (Node node : IteratorUtil.asIterable(customer_column)) {
                // Only one iteration
                customer = node;
            }
            
            Node node = graphDB.createNode();
            node.setProperty("O_OrderKey", firstInsertPK + i);
            node.createRelationshipTo(customer, RelTypes.ORDERED_BY_CUSTOMER);
            node.setProperty("O_OrderStatus", getRandomString(64/2));
            node.setProperty("O_TotalPrice", getRandomNumber(13/2, 2));
            node.setProperty("O_OrderDate", getRandomDate());
            node.setProperty("O_OrderPriority", getRandomString(15/2));
            node.setProperty("O_Clerk", getRandomString(64/2));
            node.setProperty("O_ShipPriority", getRandomInteger());
            node.setProperty("O_Comment", getRandomString(80/2));
            node.setProperty("skip", getRandomString(64/2)); 
        }
    }
    
    private void insertLineitems(int numInserts, int numOrders, int numPartsupps) {
        for (int i = 0; i < numInserts; i++) {
            ArrayList<Integer> pk;
            int orderKey, linenumber;
            do {
                pk = new ArrayList<Integer>(2);
                orderKey = 1 + r.nextInt(numOrders);      
                linenumber = r.nextInt(Integer.MAX_VALUE);
                pk.add(orderKey);
                pk.add(linenumber);
            } while (lineitemPK.contains(pk));
            lineitemPK.add(pk);
            int index = r.nextInt(numPartsupps);
            
            ExecutionResult result = engine.execute("START order=node(*) WHERE order.O_OrderKey = '" + orderKey + "' RETURN order");
            Node order = null;
            Iterator<Node> order_column = result.columnAs("order");
            for (Node node : IteratorUtil.asIterable(order_column)) {
                // Only one iteration
                order = node;
            }
            
            result = engine.execute("START partsupp=node(*) MATCH (partsupp)-[r]->(node) " +
                                    "WHERE (type(r) = " + RelTypes.IS_PART + " AND node.P_PartKey = '" + partsuppPK.get(index).get(0) +
                                    "OR (type(r) = " + RelTypes.FROM_SUPPLIER + " AND node.S_SuppKey = '" + partsuppPK.get(index).get(1) + "') RETURN partsupp");
            Node partsupp = null;
            Iterator<Node> partsupp_column = result.columnAs("partsupp");
            for (Node node : IteratorUtil.asIterable(order_column)) {
                // Only one iteration
                order = node;
            }
            
            Node node = graphDB.createNode();
            node.createRelationshipTo(order, RelTypes.MEMBER_OF_ORDER);
            node.createRelationshipTo(partsupp, RelTypes.HAS_PARTSUPP);
            node.setProperty("L_LineNumber", linenumber);
            node.setProperty("L_Quantity", getRandomInteger());
            node.setProperty("L_ExtendedPrice", getRandomNumber(13/2, 2));
            node.setProperty("L_Discount", getRandomNumber(13/2, 2));
            node.setProperty("L_Tax", getRandomNumber(13/2, 2));
            node.setProperty("L_ReturnFlag", getRandomString(64/2));
            node.setProperty("L_LineStatus", getRandomString(64/2));
            node.setProperty("L_ShipDate", getRandomDate());
            node.setProperty("L_CommitDate", getRandomDate());
            node.setProperty("L_ReceiptDate", getRandomDate());
            node.setProperty("L_ShipInstruct", getRandomString(64/2));
            node.setProperty("L_ShipMode", getRandomString(64/2));
            node.setProperty("L_Comment", getRandomString(64/2));
            node.setProperty("skip", getRandomString(64/2));
        }
    }
    
    /* Utility methods for getting randoms without nulls */
    private String getRandomString(int stringLength) {
        String str = null;
        do {
            str = GenerationUtility.generateString(stringLength);
        } while (str == null);
        return str;
    }
    
    private Integer getRandomInteger() {
        Integer i = null;
        do {
            i = GenerationUtility.generateInteger();
        } while (i == null);
        return i;
    }
    
    private Double getRandomNumber(int numDigits, int numDecimals) {
        Double dbl = null;
        do {
            dbl = GenerationUtility.generateNumber(numDigits, numDecimals);
        } while (dbl == null);
        return dbl;
    }
    
    private Date getRandomDate() {
        Date dt = null;
        do {
            dt = GenerationUtility.generateDate();
        } while (dt == null);
        return dt;
    }
    
}