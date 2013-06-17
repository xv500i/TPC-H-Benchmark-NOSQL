
package org.database.test;

import java.util.ArrayList;
import java.util.Date;
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
    
    /* First nodes ID */
    private final static int FIRST_REGION_NODE = 1;
    private final static int FIRST_NATION_NODE = FIRST_REGION_NODE + NUM_REGIONS;
    private final static int FIRST_PART_NODE = FIRST_NATION_NODE + NUM_NATIONS;
    private final static int FIRST_SUPPLIER_NODE = FIRST_PART_NODE + NUM_PARTS;
    private final static int FIRST_CUSTOMER_NODE = FIRST_SUPPLIER_NODE + NUM_SUPPLIERS;
    private final static int FIRST_PARTSUPP_NODE = FIRST_CUSTOMER_NODE + NUM_CUSTOMERS;
    private final static int FIRST_ORDER_NODE = FIRST_PARTSUPP_NODE + NUM_PARTSUPPS;
    private final static int FIRST_LINEITEM_NODE = FIRST_ORDER_NODE + NUM_ORDERS;
    
    private final static String DB_PATH = "neo4j";

    
    /* Partsupp PKs */
    HashSet<ArrayList<Integer>> partsuppPK = new HashSet<ArrayList<Integer>>(NUM_PARTSUPPS);
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
            insertRegions(NUM_REGIONS, 1);                                                                      // 5 Regions
            System.out.println(NUM_REGIONS + " inserts region acabats");
            insertNations(NUM_NATIONS, 1, FIRST_REGION_NODE, NUM_REGIONS);                                      // 25 Nations
            System.out.println(NUM_NATIONS + " inserts nation acabats");
            insertParts(NUM_PARTS, 1);                                                                          // 666 Parts
            System.out.println(NUM_PARTS + " inserts part acabats");
            insertSuppliers(NUM_SUPPLIERS, 1, FIRST_NATION_NODE, NUM_NATIONS);                                  // 33 Suppliers
            System.out.println(NUM_SUPPLIERS + " inserts supplier acabats");
            insertCustomers(NUM_CUSTOMERS, 1, FIRST_NATION_NODE, NUM_NATIONS);                                  // 500 Customers
            System.out.println(NUM_CUSTOMERS + " inserts customer acabats");
            insertPartsupps(NUM_PARTSUPPS, FIRST_PART_NODE, NUM_PARTS, FIRST_SUPPLIER_NODE, NUM_SUPPLIERS);     // 2666 Partsupps
            System.out.println(NUM_PARTSUPPS + " inserts partsupp acabats");
            insertOrders(NUM_ORDERS, 1, FIRST_CUSTOMER_NODE, NUM_CUSTOMERS);                                    // 5000 Orders
            System.out.println(NUM_ORDERS + " inserts order acabats");
            insertLineitems(NUM_LINEITEMS, FIRST_ORDER_NODE, NUM_ORDERS, FIRST_PARTSUPP_NODE, NUM_PARTSUPPS);   // 20000 Lineitems
            System.out.println(NUM_LINEITEMS + " inserts lineitem acabats");
            
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
        ExecutionResult result = engine.execute(
                "START li=node(*) " +
                "WHERE HAS(li.L_LineNumber) " +    
                "RETURN li.L_ReturnFlag, li.L_LineStatus, SUM(li.L_Quantity) AS Sum_Qty, SUM(li.L_ExtendedPrice) AS Sum_Base_Price, " +
                "SUM(li.L_ExtendedPrice*(1 - li.L_Discount)) AS Sum_Disc_Price, SUM(li.L_ExtendedPrice*(1 - li.L_Discount)*(1 + li.L_Tax)) AS Sum_Charge, " +
                "AVG(li.L_Quantity) AS Avg_Qty, AVG(li.L_ExtendedPrice) AS Avg_Price, AVG(li.L_Discount) AS Avg_Disc, COUNT(*) AS Count_Order " +
                "ORDER BY li.L_ReturnFlag, li.L_LineStatus");
        //System.out.println(result.dumpToString());
    }

    @Override
    public void doQuery2() {
        ExecutionResult subResult = engine.execute(
                "START ps=node(*) " +
                "MATCH (ps)-[:FROM_SUPPLIER]->(s)-[:FROM_NATION]->(n)-[:MEMBER_OF_REGION]->(r) " +
                "WHERE r.R_Name = '???' " +
                "RETURN MIN(ps.PS_SupplyCost) AS Min_Cost");
        
        int min_cost = 0;
        Iterator<Integer> min_cost_it = subResult.columnAs("Min_Cost");
        for (Integer cost : IteratorUtil.asIterable(min_cost_it)) {
            min_cost = cost;
        }
        
        ExecutionResult result = engine.execute(
                "START ps=node(*) " +
                "MATCH (p)<-[:IS_PART]-(ps)-[:FROM_SUPPLIER]->(s)-[:FROM_NATION]->(n)-[:MEMBER_OF_REGION]->(r) " +
                "WHERE p.P_Size = ??? AND p.P_Type =~ '*.???' AND r.R_Name = '???' AND ps.PS_SupplyCost = " + min_cost + " " +
                "RETURN s.S_AcctBal, s.S_Name, n.N_Name, p.P_PartKey, p.P_Mfgr, s.S_Address, s.S_Phone, s.S_Comment " +
                "ORDER BY s.S_AcctBal DESC, n.N_Name, s.S_Name, p.P_PartKey");
        System.out.println(result.dumpToString());
    }

    @Override
    public void doQuery3() {
        ExecutionResult result = engine.execute(
                "START li=node(*) " +
                "MATCH (c)<-[:ORDERED_BY_CUSTOMER]-(o)<-[:MEMBER_OF_ORDER]-(li) " +
                "WHERE c.C_MktSegment = '???' AND o.O_OrderDate < '???' AND li.L_ShipDate > '???' " +    
                "RETURN li.L_OrderKey, SUM(li.L_ExtendedPrice*(1 - li.L_Discount)) AS Revenue, o.O_OrderDate, o.O_ShipPriority " +
                "ORDER BY Revenue DESC, o.O_OrderDate");
        System.out.println(result.dumpToString());
    }

    @Override
    public void doQuery4() {
        ExecutionResult result = engine.execute(
                "START ps=node(*) " +
                "MATCH (c)<-[:ORDERED_BY_CUSTOMER]-(o)<-[:MEMBER_OF_ORDER]-(li)-[:HAS_PARTSUPP]->(ps)-[:FROM_SUPPLIER]->(s)-[:FROM_NATION]->(n)-[:MEMBER_OF_REGION]->(r) " +
                "(p)<-[:IS_PART]-(ps)-[:FROM_SUPPLIER]->(s)-[:FROM_NATION]->(n)-[:MEMBER_OF_REGION]->(r) " +
                "WHERE r.R_Name = '???' AND o.O_OrderDate >= '???' AND o.O_OrderDate < '???' " +
                "RETURN n.N_Name, SUM(li.L_ExtendedPrice*(1 - li.L_Discount)) AS Revenue");
        System.out.println(result.dumpToString());
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
    
    private void insertNations(int numInserts, int firstInsertPK, int firstRegionNode, int numRegions) {
        for (int i = 0; i < numInserts; i++) {
            int regionNode = firstRegionNode + r.nextInt(numRegions);
            Node region = graphDB.getNodeById(regionNode);
            
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
    
    private void insertSuppliers(int numInserts, int firstInsertPK, int firstNationNode, int numNations) {
        for (int i = 0; i < numInserts; i++) {
            int nationNode = firstNationNode + r.nextInt(numNations);
            Node nation = graphDB.getNodeById(nationNode);
            
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
    
    private void insertCustomers(int numInserts, int firstInsertPK, int firstNationNode, int numNations) {
        for (int i = 0; i < numInserts; i++) {
            int nationNode = firstNationNode + r.nextInt(numNations);
            Node nation = graphDB.getNodeById(nationNode);
            
            Node node = graphDB.createNode();
            node.setProperty("C_CustKey", firstInsertPK + i);
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
    
    private void insertPartsupps(int numInserts, int firstPartNode, int numParts, int firstSupplierNode, int numSuppliers) {
        for (int i = 0; i < numInserts; i++) {
            ArrayList<Integer> pk;
            int partNode, supplierNode;
            do {
                pk = new ArrayList<Integer>(2);
                partNode = firstPartNode + r.nextInt(numParts);            
                supplierNode = firstSupplierNode + r.nextInt(numSuppliers);    
                pk.add(partNode);
                pk.add(supplierNode);
            } while (partsuppPK.contains(pk));
            partsuppPK.add(pk);
            
            Node part = graphDB.getNodeById(partNode);
            Node supplier = graphDB.getNodeById(supplierNode);
            
            Node node = graphDB.createNode();
            node.createRelationshipTo(part, RelTypes.IS_PART);
            node.createRelationshipTo(supplier, RelTypes.FROM_SUPPLIER);
            node.setProperty("PS_AvailQty", getRandomInteger());
            node.setProperty("PS_SupplyCost", getRandomNumber(13/2, 2));
            node.setProperty("PS_Comment", getRandomString(200/2));
            node.setProperty("skip", getRandomString(64/2));
        }
    }
    
    private void insertOrders(int numInserts, int firstInsertPK, int firstCustomerNode, int numCustomers) {
        for (int i = 0; i < numInserts; i++) {
            int customerNode = firstCustomerNode + r.nextInt(numCustomers);
            Node customer = graphDB.getNodeById(customerNode);
            
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
    
    private void insertLineitems(int numInserts, int firstOrderNode, int numOrders, int firstPartsuppNode, int numPartsupps) {
        for (int i = 0; i < numInserts; i++) {
            ArrayList<Integer> pk;
            int orderNode, linenumber;
            do {
                pk = new ArrayList<Integer>(2);
                orderNode = firstOrderNode + r.nextInt(numOrders);     
                linenumber = getRandomInteger();
                pk.add(orderNode);
                pk.add(linenumber);
            } while (lineitemPK.contains(pk));
            lineitemPK.add(pk);
            
            Node order = graphDB.getNodeById(orderNode);
            
            int partsuppNode = firstPartsuppNode + r.nextInt(numPartsupps);
            Node partsupp = graphDB.getNodeById(partsuppNode);
            
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
    
    private Long getRandomDate() {
        Date dt = null;
        do {
            dt = GenerationUtility.generateDate();
        } while (dt == null);
        return dt.getTime();
    }
    
}