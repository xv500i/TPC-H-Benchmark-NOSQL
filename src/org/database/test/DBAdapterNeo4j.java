
package org.database.test;

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
    
}