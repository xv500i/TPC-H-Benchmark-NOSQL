
package org.database.test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;


/**
 * @author Natxo Raga Llorens
 */
public class DBAdapterNeo4j extends AbstractDBAdapter {

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
        throw new UnsupportedOperationException("Not supported yet.");
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
    
}
