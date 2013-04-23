/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.database.test;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Alex
 */
public class DBAdapterMongo implements IDBAdapter {

    
    /*
     MongoClient mc = null;
        try {
            mc = new MongoClient("localhost");
        } catch (UnknownHostException ex) {
            Logger.getLogger(MainMongo.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(-1);
        }
        DB db = mc.getDB("test");
        DBCollection tpcCollection = db.getCollection("tpc");
        BasicDBObject removeAllObject = new BasicDBObject();
        tpcCollection.remove( removeAllObject );
        BasicDBObject insertObject = new BasicDBObject().append("_id", "1").append("name", "alex");
        tpcCollection.insert(insertObject);
        BasicDBObject findObject = new BasicDBObject().append("_id", "1");
        DBObject findOne = tpcCollection.findOne(findObject);
        System.out.println(findOne);
        tpcCollection.drop();
        Set<String> collections = db.getCollectionNames();
        System.out.println("Database collections");
        for (String s: collections) {
            System.out.println(s);
        }
     */
    
    
    MongoClient mc;
    DB db;
    
    @Override
    public void connect() {
        try {
            mc = new MongoClient("localhost");
        } catch (UnknownHostException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(-1);
        }
        db = mc.getDB("test");
        // Clean the DB
        DBCollection tpcCollection = db.getCollection("tpc");
        tpcCollection.drop();
    }

    @Override
    public void disconnect() {
        mc.close();
    }

    @Override
    public float insertFirstBulk() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public float insertSecondBulk() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public float executeQuery1() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public float executeQuery2() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public float executeQuery3() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public float executeQuery4() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}
