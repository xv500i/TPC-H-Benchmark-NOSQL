/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.database.test;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import java.net.UnknownHostException;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author Alex
 */
public class MainMongo {
    
    public static float insertBulk() {
        return 0.0f;
    }
    
    public static float executeQuery1() {
        return 0.0f;
    }
    
    public static float executeQuery2() {
        return 0.0f;
    }
    
    public static float executeQuery3() {
        return 0.0f;
    }
    
    public static float executeQuery4() {
        return 0.0f;
    }
    
    public static void main (String args[]) {
        MongoClient mc = null;
        try {
            mc = new MongoClient("localhost");
        } catch (UnknownHostException ex) {
            Logger.getLogger(MainMongo.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(-1);
        }
        DB db = mc.getDB("test");
        DBCollection tcpCollection = db.getCollection("tpc");
        
        /* TEST */
        BasicDBObject removeAllObject = new BasicDBObject();
        tcpCollection.remove( removeAllObject );
        BasicDBObject insertObject = new BasicDBObject().append("_id", "1").append("name", "alex");
        tcpCollection.insert(insertObject);
        BasicDBObject findObject = new BasicDBObject().append("_id", "1");
        DBObject findOne = tcpCollection.findOne(findObject);
        System.out.println(findOne);
        tcpCollection.drop();
        Set<String> collections = db.getCollectionNames();
        System.out.println("Database collections");
        for (String s: collections) {
            System.out.println(s);
        }
        /* END TEST */
        float time;
        time = insertBulk();
        System.out.println("Bulk insert time = " + time);
        int i;
        time = Float.MAX_VALUE;
        for (i = 0; i < 5; i++) {
            time = Math.min(time, executeQuery1());
        }
        System.out.println("Query1 time = " + time);
        time = Float.MAX_VALUE;
        for (i = 0; i < 5; i++) {
            time = Math.min(time, executeQuery2());
        }
        System.out.println("Query2 time = " + time);
        time = Float.MAX_VALUE;
        for (i = 0; i < 5; i++) {
            time = Math.min(time, executeQuery3());
        }
        System.out.println("Query3 time = " + time);
        time = Float.MAX_VALUE;
        for (i = 0; i < 5; i++) {
            time = Math.min(time, executeQuery4());
        }
        System.out.println("Query4 time = " + time);
    }
}
