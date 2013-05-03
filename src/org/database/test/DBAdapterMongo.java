/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.database.test;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Alex
 */
public class DBAdapterMongo extends AbstractDBAdapter {

    
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
    }

    @Override
    public void disconnect() {
        mc.close();
    }


    @Override
    protected void firstInsertOperation() {
        List<DBObject> inserts;
        // 5 regions
        inserts = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            BasicDBObject part = new BasicDBObject();
            part.append("_id", i);// [1-5]
            part.append("R_name", GenerationUtility.generateString(32));
            part.append("R_comment", GenerationUtility.generateString(80));
            inserts.add(part);
        }
        db.getCollection("region").insert(inserts);
        
        // 25 nations
        inserts = new ArrayList<>();
        for (int i = 1; i <= 25; i++) {
            BasicDBObject part = new BasicDBObject();
            part.append("_id", i); // [1-25]
            part.append("a", GenerationUtility.generateString(32));
            part.append("b", (i%5)+1);
            part.append("c", GenerationUtility.generateString(32));
            inserts.add(part);
        }
        db.getCollection("nation").insert(inserts);
        
        // 666 part
        inserts = new ArrayList<>();
        for (int i = 1; i <= 666; i++) {
            BasicDBObject part = new BasicDBObject();
            part.append("_id", i); // [1-666]
            part.append("a", GenerationUtility.generateString(32));
            part.append("b", GenerationUtility.generateString(32));
            part.append("c", GenerationUtility.generateString(32));
            part.append("d", GenerationUtility.generateString(32));
            part.append("e", GenerationUtility.generateInteger());
            part.append("f", GenerationUtility.generateString(32));
            part.append("g", GenerationUtility.generateNumber(6,2));
            part.append("h", GenerationUtility.generateString(32));
            inserts.add(part);
        }
        db.getCollection("part").insert(inserts);
        
        // 33 supplier
        inserts = new ArrayList<>();
        for (int i = 1; i <= 33; i++) {
            BasicDBObject part = new BasicDBObject();
            part.append("_id", i); // [1-33]
            part.append("a", GenerationUtility.generateString(32));
            part.append("b", GenerationUtility.generateString(32));
            part.append("c", (i%25)+1);
            part.append("d", GenerationUtility.generateString(32));
            part.append("e", GenerationUtility.generateNumber(6, 2));
            part.append("f", GenerationUtility.generateString(52));
            inserts.add(part);
        }
        db.getCollection("supplier").insert(inserts);

        // 500 customers
        inserts = new ArrayList<>();
        for (int i = 1; i <= 500; i++) {
            BasicDBObject part = new BasicDBObject();
            part.append("_id", i); // [1-500]
            part.append("a", GenerationUtility.generateString(32));
            part.append("b", GenerationUtility.generateString(32));
            part.append("c", (i%25)+1);
            part.append("d", GenerationUtility.generateString(32));
            part.append("e", GenerationUtility.generateNumber(6, 2));
            part.append("f", GenerationUtility.generateString(32));
            part.append("g", GenerationUtility.generateString(60));
            inserts.add(part);
        }
        db.getCollection("customer").insert(inserts);

        // 2666 partsupps
        inserts = new ArrayList<>();
        for (int i = 1; i <= 2666; i++) {
            BasicDBObject part = new BasicDBObject();
            part.append("a", (i%666)+1);
            part.append("b", (i%33)+1);
            part.append("c", GenerationUtility.generateInteger());
            part.append("d", GenerationUtility.generateNumber(6, 2));
            part.append("f", GenerationUtility.generateString(100));
            inserts.add(part);
        }
        db.getCollection("partsupp").insert(inserts);

        // 5000 orders
        inserts = new ArrayList<>();
        for (int i = 1; i <= 5000; i++) {
            BasicDBObject part = new BasicDBObject();
            part.append("_id", i); // [1-5000]
            part.append("a", (i%500)+1);
            part.append("b", GenerationUtility.generateString(32));
            part.append("c", GenerationUtility.generateNumber(6, 2));
            part.append("d", GenerationUtility.generateDate());
            part.append("e", GenerationUtility.generateString(32));
            part.append("f", GenerationUtility.generateString(32));
            part.append("g", GenerationUtility.generateInteger());
            part.append("h", GenerationUtility.generateString(40));
            inserts.add(part);
        }
        db.getCollection("order").insert(inserts);
        
        // 20000 lineitems
        inserts = new ArrayList<>();
        for (int i = 1; i <= 20000; i++) {
            BasicDBObject part = new BasicDBObject();
            part.append("_id", (i%5000)+1);
            part.append("a", (i%666)+1);
            part.append("b", (i%33)+1);
            part.append("c", GenerationUtility.generateInteger());
            part.append("d", GenerationUtility.generateInteger());
            part.append("e", GenerationUtility.generateNumber(6, 2));
            part.append("f", GenerationUtility.generateNumber(6, 2));
            part.append("g", GenerationUtility.generateNumber(6, 2));
            part.append("h", GenerationUtility.generateString(32));
            part.append("i", GenerationUtility.generateString(32));
            part.append("j", GenerationUtility.generateDate());
            part.append("k", GenerationUtility.generateDate());
            part.append("l", GenerationUtility.generateDate());
            part.append("m", GenerationUtility.generateString(32));
            part.append("n", GenerationUtility.generateString(32));
            part.append("o", GenerationUtility.generateString(32));
            inserts.add(part);
        }
        db.getCollection("lineitem").insert(inserts);
        System.exit(0);    
    }

    @Override
    protected void secondInsertOperation() {
        // C&P pero sumant a i +20000
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void doQuery1() {
        //DBCursor find = tpcCollection.find(new BasicDBObject());
        //System.out.println("Query 1 returned: " + find.count() + " rows.");
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
