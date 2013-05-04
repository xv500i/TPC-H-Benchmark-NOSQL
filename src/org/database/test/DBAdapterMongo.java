/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.database.test;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
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
        /*
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
            part.append("N_name", GenerationUtility.generateString(32));
            part.append("N_regionkey", (i%5)+1);
            part.append("N_comment", GenerationUtility.generateString(32));
            inserts.add(part);
        }
        db.getCollection("nation").insert(inserts);
        
        // 666 part
        inserts = new ArrayList<>();
        for (int i = 1; i <= 666; i++) {
            BasicDBObject part = new BasicDBObject();
            part.append("_id", i); // [1-666]
            part.append("P_name", GenerationUtility.generateString(32));
            part.append("P_mfgr", GenerationUtility.generateString(32));
            part.append("P_brand", GenerationUtility.generateString(32));
            part.append("P_type", GenerationUtility.generateString(32));
            Integer is = GenerationUtility.generateInteger();
            part.append("P_size", is);
            part.append("P_container", GenerationUtility.generateString(32));
            part.append("P_retailprice", GenerationUtility.generateNumber(6,2));
            part.append("P_comment", GenerationUtility.generateString(32));
            inserts.add(part);
        }
        db.getCollection("part").insert(inserts);
        
        // 33 supplier
        inserts = new ArrayList<>();
        for (int i = 1; i <= 33; i++) {
            BasicDBObject part = new BasicDBObject();
            part.append("_id", i); // [1-33]
            part.append("S_name", GenerationUtility.generateString(32));
            part.append("S_address", GenerationUtility.generateString(32));
            part.append("S_nationkey", (i%25)+1);
            part.append("S_phone", GenerationUtility.generateString(32));
            part.append("S_acctbal", GenerationUtility.generateNumber(6, 2));
            part.append("S_comment", GenerationUtility.generateString(52));
            inserts.add(part);
        }
        db.getCollection("supplier").insert(inserts);

        // 500 customers
        inserts = new ArrayList<>();
        for (int i = 1; i <= 500; i++) {
            BasicDBObject part = new BasicDBObject();
            part.append("_id", i); // [1-500]
            part.append("C_name", GenerationUtility.generateString(32));
            part.append("C_address", GenerationUtility.generateString(32));
            part.append("C_nationkey", (i%25)+1);
            part.append("C_phone", GenerationUtility.generateString(32));
            part.append("C_acctbal", GenerationUtility.generateNumber(6, 2));
            part.append("C_mktsegment", GenerationUtility.generateString(32));
            part.append("C_comment", GenerationUtility.generateString(60));
            inserts.add(part);
        }
        db.getCollection("customer").insert(inserts);

        // 2666 partsupps
        inserts = new ArrayList<>();
        for (int i = 1; i <= 2666; i++) {
            BasicDBObject part = new BasicDBObject();
            part.append("PS_partkey", (i%666)+1);
            part.append("PS_suppkey", (i%33)+1);
            Integer is = GenerationUtility.generateInteger();
            part.append("PS_availqty", is);
            part.append("PS_supplycost", GenerationUtility.generateNumber(6, 2));
            part.append("PS_comment", GenerationUtility.generateString(100));
            inserts.add(part);
        }
        db.getCollection("partsupp").insert(inserts);

        // 5000 orders
        inserts = new ArrayList<>();
        for (int i = 1; i <= 5000; i++) {
            BasicDBObject part = new BasicDBObject();
            part.append("_id", i); // [1-5000]
            part.append("O_custkey", (i%500)+1);
            part.append("O_orderstatus", GenerationUtility.generateString(32));
            part.append("O_totalprice", GenerationUtility.generateNumber(6, 2));
            part.append("O_orderdate", new java.util.Date());
            part.append("O_orderpriority", GenerationUtility.generateString(32));
            part.append("O_clerk", GenerationUtility.generateString(32));
            Integer is = GenerationUtility.generateInteger();
            part.append("O_shippriority", is);
            part.append("O_comment", GenerationUtility.generateString(40));
            inserts.add(part);
        }
        db.getCollection("order").insert(inserts);
        
        // 20000 lineitems
        inserts = new ArrayList<>();
        for (int i = 1; i <= 20000; i++) {
            BasicDBObject part = new BasicDBObject();
            part.append("L_orderkey", (i%5000)+1);
            part.append("L_partkey", (i%666)+1);
            part.append("L_suppkey", (i%33)+1);
            Integer is = GenerationUtility.generateInteger();
            part.append("L_linenumber", is);
            is = GenerationUtility.generateInteger();
            part.append("L_quantity", is);
            part.append("L_extentedprice", GenerationUtility.generateNumber(6, 2));
            part.append("L_discount", GenerationUtility.generateNumber(6, 2));
            part.append("L_tax", GenerationUtility.generateNumber(6, 2));
            part.append("L_returnflag", GenerationUtility.generateString(32));
            part.append("L_linestatus", GenerationUtility.generateString(32));
            part.append("L_shipdate", new java.util.Date());
            part.append("L_commitdate", new java.util.Date());
            part.append("L_receiptdate", new java.util.Date());
            part.append("L_shipinstruct", GenerationUtility.generateString(32));
            part.append("L_shipmode", GenerationUtility.generateString(32));
            part.append("L_comment", GenerationUtility.generateString(32));
            inserts.add(part);
        }
        db.getCollection("lineitem").insert(inserts);
        */    
    }

    @Override
    protected void secondInsertOperation() {
        // C&P pero sumant a i +20000
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void doQuery1() {
        DBCollection lineitemCollection = db.getCollection("lineitem");
        
        DBObject match = new BasicDBObject("$match", new BasicDBObject("L_shipdate", new BasicDBObject("$lt", new java.util.Date())) );
        // build the $projection operation
        
        DBObject fields = new BasicDBObject("L_returnflag", 1);
        fields.put("L_linestatus", 1);
        fields.put("L_quantity", 1);
        fields.put("L_extendedprice", 1);
        fields.put("L_discount", 1);
        fields.put("L_tax", 1);
        fields.put("_id", 0);
        DBObject project = new BasicDBObject("$project", fields );

        // Now the $group operation
        DBObject groupBy = new BasicDBObject();
        groupBy.put("L_returnflag", "$L_returnflag");
        groupBy.put("L_linestatus", "$L_linestatus");
        DBObject groupFields = new BasicDBObject("_id", groupBy);
        groupFields.put("sum_qty", new BasicDBObject("$sum", "$L_quantity"));
        groupFields.put("sum_base_price", new BasicDBObject("$sum", "$L_extendedprice"));
        groupFields.put("sum_disc_price", new BasicDBObject("$sum", "$L_extendedprice*(1-$L_discount)"));
        groupFields.put("sum_charge", new BasicDBObject("$sum", "$L_extendedprice*(1-$L_discount)*(1-$L_tax)"));
        groupFields.put("avg_qty", new BasicDBObject("$avg", "$L_quantity"));
        groupFields.put("avg_extendedprice", new BasicDBObject("$avg", "$L_extendedprice"));
        groupFields.put("avg_discount", new BasicDBObject("$avg", "$L_discount"));
        groupFields.put("count_order", new BasicDBObject("$sum", 1));
        DBObject group = new BasicDBObject("$group", groupFields);
        
        // order by
        DBObject orderClause = new BasicDBObject("L_returnflag", 1);
        orderClause.put("L_linestatus", 1);
        DBObject order = new BasicDBObject("$sort", orderClause);
        
        // run aggregation
        AggregationOutput output = lineitemCollection.aggregate( match, project, group, order );
        
        /*
        // note - the collection still has the field name "dolaznaStr"
        // but, to we access "dolaznaStr" in the aggregation command, 
        // we add a $ sign in the BasicDBObject 

        DBObject groupFields = new BasicDBObject( "_id", "$L_linestatus");

        // we use the $sum operator to increment the "count" 
        // for each unique dolaznaStr 
        groupFields.put("count", new BasicDBObject( "$sum", 1));
        DBObject group = new BasicDBObject("$group", groupFields );


        // You can add a sort to order by count descending

        DBObject sortFields = new BasicDBObject("count", -1);
        DBObject sort = new BasicDBObject("$sort", sortFields );
        

        AggregationOutput output = lineitemCollection.aggregate(group);
        */
        System.out.println( output.getCommandResult() );
        System.exit(0);
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
