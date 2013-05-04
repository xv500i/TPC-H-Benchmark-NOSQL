/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.database.test;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
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
public class DBAdapterTunnedMongo extends AbstractDBAdapter{
 
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
        for (int i = 1; i <= 2666; i++) {
            BasicDBObject lineitem = new BasicDBObject();
            lineitem.append("L_orderkey", (i%5000)+1);
            lineitem.append("L_partkey", (i%666)+1);
            lineitem.append("L_suppkey", (i%33)+1);
            Integer is = GenerationUtility.generateInteger();
            lineitem.append("L_linenumber", is);
            is = GenerationUtility.generateInteger();
            lineitem.append("L_quantity", is);
            lineitem.append("L_extentedprice", GenerationUtility.generateNumber(6, 2));
            lineitem.append("L_discount", GenerationUtility.generateNumber(6, 2));
            lineitem.append("L_tax", GenerationUtility.generateNumber(6, 2));
            lineitem.append("L_returnflag", GenerationUtility.generateString(32));
            lineitem.append("L_linestatus", GenerationUtility.generateString(32));
            lineitem.append("L_shipdate", new java.util.Date());
            lineitem.append("L_commitdate", new java.util.Date());
            lineitem.append("L_receiptdate", new java.util.Date());
            lineitem.append("L_shipinstruct", GenerationUtility.generateString(32));
            lineitem.append("L_shipmode", GenerationUtility.generateString(32));
            lineitem.append("L_comment", GenerationUtility.generateString(32));
            
            // partsupp
            BasicDBObject partsupp = new BasicDBObject();
            
            ///part
            BasicDBObject part = new BasicDBObject();
            
            /// supplier
            BasicDBObject supplier = new BasicDBObject();
            
            //// nation
            
            ///// region
            
            /// order
            BasicDBObject order = new BasicDBObject();
            
            //// consumer
            
            ///// nation
            
            ////// region

            inserts.add(lineitem);
        }
        db.getCollection("lineitem").insert(inserts);
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
