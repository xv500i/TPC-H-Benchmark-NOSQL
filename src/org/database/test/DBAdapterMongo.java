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
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
           
    }

    @Override
    protected void secondInsertOperation() {
        // C&P pero sumant a i +20000
        List<DBObject> inserts;
        // 5 regions
              
        inserts = new ArrayList<>();
        for (int i = 1+20000; i <= 5+20000; i++) {
            BasicDBObject part = new BasicDBObject();
            part.append("_id", i);// [1-5]
            part.append("R_name", GenerationUtility.generateString(32));
            part.append("R_comment", GenerationUtility.generateString(80));
            inserts.add(part);
        }
        db.getCollection("region").insert(inserts);
        
        // 25 nations
        inserts = new ArrayList<>();
        for (int i = 1+20000; i <= 25+20000; i++) {
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
        for (int i = 1+20000; i <= 666+20000; i++) {
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
        for (int i = 1+20000; i <= 33+20000; i++) {
            BasicDBObject part = new BasicDBObject();
            part.append("_id", i); // [1-33]
            part.append("S_name", GenerationUtility.generateString(32));
            part.append("S_address", GenerationUtility.generateString(32));
            part.append("S_nationkey", (i%25)+1+20000);
            part.append("S_phone", GenerationUtility.generateString(32));
            part.append("S_acctbal", GenerationUtility.generateNumber(6, 2));
            part.append("S_comment", GenerationUtility.generateString(52));
            inserts.add(part);
        }
        db.getCollection("supplier").insert(inserts);

        // 500 customers
        inserts = new ArrayList<>();
        for (int i = 1+20000; i <= 500+20000; i++) {
            BasicDBObject part = new BasicDBObject();
            part.append("_id", i); // [1-500]
            part.append("C_name", GenerationUtility.generateString(32));
            part.append("C_address", GenerationUtility.generateString(32));
            part.append("C_nationkey", (i%25)+1+20000);
            part.append("C_phone", GenerationUtility.generateString(32));
            part.append("C_acctbal", GenerationUtility.generateNumber(6, 2));
            part.append("C_mktsegment", GenerationUtility.generateString(32));
            part.append("C_comment", GenerationUtility.generateString(60));
            inserts.add(part);
        }
        db.getCollection("customer").insert(inserts);

        // 2666 partsupps
        inserts = new ArrayList<>();
        for (int i = 1+20000; i <= 2666+20000; i++) {
            BasicDBObject part = new BasicDBObject();
            part.append("PS_partkey", (i%666)+1+20000);
            part.append("PS_suppkey", (i%33)+1+20000);
            Integer is = GenerationUtility.generateInteger();
            part.append("PS_availqty", is);
            part.append("PS_supplycost", GenerationUtility.generateNumber(6, 2));
            part.append("PS_comment", GenerationUtility.generateString(100));
            inserts.add(part);
        }
        db.getCollection("partsupp").insert(inserts);

        // 5000 orders
        inserts = new ArrayList<>();
        for (int i = 1+20000; i <= 5000+20000; i++) {
            BasicDBObject part = new BasicDBObject();
            part.append("_id", i); // [1-5000]
            part.append("O_custkey", (i%500)+1+20000);
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
        for (int i = 1+20000; i <= 20000+20000; i++) {
            BasicDBObject part = new BasicDBObject();
            part.append("L_orderkey", (i%5000)+1+20000);
            part.append("L_partkey", (i%666)+1+20000);
            part.append("L_suppkey", (i%33)+1+20000);
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
        System.out.println( output.getCommandResult() );
    }

    @Override
    public void doQuery2() {
        
        Object size;
        Object type;
        Object regionName = "wathever";
        
        ArrayList<Object []> select = new ArrayList<>();
        
        DBCollection partCollection = db.getCollection("part");
        DBCollection partSuppCollection = db.getCollection("partsupp");
        DBCollection supplierCollection = db.getCollection("supplier");
        DBCollection regionCollection = db.getCollection("region");
        DBCollection nationCollection = db.getCollection("nation");
        
        DBObject match = new BasicDBObject("R_name", regionName);
        DBObject fields = new BasicDBObject();
        fields.put("_id", 1);
        DBCursor regionsMatched = regionCollection.find(match, fields);
        while (regionsMatched.hasNext()) {
            DBObject region = regionsMatched.next();
            Object regionId = region.get("_id");
            
            DBObject join = new BasicDBObject("N_regionkey", regionId);
            DBObject joinFields = new BasicDBObject();
            joinFields.put("_id",1);
            joinFields.put("N_name",1);
            
            DBCursor nationsMatched = nationCollection.find(join, joinFields);
            while (nationsMatched.hasNext()) {
                DBObject nation = nationsMatched.next();
                Object nationId = nation.get("_id");
                Object nationName = nation.get("N_name");
                
                DBObject join2 = new BasicDBObject("S_nationkey", nationId);
                DBObject join2Fields = new BasicDBObject();
                join2Fields.put("_id", 1);
                join2Fields.put("S_acctbal", 1);
                join2Fields.put("S_name", 1);
                join2Fields.put("S_phone", 1);
                join2Fields.put("S_comment", 1);
                join2Fields.put("S_address", 1);
                
                DBCursor supplierMatched = supplierCollection.find(join2, join2Fields);
                while (supplierMatched.hasNext()) {
                    DBObject supplier = supplierMatched.next();
                    Object supplierId = supplier.get("_id");
                    Object supplierBalance = supplier.get("S_acctbal");
                    Object supplierName = supplier.get("S_name");
                    Object supplierPhone = supplier.get("S_phone");
                    Object supplierAddress = supplier.get("S_address");
                    Object supplierComment = supplier.get("S_comment");
                    
                    DBObject join3 = new BasicDBObject("PS_suppkey", supplierId);
                    DBObject join3Fields = new BasicDBObject(); 
                    join3Fields.put("_id", 1);
                    join3Fields.put("PS_partkey", 1);
                    
                    DBCursor partsuppMatched = partSuppCollection.find(join3, join3Fields);
                    
                    while (partsuppMatched.hasNext()) {
                        DBObject partsupp = partsuppMatched.next();
                        Object partId = partsupp.get("PS_partkey");
                        
                        DBObject join4 = new BasicDBObject("_id", partId);
                        DBObject join4Fields = new BasicDBObject();
                        join4Fields.put("_id", 1);
                        join4Fields.put("P_size", 1);
                        join4Fields.put("P_type", 1);
                        join4Fields.put("P_mfgr", 1);
                        
                        DBCursor partMatched = partCollection.find(join4, join4Fields);
                        while (partMatched.hasNext()) {
                            DBObject part = partMatched.next();
                            Object partKey = part.get("_id");
                            Object partMFGR = part.get("P_mfgr");
                            
                            if (true) {
                                select.add( new Object[] {supplierBalance, supplierName, nationName, partKey, partMFGR, supplierAddress, supplierPhone, supplierComment} );
                            }
                        }
                    }
                }
            }
        }
        System.out.println( select.size() );
    }

    @Override
    public void doQuery3() {
        DBCollection lineitemCollection = db.getCollection("lineitem");
        DBCollection orderCollection = db.getCollection("order");
        DBCollection customerCollection = db.getCollection("customer");
        String mktsegment = "gmnTRTVqCJsgMtcUjqJQCJWbMrohaXIu";
        java.util.Date date1  = new Date();
        java.util.Date date2 = new Date();
        date2.setYear(2013 - 1900);
        date1.setYear(2012 - 1900);
        ArrayList<Object []> select = new ArrayList<>();
        // l_orderkey, l_extendedprice, l_discount, o_orderdate, o_shippriority
        
        DBObject match = new BasicDBObject("C_mktsegment", mktsegment);
        DBObject fields = new BasicDBObject();
        fields.put("_id", 1);
        DBCursor customersMatched = customerCollection.find(match, fields);
        while (customersMatched.hasNext()) {
            DBObject customer = customersMatched.next();
            Object custkey = customer.get("_id");
            DBObject join = new BasicDBObject("O_custkey", custkey);
            DBObject joinFields = new BasicDBObject();
            joinFields.put("_id", 1);
            joinFields.put("O_orderdate", 1);
            joinFields.put("O_shippriority", 1);
            DBCursor ordersMatched = orderCollection.find(join, joinFields);
            while (ordersMatched.hasNext()) {
                DBObject order = ordersMatched.next();
                Object orderKey = order.get("_id");
                Object orderDate = order.get("O_orderdate");
                Object shipPriority = order.get("O_shippriority");
                if (date1.before((java.util.Date) orderDate)) {
                    DBObject join2 = new BasicDBObject("L_orderkey", orderKey);
                    DBObject joinFields2 = new BasicDBObject();
                    //joinFields.put("L_orderkey", 1); igual que O_orderkey
                    joinFields2.put("L_extendedprice", 1);
                    joinFields2.put("L_discount", 1);
                    joinFields2.put("L_shipdate", 1);
                    DBCursor lineitemsMatched = lineitemCollection.find(join2, joinFields2);
                    while (lineitemsMatched.hasNext()) {
                        DBObject lineitem = lineitemsMatched.next();
                        Object shipDate = lineitem.get("L_shipdate");
                        Object discount = lineitem.get("L_discount");
                        Object extendedPrice = lineitem.get("L_extentedprice");
                        if (date2.after((java.util.Date) shipDate)) {
                            select.add( new Object[] {orderKey, extendedPrice, discount, orderDate, shipPriority});
                        }
                    }
                }
            }
        }
        
        System.out.println( select.size() );
    }

    @Override
    public void doQuery4() {
        DBCollection lineitemCollection = db.getCollection("lineitem");
        DBCollection orderCollection = db.getCollection("order");
        DBCollection customerCollection = db.getCollection("customer");
        DBCollection supplierCollection = db.getCollection("supplier");
        DBCollection nationCollection = db.getCollection("nation");
        DBCollection regionCollection = db.getCollection("region");
        String regionName = "NFWlqIfksxATCFWVaEgsngPCYVbYbQOP";
        java.util.Date date1  = new Date();
        date1.setYear(2012 - 1900);
        java.util.Date date2 = (java.util.Date) date1.clone();
        date2.setYear(date2.getYear() + 1);
        //  n_name, sum(l_extendedprice * (1-l_discount)) as revenue
        ArrayList<Object []> select = new ArrayList<>();
        
        // get nationkeys with region = regioname
        Set< String > nationNamesWhere = new HashSet<>();
        DBObject matching = new BasicDBObject("R_name", regionName);
        DBCursor regionsMatched = regionCollection.find(matching, new BasicDBObject("_id", 1));
        while (regionsMatched.hasNext()) {
            DBObject region = regionsMatched.next();
            Object regionkey = region.get("_id");
            
            DBCursor nationsMatched = nationCollection.find(new BasicDBObject("N_regionkey", regionkey), new BasicDBObject("_id", 1));
            while (nationsMatched.hasNext()) {
                DBObject nation = nationsMatched.next();
                String nationName = (nation.get("_id")).toString();
                nationNamesWhere.add(nationName);
            }
        }
        
        DBObject match = new BasicDBObject();
        DBObject fields = new BasicDBObject();
        fields.put("_id", 1); // O_orderkey
        fields.put("O_custkey", 1);
        fields.put("O_orderdate", 1);
        DBCursor ordersMatched = orderCollection.find(match, fields);
        while (ordersMatched.hasNext()) {
            DBObject order = ordersMatched.next();
            Object orderkey = order.get("_id");
            Object customerkey = order.get("O_custkey");
            Object orderdate = order.get("O_orderdate");
            java.util.Date aux = (java.util.Date)orderdate;
            if (aux.after(date1) && aux.before(date2)) {
                DBObject join = new BasicDBObject("_id", customerkey);
                DBObject joinFields = new BasicDBObject();
                joinFields.put("_id", 1);
                joinFields.put("C_nationkey", 1);
                DBCursor customersMatched = customerCollection.find(join, joinFields);
                while (customersMatched.hasNext()) {
                    DBObject customer = customersMatched.next();
                    Object nationKey = customer.get("C_nationkey");

                    if (nationNamesWhere.contains(nationKey.toString())) {
                        // nation with region with name specified
                        DBObject join2 = new BasicDBObject("L_orderkey", orderkey);
                        DBObject joinFields2 = new BasicDBObject();
                        joinFields2.put("L_suppkey", 1);
                        joinFields2.put("L_discount", 1);
                        joinFields2.put("L_extendedprice", 1);
                        DBCursor lineitemsMatched = lineitemCollection.find(join2, joinFields2);
                        while (lineitemsMatched.hasNext()) {
                            DBObject lineitem = lineitemsMatched.next();
                            Object suppkey = lineitem.get("L_suppkey");
                            Object discount = lineitem.get("L_discount");
                            Object extendedprice = lineitem.get("L_extendedprice");
                            DBObject join3 = new BasicDBObject("_id", suppkey);
                            DBObject joinFields3 = new BasicDBObject("S_nationkey", 1);
                            DBCursor suppliersMatched = supplierCollection.find(join3, joinFields3);
                            while (suppliersMatched.hasNext()) {
                                DBObject supplier = suppliersMatched.next();
                                Object suppliernation = supplier.get("S_nationkey");
                                if (nationNamesWhere.contains(suppliernation.toString())) {
                                    select.add( new Object[] {nationKey, extendedprice, discount});
                                }
                            }
                        } 
                    }    
                }
            }
        }
        System.out.println( select.size() );
    }
    
}
