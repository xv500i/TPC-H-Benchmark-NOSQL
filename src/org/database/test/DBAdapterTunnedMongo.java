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
import java.sql.Date;
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
        inserts = new ArrayList<>();
        
        Integer ps_availqty = GenerationUtility.generateInteger();
        Double ps_supplycost = GenerationUtility.generateNumber(6, 2);
        String ps_comment = GenerationUtility.generateString(100);
        
        String p_name = GenerationUtility.generateString(32);
        String p_mfgr = GenerationUtility.generateString(32);
        String p_brand = GenerationUtility.generateString(32);
        String p_type = GenerationUtility.generateString(32);
        Integer p_size = GenerationUtility.generateInteger();
        String p_container = GenerationUtility.generateString(32);
        Double p_retailprice = GenerationUtility.generateNumber(6,2);
        String p_comment = GenerationUtility.generateString(32);
        
        String s_name = GenerationUtility.generateString(32);
        String s_address = GenerationUtility.generateString(32);
        String s_phone = GenerationUtility.generateString(32);
        Double s_acctbal = GenerationUtility.generateNumber(6, 2);
        String s_comment = GenerationUtility.generateString(52);
        
        String n_name_supplier = GenerationUtility.generateString(32);
        String n_comment_supplier = GenerationUtility.generateString(32);
        
        String n_name_customer = GenerationUtility.generateString(32);
        String n_comment_customer = GenerationUtility.generateString(32);
        
        String r_name_customer = GenerationUtility.generateString(32);
        String r_comment_customer = GenerationUtility.generateString(80);

        String r_name_supplier = GenerationUtility.generateString(32);
        String r_comment_supplier = GenerationUtility.generateString(80);
        
        String o_orderstatus = GenerationUtility.generateString(32);
        Double o_totalprice = GenerationUtility.generateNumber(6, 2);
        java.util.Date o_orderdate = new java.util.Date();
        String o_orderpriority = GenerationUtility.generateString(32);
        String o_clerk = GenerationUtility.generateString(32);
        Integer o_shippriority = GenerationUtility.generateInteger();
        String o_comment = GenerationUtility.generateString(40);
        
        String c_name = GenerationUtility.generateString(32);
        String c_address = GenerationUtility.generateString(32);
        String c_phone = GenerationUtility.generateString(32);
        Double c_acctbal = GenerationUtility.generateNumber(6, 2);
        String c_mktsegment = GenerationUtility.generateString(32);
        String c_comment = GenerationUtility.generateString(60);
        
        for (int i = 1; i <= 20000; i++) {
            if (i%4000 == 0) {
                //changeCustomerRegion = true;
                r_name_customer = GenerationUtility.generateString(32);
                r_comment_customer = GenerationUtility.generateString(80);
                //changeSupplierRegion = true;
                r_name_supplier = GenerationUtility.generateString(32);
                r_comment_supplier = GenerationUtility.generateString(80);
            }
            if (i%800 == 0) {
                //changeCustomerNation = true;
                n_name_customer = GenerationUtility.generateString(32);
                n_comment_customer = GenerationUtility.generateString(32);
                //changeSupplierNation = true;
                n_name_supplier = GenerationUtility.generateString(32);
                n_comment_supplier = GenerationUtility.generateString(32);
            }
            if (i%4 == 0) {
                //changeOrder = true;
                o_orderstatus = GenerationUtility.generateString(32);
                o_totalprice = GenerationUtility.generateNumber(6, 2);
                o_orderdate = new java.util.Date();
                o_orderpriority = GenerationUtility.generateString(32);
                o_clerk = GenerationUtility.generateString(32);
                o_shippriority = GenerationUtility.generateInteger();
                o_comment = GenerationUtility.generateString(40);
            }
            if (i%600 == 0) {
                //changeSupplier = true;
                s_name = GenerationUtility.generateString(32);
                s_address = GenerationUtility.generateString(32);
                s_phone = GenerationUtility.generateString(32);
                s_acctbal = GenerationUtility.generateNumber(6, 2);
                s_comment = GenerationUtility.generateString(52);
            }
            if (i%40 == 0) {
                //changeCustomer = true;
                c_name = GenerationUtility.generateString(32);
                c_address = GenerationUtility.generateString(32);
                c_phone = GenerationUtility.generateString(32);
                c_acctbal = GenerationUtility.generateNumber(6, 2);
                c_mktsegment = GenerationUtility.generateString(32);
                c_comment = GenerationUtility.generateString(60);
            }
            if (i%30 == 0) {
                //changePart = true;
                p_name = GenerationUtility.generateString(32);
                p_mfgr = GenerationUtility.generateString(32);
                p_brand = GenerationUtility.generateString(32);
                p_type = GenerationUtility.generateString(32);
                p_size = GenerationUtility.generateInteger();
                p_container = GenerationUtility.generateString(32);
                p_retailprice = GenerationUtility.generateNumber(6,2);
                p_comment = GenerationUtility.generateString(32);
            }
            if (i%7 == 0) {
                // changePartSupplier = true;
                ps_availqty = GenerationUtility.generateInteger();
                ps_supplycost = GenerationUtility.generateNumber(6, 2);
                ps_comment = GenerationUtility.generateString(100);
            }
            
            BasicDBObject lineitem = new BasicDBObject();
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
            partsupp.append("PS_availqty", ps_availqty);
            partsupp.append("PS_supplycost", ps_supplycost);
            partsupp.append("PS_comment", ps_comment);
            
            ///part
            BasicDBObject part = new BasicDBObject();
            part.append("P_name", p_name);
            part.append("P_mfgr", p_mfgr);
            part.append("P_brand", p_brand);
            part.append("P_type", p_type);
            part.append("P_size", p_size);
            part.append("P_container", p_container);
            part.append("P_retailprice", p_retailprice);
            part.append("P_comment", p_comment);
            
            /// supplier
            BasicDBObject supplier = new BasicDBObject();
            supplier.append("S_name", s_name);
            supplier.append("S_address", s_address);
            supplier.append("S_phone", s_phone);
            supplier.append("S_acctbal", s_acctbal);
            supplier.append("S_comment", s_comment);
            
            //// nation
            BasicDBObject nationSupp = new BasicDBObject();
            nationSupp.append("N_name", n_name_supplier);
            nationSupp.append("N_comment", n_comment_supplier);
            
            ///// region
            BasicDBObject regionSupp = new BasicDBObject();
            regionSupp.append("R_name", r_name_supplier);
            regionSupp.append("R_comment", r_comment_supplier);
            
            /// order
            BasicDBObject order = new BasicDBObject();
            order.append("O_orderstatus", o_orderstatus);
            order.append("O_totalprice", o_totalprice);
            order.append("O_orderdate", o_orderdate);
            order.append("O_orderpriority", o_orderpriority);
            order.append("O_clerk", o_clerk);
            order.append("O_shippriority", o_shippriority);
            order.append("O_comment", o_comment);
            
            //// customer
            BasicDBObject customer = new BasicDBObject();
            customer.append("C_name", c_name);
            customer.append("C_address", c_address);
            customer.append("C_phone", c_phone);
            customer.append("C_acctbal", c_acctbal);
            customer.append("C_mktsegment", c_mktsegment);
            customer.append("C_comment", c_comment);
            
            ///// nation
            BasicDBObject nationCust = new BasicDBObject();
            nationCust.append("N_name", n_name_customer);
            nationCust.append("N_comment", n_comment_customer);
           
            ////// region
            BasicDBObject regionCust = new BasicDBObject();
            regionCust.append("R_name", r_name_customer);
            regionCust.append("R_comment", r_comment_customer);
            
            nationCust.append("region", regionCust);
            customer.append("nation", nationCust);
            order.append("customer", customer);
            lineitem.append("order", order);
            
            partsupp.append("part", part);
            nationSupp.append("region", regionSupp);
            supplier.append("nation", nationSupp);
            partsupp.append("supplier", supplier);
            lineitem.append("partsupp", partsupp);
            
            inserts.add(lineitem);
        }
        db.getCollection("lineitem").insert(inserts);
        
    }

    @Override
    protected void secondInsertOperation() {
        // equival a la primera
        firstInsertOperation();
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
        DBCollection lineitemCollection = db.getCollection("lineitem");
        Integer size = 203032;
        String type = "/A$/"; // regex
        String regionName = "ADSaIKAGo";
        DBObject where = new BasicDBObject();
        where.put("$partsupp.part.P_type", type);
        where.put("$partsupp.part.P_size", size);
        where.put("$supplier.nation.region.R_name", regionName);
        DBObject match = new BasicDBObject("$match", where );

        // Now the $group operation
        DBObject groupBy = new BasicDBObject();
        groupBy.put("S_acctbal", "$partsupp.supply.S_acctbal");
        groupBy.put("S_name", "$partsupp.supply.S_name");
        groupBy.put("N_name", "$partsupp.supply.nation.N_name");
        groupBy.put("P_mfgr", "$partsupp.part.P_mfgr");
        groupBy.put("S_address", "$partsupp.supply.S_address");
        groupBy.put("S_phone", "$partsupp.supply.S_phone");
        groupBy.put("S_comment", "$partsupp.supply.S_comment");
        DBObject groupFields = new BasicDBObject("_id", groupBy);
        DBObject group = new BasicDBObject("$group", groupFields);
        
        // build the $projection operation
        
        DBObject fields = new BasicDBObject("_id", 0);
        fields.put("S_acctbal", 1);
        fields.put("S_name", 1);
        fields.put("N_name", 1);
        fields.put("P_mfgr", 1);
        fields.put("S_address", 1);
        fields.put("S_phone", 1);
        fields.put("S_comment", 1);
        DBObject project = new BasicDBObject("$project", fields );
        
        // order by
        DBObject orderClause = new BasicDBObject("S_acctbal", -1);
        orderClause.put("N_name", 1);
        orderClause.put("S_name", 1);
        DBObject orderby = new BasicDBObject("$sort", orderClause);
        
        // run aggregation
        AggregationOutput output = lineitemCollection.aggregate( match, group, project, orderby );
        System.out.println( output.getCommandResult() );
    }

    @Override
    public void doQuery3() {
        
        DBCollection lineitemCollection = db.getCollection("lineitem");
        java.util.Date date1 = new java.util.Date();
        java.util.Date date2 = new java.util.Date();
        String mktsegment = "leaoLADSk";
        DBObject where = new BasicDBObject();
        where.put("L_shipdate", new BasicDBObject("$gt", date2));
        DBObject order = new BasicDBObject(new BasicDBObject ("customer" , new BasicDBObject("C_mktsegment", mktsegment)));
        order.put("O_orderdate", new BasicDBObject("$lt", date1));
        where.put("order", order);
        DBObject match = new BasicDBObject("$match", where );

        // Now the $group operation
        DBObject groupBy = new BasicDBObject();
        groupBy.put("L_orderkey", "$_id");
        groupBy.put("O_orderdate", "$order.O_orderdate");
        groupBy.put("O_shippriority", "$order.O_shippriority");
        DBObject groupFields = new BasicDBObject("_id", groupBy);
        groupFields.put("revenue", new BasicDBObject("$sum", "$L_extendedprice*(1-$L_discount)"));
        DBObject group = new BasicDBObject("$group", groupFields);
        
        // build the $projection operation
        
        DBObject fields = new BasicDBObject("_id", 1);
        fields.put("L_orderkey", 1);
        fields.put("O_orderdate", 1);
        fields.put("O_shippriority", 1);
        fields.put("revenue", 1);
        DBObject project = new BasicDBObject("$project", fields );
        
        // order by
        DBObject orderClause = new BasicDBObject("revenue", -1);
        orderClause.put("O_orderdate", 1);
        DBObject orderby = new BasicDBObject("$sort", orderClause);
        
        // run aggregation
        AggregationOutput output = lineitemCollection.aggregate( match, group, project, orderby );
        System.out.println( output.getCommandResult() );
        
    }

    @Override
    public void doQuery4() {
        DBCollection lineitemCollection = db.getCollection("lineitem");
        java.util.Date date = new java.util.Date();
        String regionName = "ADSaIKAGo";
        DBObject where = new BasicDBObject();
        java.util.Date aux = new java.util.Date(date.getTime());
        aux.setYear(aux.getYear()+1);
        where.put("$supplier.nation.region.R_name", regionName);
        where.put("$order.O_orderdate", new BasicDBObject("$gt", date));
        where.put("$order.O_orderdate", new BasicDBObject("$lt", aux));
        DBObject match = new BasicDBObject("$match", where );

        // Now the $group operation
        DBObject groupBy = new BasicDBObject();
        groupBy.put("N_name", "$partsupp.supplier.nation.N_name");
        DBObject groupFields = new BasicDBObject("_id", groupBy);
        groupFields.put("revenue", new BasicDBObject("$sum", "$L_extendedprice*(1-$L_discount)"));
        DBObject group = new BasicDBObject("$group", groupFields);
        
        // build the $projection operation
        
        DBObject fields = new BasicDBObject("_id", 0);
        fields.put("N_name", 1);
        fields.put("revenue", 1);
        DBObject project = new BasicDBObject("$project", fields );
        
        // order by
        DBObject orderClause = new BasicDBObject("revenue", -1);
        DBObject orderby = new BasicDBObject("$sort", orderClause);
        
        // run aggregation
        AggregationOutput output = lineitemCollection.aggregate( match, group, project, orderby );
        System.out.println( output.getCommandResult() );
        
    }

}
