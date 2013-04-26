/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.database.test;

/**
 *
 * @author Alex
 */
public abstract class AbstractDBAdapter implements IDBAdapter {
    
    private enum Operations {INSERT_1, INSERT_2, QUERY_1, QUERY_2, QUERY_3, QUERY_4};
    
    // Strategy pattern
    private float insertBulk(Operations op) {
        long initialTime = System.currentTimeMillis();
        switch (op) {
            case INSERT_1:
                firstInsertOperation();
                break;
            case INSERT_2:
                secondInsertOperation();
                break;
            case QUERY_1:
                doQuery1();
                break;
            case QUERY_2:
                doQuery2();
                break;
            case QUERY_3:
                doQuery3();
                break;
            case QUERY_4:
                doQuery4();
                break;
            default:
                break;
        }
        long endTime = System.currentTimeMillis();
        return (float)(endTime-initialTime)/1000.0f;
    }
    
    @Override
    public float insertFirstBulk() {
        return insertBulk(Operations.INSERT_1);
    }
    
    @Override
    public float insertSecondBulk() {
        return insertBulk(Operations.INSERT_2);
    }  
    
    @Override
    public abstract float executeQuery1();

    @Override
    public abstract float executeQuery2();    

    @Override
    public abstract float executeQuery3();
    
    @Override
    public abstract float executeQuery4();

    // subclass must implement
    @Override
    public abstract void connect();
    
    @Override
    public abstract void disconnect();
    
    protected abstract void firstInsertOperation();
    
    protected abstract void secondInsertOperation();
    
    public abstract float doQuery1();

    public abstract float doQuery2(); 

    public abstract float doQuery3();

    public abstract float doQuery4();

}
