
package org.database.test;

/**
 *
 * @author Alex
 */
public abstract class AbstractDBAdapter implements IDBAdapter {
    
    private enum Operations {INSERT_1, INSERT_2, QUERY_1, QUERY_2, QUERY_3, QUERY_4};
    
    // Strategy pattern
    private float doOperation(Operations op) {
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
        return doOperation(Operations.INSERT_1);
    }
    
    @Override
    public float insertSecondBulk() {
        return doOperation(Operations.INSERT_2);
    }  
    
    @Override
    public float executeQuery1() {
        return doOperation(Operations.QUERY_1);
    }

    @Override
    public float executeQuery2() {
        return doOperation(Operations.QUERY_2);
    }    

    @Override
    public float executeQuery3() {
        return doOperation(Operations.QUERY_3);
    }
    
    @Override
    public float executeQuery4() {
        return doOperation(Operations.QUERY_4);
    }

    // subclass must implement
    @Override
    public abstract void connect();
    
    @Override
    public abstract void disconnect();
    
    protected abstract void firstInsertOperation();
    
    protected abstract void secondInsertOperation();
    
    public abstract void doQuery1();

    public abstract void doQuery2(); 

    public abstract void doQuery3();

    public abstract void doQuery4();

}
