/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.database.test;

import java.sql.Date;
import java.util.Random;

/**
 *
 * @author Alex
 */
public class GenerationUtility {
    
    private static Random r = new Random(System.currentTimeMillis());
    
    /**
     * 
     * @return integer inside [1000-9999] 
     */
    public static int generateInteger() {
        return 1000 + r.nextInt(9000);
    }
    
    /**
     * 
     * @return a random date 
     */
    public static Date generateDate() {
        return new Date(r.nextLong());
    }
    
    /**
     * 
     * @param size the string size
     * @return a string of size characters in [A-Za-z]
     */
    public static String generateString(int size) {
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            char c = (char) ((r.nextBoolean() ? 'A' : 'a') + r.nextInt('z' - 'a'));
            sb.append(c);
        }
        return sb.toString();
    }

    /**
     * 
     * @param numberOfDigits the number of digits wanted
     * @return a double with numberOfDigits digits
     */
    public static double generateNumber(int numberOfDigits, int decimalDigits) {
        // example 7,2: [0, 9999,99]
        double digits = (int)(Math.pow(10d, numberOfDigits) * r.nextDouble() - 1.0d);
        return digits / Math.pow(10.0d, decimalDigits);
    }
}
