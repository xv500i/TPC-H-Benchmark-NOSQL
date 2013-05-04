
package org.database.test;

import java.sql.Date;
import java.util.Random;

/**
 *
 * @author Alex Soms Batalla
 * @author Natxo Raga Llorens
 */
public class GenerationUtility {
    
    private static Random r = new Random(System.currentTimeMillis());
    
    /**
     * @return an integer with at least 4 digits 
     */
    public static Integer generateInteger(boolean nullAllowed) {
        // NULL value?
        if (nullAllowed) {
            int nullLimit = r.nextInt(10);
            if (nullLimit == 0) return null;
        }
        
        return 1000 + r.nextInt(Integer.MAX_VALUE - 1000);
    }
    
    /**
     * @return a random date 
     */
    public static Date generateDate() {
        // NULL value?
        int nullLimit = r.nextInt(10);
        if (nullLimit == 0) return null;
        
        return new Date(r.nextLong());
    }
    
    /**
     * @param size the string size
     * @return a string of size 'size' with characters [A-Z, a-z]
     */
    public static String generateString(int size) {
        // NULL value?
        int nullLimit = r.nextInt(10);
        if (nullLimit == 0) return null;
        
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            char c = (char) ((r.nextBoolean() ? 'A' : 'a') + r.nextInt('z' - 'a'));
            sb.append(c);
        }
        return sb.toString();
    }

    /**
     * @param numberOfDigits the number of digits wanted
     * @param decimalDigits the number of decimal digits wanted
     * @return a double with numberOfDigits digits and decimalDigits decimal digits
     */
    public static Double generateNumber(int numberOfDigits, int decimalDigits) {
        // example 7,2: [0, 9999,99]
        // NULL value?
        int nullLimit = r.nextInt(10);
        if (nullLimit == 0) return null;
        
        double digits = (int)(Math.pow(10d, numberOfDigits) * r.nextDouble() - 1.0d);
        return digits / Math.pow(10.0d, decimalDigits);
    }
}
