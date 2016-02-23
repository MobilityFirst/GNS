package edu.umass.cs.gnsclient.examples;

import java.util.ArrayList;

/**
 * @author gaozy
 *
 */
public class MessageStats {
	protected static final ArrayList<Long> latency = new ArrayList<Long>();
	protected static final ArrayList<Long> mal_request = new ArrayList<Long>();
	
	/**
	 * total time is 10s
	 */
	public static int DURATION = 10;
    
	/**
     * benign active code takes 5ms
     */
    public static int INTERVAL = 5;
   
    /**
     * malicious active code takes 1000ms
     */
    public static int MAL_INTERVAL = 1000;
    
    /**
     * The depth of malicious code
     */
    public static int DEPTH = 1;
}
