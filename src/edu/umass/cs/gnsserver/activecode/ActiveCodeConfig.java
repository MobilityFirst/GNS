package edu.umass.cs.gnsserver.activecode;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author gaozy
 *
 */
public class ActiveCodeConfig {

	/**
	 * @param propFileName
	 * @throws IOException
	 */
	public ActiveCodeConfig(String propFileName) throws IOException{
		Properties prop = new Properties();
		InputStream inputStream = new FileInputStream(new File(propFileName));
		if(inputStream != null){
			prop.load(inputStream);
		}
		
		initializeFromOption(prop);
	}
	
	
	 /**
	   * Number of active code worker.
	   */
	  public static int activeCodeWorkerCount = 4;
	  
	  /**
	   * Number of threads running in each worker
	   */
	  public static int activeWorkerThreads = 5;
	  
	  /**
	   * Number of spare workers.
	   */
	  public static int activeCodeSpareWorker = 0;
	  
	  /**
	   * True if timeout is enabled, i.e., ActiveCodeGuardian thread will run.
	   */
	  public static boolean activeCodeEnableTimeout = true;
	  /**
	   * Enable debug message in active code package
	   */
	  public static boolean activeCodeEnableDebugging = false;
	  
	  
	  private static final String ACTIVE_CODE_WORKER_COUNT = "ACTIVE_CODE_WORKER_COUNT";
	  
	  private static final String ACTIVE_WORKER_THREADS = "ACTIVE_WORKER_THREADS";
	  
	  private static final String ACTIVE_CODE_SPARE_WORKER = "ACTIVE_CODE_SPARE_WORKER";
	  
	  private static final String ACTIVE_CODE_ENABLE_TIMEOUT = "ACTIVE_CODE_ENABLE_TIMEOUT";
	  
	  private static final String ACTIVE_CODE_ENABLE_DEBUGGING = "ACTIVE_CODE_ENABLE_DEBUGGING";
	  
	  
	/**
	 * @param allValues
	 */
	public static void initializeFromOption(Properties allValues){
		    if (allValues.containsKey(ACTIVE_CODE_WORKER_COUNT)) {
		    	activeCodeWorkerCount = Integer.parseInt(allValues.getProperty(ACTIVE_CODE_WORKER_COUNT));
		    }
		    
		    if (allValues.containsKey(ACTIVE_WORKER_THREADS)) {
		    	activeWorkerThreads = Integer.parseInt(allValues.getProperty(ACTIVE_WORKER_THREADS));
		    }
		    
		    if (allValues.containsKey(ACTIVE_CODE_SPARE_WORKER)) {
		    	activeCodeSpareWorker = Integer.parseInt(allValues.getProperty(ACTIVE_CODE_SPARE_WORKER));
		    }
		    
		    if (allValues.containsKey(ACTIVE_CODE_ENABLE_TIMEOUT)) {
		    	activeCodeEnableTimeout = Boolean.parseBoolean(allValues.getProperty(ACTIVE_CODE_ENABLE_TIMEOUT));
		    }
		    
		    if (allValues.containsKey(ACTIVE_CODE_ENABLE_DEBUGGING)) {
		    	activeCodeEnableDebugging = Boolean.parseBoolean(allValues.getProperty(ACTIVE_CODE_ENABLE_DEBUGGING));
		    }
	  }
	 
	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException{
		
		ActiveCodeConfig conf = new ActiveCodeConfig("conf/activeCode/active.properties");
		System.out.println(activeCodeWorkerCount+" "+activeWorkerThreads);
		
	}
	
}
