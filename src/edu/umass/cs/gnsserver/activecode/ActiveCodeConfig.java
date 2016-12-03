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
		System.out.println("Initial active code options...");
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
	  public static int activeCodeWorkerCount = 1;
	  
	  /**
	   * Number of threads running in each worker
	   */
	  public static int activeWorkerThreads = 10;
	  
	  /**
	   * switch between blocking and non-blocking design
	   */
	  public static boolean acitveCodeBlockingEnabled = false;
	  	  
	  /**
	   * Worker heap size
	   */
	  public static int activeWorkerHeapSize = 128;
	  
	  /**
	   * To use GeoIP API, put the binary DB file to the proper path.
	   */
	  public static String activeGeoIPFilePath = "conf/activeCode/GeoLite2-City.mmdb";
	  
	  /**
	   * Default request timeout (ms)
	   */
	  public static int activeRequestTimeout = 2000;
	  
	  /**
	   * True if crash on OutOfMemoryError is enabled
	   */
	  public static boolean activeCrashEnabled = true;
	  
	  /**
	   * Deprecated: Number of spare workers.
	   */
	  public static int activeCodeSpareWorker = 0;
	  
	  /**
	   * True if timeout is enabled, i.e., ActiveCodeGuardian thread will run.
	   */
	  public static boolean activeCodeEnableTimeout = true;
	  
	  /**
	   * Deprecated: Enable debug message in active code package
	   */
	  public static boolean activeCodeEnableDebugging = false;
	  
	  
	  private static final String ACTIVE_CODE_WORKER_COUNT = "ACTIVE_CODE_WORKER_COUNT";
	  
	  private static final String ACTIVE_WORKER_THREADS = "ACTIVE_WORKER_THREADS";
	  
	  private static final String ACTIVE_CODE_BLOCKING_ENABLED = "ACTIVE_CODE_BLOCKING_ENABLED"; 
	  
	  private static final String ACTIVE_WORKER_HEAP_SIZE = "ACTIVE_WORKER_HEAP_SIZE";
	  
	  private static final String ACTIVE_GEOIP_FILE_PATH = "ACTIVE_GEOIP_FILE_PATH";
	  
	  private static final String ACTIVE_REQUEST_TIMEOUT = "ACTIVE_REQUEST_TIMEOUT";
	  
	  private static final String ACTIVE_CRASH_ENEABLED = "ACTIVE_CRASH_ENEABLED";
	  
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
		    
		    if(allValues.containsKey(ACTIVE_CODE_BLOCKING_ENABLED)) {
		    	acitveCodeBlockingEnabled = Boolean.parseBoolean(allValues.getProperty(ACTIVE_CODE_BLOCKING_ENABLED));
		    }
		    
		    if(allValues.containsKey(ACTIVE_WORKER_HEAP_SIZE)){
		    	activeWorkerHeapSize = Integer.parseInt(allValues.getProperty(ACTIVE_WORKER_HEAP_SIZE));
		    }
		    
		    if(allValues.containsKey(ACTIVE_REQUEST_TIMEOUT)) {
		    	activeRequestTimeout = Integer.parseInt(allValues.getProperty(ACTIVE_REQUEST_TIMEOUT));
		    }
		    
		    if(allValues.containsKey(ACTIVE_CRASH_ENEABLED)) {
		    	activeCrashEnabled = Boolean.parseBoolean(allValues.getProperty(ACTIVE_CRASH_ENEABLED));
		    }
		    
		    if(allValues.containsKey(ACTIVE_GEOIP_FILE_PATH)) {
		    	activeGeoIPFilePath = allValues.getProperty(ACTIVE_GEOIP_FILE_PATH);
		    }
	  }
	 
	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException{
		
		@SuppressWarnings("unused")
		ActiveCodeConfig conf = new ActiveCodeConfig("conf/activeCode/active.properties");
		System.out.println(activeCodeWorkerCount+" "+activeWorkerThreads);
		
	}
	
}
