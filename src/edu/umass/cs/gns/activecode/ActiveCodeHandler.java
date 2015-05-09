package edu.umass.cs.gns.activecode;


import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.binary.Base64;
import org.json.JSONException;

import edu.umass.cs.gns.activecode.protocol.ActiveCodeParams;
import edu.umass.cs.gns.clientsupport.ActiveCode;
import edu.umass.cs.gns.clientsupport.FieldMetaData;
import edu.umass.cs.gns.clientsupport.MetaDataTypeName;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.nsdesign.GnsApplicationInterface;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurable;
import edu.umass.cs.gns.nsdesign.recordmap.NameRecord;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.ValuesMap;

public class ActiveCodeHandler {	
	GnsApplicationInterface gnsApp;
	ClientPool clientPool;
	ThreadPoolExecutor executorPool;
	Map<String, Long> blacklist;
	long blacklistSeconds;
	
	public static final int NANOSECONDS_PER_SEC = 1000000000;
	
	public ActiveCodeHandler(GnsApplicationInterface app, int numProcesses, long blacklistSeconds) {
		gnsApp = app;
		clientPool = new ClientPool(app); 
	    // Get the ThreadFactory implementation to use
	    ThreadFactory threadFactory = new ActiveCodeThreadFactory(clientPool);
	    // Create the ThreadPoolExecutor
	    executorPool = new ActiveCodeExecutor(numProcesses, numProcesses, 0, TimeUnit.SECONDS, 
	    		new LinkedBlockingQueue<Runnable>(100), 
	    		// new SynchronousQueue<Runnable>(),
	    		threadFactory, new ThreadPoolExecutor.DiscardPolicy());
	    // Start the processes
	    executorPool.prestartAllCoreThreads();
	    // Blacklist init
		blacklist = new HashMap<String, Long>();
		this.blacklistSeconds = blacklistSeconds;
	}
	
	/**
	 * Checks to see if this guid has active code for the specified action.
	 * @param nameRecord
	 * @param action can be 'read' or 'write'
	 * @return whether or not there is active code
	 */
	public boolean hasCode(NameRecord nameRecord, String action) {
		try {
			ResultValue code = nameRecord.getKeyAsArray(ActiveCode.codeField(action));
			return code != null && !code.isEmpty();
		} catch (FieldNotFoundException e) {
			return false;
		}
	}
	
	/**
	 * Checks to see if all of the workers are busy
	 * @return
	 */
	public boolean isPoolBusy() {
		return executorPool.getActiveCount() == executorPool.getMaximumPoolSize();
	}
	
	/**
	 * Checks to see if the guid is currently blacklisted.
	 * @param guid
	 * @return
	 */
	public boolean isBlacklisted(String guid) {
		if(!blacklist.containsKey(guid))
			return false;
		
		long blacklistedAt = blacklist.get(guid);
		
		return (System.nanoTime() - blacklistedAt) < (blacklistSeconds * NANOSECONDS_PER_SEC);
	}
	
	/**
	 * Adds the guid to the blacklist
	 * @param guid
	 */
	public void addToBlacklist(String guid) {
		blacklist.put(guid, System.nanoTime());
	}
	
	/**
	 * Runs the active code
	 * @param code64 base64 encoded active code, as stored in the db
	 * @param guid the guid
	 * @param field the field
	 * @param action either 'read' or 'write'
	 * @param value the value previously read or about to written
	 * @param activeCodeTTL the remaining active code TTL
	 * @return
	 */
	public ValuesMap runCode(String code64, String guid, String field, String action, ValuesMap valuesMap, int activeCodeTTL) {		
		String code = new String(Base64.decodeBase64(code64));
		String values = valuesMap.toString();
		ValuesMap result = null;
		
		ActiveCodeParams acp = new ActiveCodeParams(guid, field, action, code, values, activeCodeTTL);
		FutureTask<ValuesMap> futureTask = new FutureTask<ValuesMap>(new ActiveCodeTask(acp, clientPool));
		
		// If the guid is blacklisted, just return immediately
		if(isBlacklisted(guid)) {
			System.out.println("Guid " + guid + " is blacklisted from running code!");
			return valuesMap;
		}
		
		// Only run if there are free workers and queue space
		// This prevents excessive CPU usage
		if(executorPool.getPoolSize() > 0 &&
				executorPool.getQueue().remainingCapacity() > 0) {
			executorPool.execute(futureTask);
	    
		    try {
				result = futureTask.get();
			} catch (ExecutionException e) {
				System.out.println("Added " + guid + " to blacklist!");
				addToBlacklist(guid);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} else {
			System.out.println("Rejecting task!");
		}
		
	    return result;
	}
}
