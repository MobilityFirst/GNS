package edu.umass.cs.gns.activecode;


import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.binary.Base64;
import org.json.JSONException;

import edu.umass.cs.gns.activecode.protocol.ActiveCodeParams;
import edu.umass.cs.gns.activecode.protocol.ActiveCodeResult;
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
	
	public ActiveCodeHandler(GnsApplicationInterface app, int numProcesses) {
		gnsApp = app;
		clientPool = new ClientPool(app); 
	    // Get the ThreadFactory implementation to use
	    ThreadFactory threadFactory = new ActiveCodeThreadFactory(clientPool);
	    // Create the ThreadPoolExecutor
	    executorPool = new ThreadPoolExecutor(numProcesses, numProcesses, 0, TimeUnit.SECONDS, 
	    		new LinkedBlockingQueue<Runnable>(), threadFactory);
	    // Start the processes
	    executorPool.prestartAllCoreThreads();
	}
	
	/**
	 * Checks to see if this guid has active code for the specified action.
	 * @param nameRecord
	 * @param action can be 'read' or 'write'
	 * @return
	 */
	public boolean hasCode(NameRecord nameRecord, String action) {
		try {
			String code = nameRecord.getValuesMap().getString(ActiveCode.codeField(action));
			return code != null && !code.trim().isEmpty();
		} catch (JSONException | FieldNotFoundException e) {
			return false;
		}
	}
	
	/**
	 * Runs the active code
	 * @param code64 base64 encoded active code, as stored in the db
	 * @param guid
	 * @param field
	 * @param action
	 * @param value
	 * @param hopLimit
	 * @return
	 */
	public ValuesMap runCode(String code64, String guid, String field, String action, ValuesMap valuesMap, int hopLimit) {
		String code = new String(Base64.decodeBase64(code64));
		String values = valuesMap.toString();
		
		ActiveCodeParams acp = new ActiveCodeParams(guid, field, action, code, values, hopLimit);
		ActiveCodeResult acr = new ActiveCodeResult();
	    executorPool.execute(new ActiveCodeTask(acp, acr, clientPool));
	    return acr.getResult();
	}
}
