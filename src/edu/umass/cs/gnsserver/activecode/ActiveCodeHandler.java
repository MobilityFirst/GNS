/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Misha Badov, Westy
 *
 */
package edu.umass.cs.gnsserver.activecode;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.script.ScriptException;

//import org.apache.commons.codec.binary.Base64;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnscommon.exceptions.server.FieldNotFoundException;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeParams;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveClient;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveHandler;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveWorker;
import edu.umass.cs.gnsserver.database.ColumnFieldType;
import edu.umass.cs.gnsserver.gnsapp.AppReconfigurableNodeOptions;
import edu.umass.cs.gnsserver.gnsapp.GNSApplicationInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.ActiveCode;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.UpdateOperation;
import edu.umass.cs.gnsserver.gnsapp.recordmap.BasicRecordMap;
import edu.umass.cs.gnsserver.gnsapp.recordmap.NameRecord;
import edu.umass.cs.gnsserver.interfaces.ActiveDBInterface;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.DelayProfiler;

/**
 * This class is the entry of activecode, it provides
 * the interface for GNS to run active code. It's creates
 * a threadpool to connect the real isolated active worker
 * to run active code. It also handles the misbehaviours.
 *
 * @author Zhaoyu Gao, Westy
 */

public class ActiveCodeHandler {	
	private ActiveDBInterface gnsApp;
	private static ClientPool clientPool;
	private static ThreadFactory threadFactory;
	private static ActiveCodeExecutor executorPool;
	private static ActiveCodeGuardian guard;
	private static ActiveCodeScheduler scheduler;
	
	private static final Logger logger = Logger.getLogger("ActiveGNS");
	
	// For instrument only, to tell whether a code is malicious or not 
	private static String noop_code;
	private static ActiveHandler handler;
	
	/**
	 * enable debug output
	 */
	public static final boolean enableDebugging = false; // AppReconfigurableNodeOptions.activeCodeEnableDebugging;
	

	/**
	 * @param gnsApp
	 * @return an app
	 */
	public static ActiveDBInterface getActiveDB(GNSApplicationInterface<?> gnsApp) {
		return new ActiveDBInterface() {

			private BasicRecordMap getDB() {
				return gnsApp.getDB();
			}

			
			private ValuesMap readSomeGuidFromLocal(String guid, String field){
				ValuesMap value = null;
				try {
					NameRecord record = NameRecord.getNameRecordMultiUserFields(getDB(), guid, ColumnFieldType.USER_JSON, field);
					value = record.getValuesMap();
				} catch (Exception e) {
					e.printStackTrace();
				}
				return value;
			}
			
			@Override
			public ValuesMap read(String querierGuid, String queriedGuid, String field) {
				ValuesMap value = null;
				if(querierGuid.equals(queriedGuid)){
					value = readSomeGuidFromLocal(queriedGuid, field);
				}else{
					/**
					 * TODO:
					 *  Do a common read here
					 *  <p>1. check whether its a local guid
					 *  <p>2. check ACL
					 *  <p>3. read
					 */
					value = readSomeGuidFromLocal(queriedGuid, field);
				}
				return value;
			}
			
			private boolean writeSomeGuidToLocal(String guid, String field, ValuesMap value){
				try {
					NameRecord nameRecord = NameRecord.getNameRecordMultiUserFields(gnsApp.getDB(), 
							guid, ColumnFieldType.USER_JSON, field);
					nameRecord.updateNameRecord(field, null, null, 0, value,
					         UpdateOperation.USER_JSON_REPLACE_OR_CREATE);
				} catch (Exception e) {
					e.printStackTrace();
					return false;
				}
				return true;
			}
			
			@Override
			public boolean write(String querierGuid, String queriedGuid, String field, ValuesMap valuesMap) {
				boolean wSuccess = false;
				if(querierGuid.equals(queriedGuid)){
					wSuccess = writeSomeGuidToLocal(queriedGuid, field, valuesMap);
				}else{
					/**
					 * TODO: 
					 *  Do a common write here
					 *  <p>1. check whether its a local guid
					 *  <p>2. check ACL
					 *  <p>3. write
					 */
					wSuccess = writeSomeGuidToLocal(queriedGuid, field, valuesMap);
				}
				return wSuccess;
			}
		};
	}
	
	/**
	 * Initializes an ActiveCodeHandler
	 * @param app
	 */
	public ActiveCodeHandler(ActiveDBInterface app) {
		String configFile = System.getProperty(AppReconfigurableNodeOptions.activeConfigFile);
		if(configFile != null){
			try {
				new ActiveCodeConfig(configFile);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}else{
			//Specify config file name and set properties
		}
		
		handler = new ActiveHandler(app, ActiveCodeConfig.activeCodeWorkerCount, ActiveCodeConfig.activeWorkerThreads);
		
		try {
			noop_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/noop.js")));
		} catch (IOException e) {
			e.printStackTrace();
		} 
		
		/*
		logger.setLevel(Level.INFO); 
		
		int numProcesses = 1; //AppReconfigurableNodeOptions.activeCodeWorkerCount;
		setGnsApp(app);
	    
		clientPool = new ClientPool(app); 
		(new Thread(clientPool)).start();		
		
		guard = new ActiveCodeGuardian(clientPool);
		
	    // Get the ThreadFactory implementation to use
	    threadFactory = new ActiveCodeThreadFactory(clientPool);
	    // Create the ThreadPoolExecutor
	    executorPool = new ActiveCodeExecutor(numProcesses, numProcesses, 0, TimeUnit.SECONDS, 
	    		new LinkedBlockingQueue<Runnable>(),
	    		threadFactory, 
	    		new ThreadPoolExecutor.DiscardPolicy(),
	    		guard,
	    		clientPool);
	    
	    // Start the processes
	    int numThread = executorPool.prestartAllCoreThreads();
	    logger.log(Level.INFO, "Initialize an active code handler and start "+numThread+" workers...");
	    //System.out.println("There are "+executorPool.getActiveCount() + " threads running, and " + executorPool.getPoolSize()+" threads available.");
	    
	    scheduler = new ActiveCodeScheduler(executorPool);
	    (new Thread(scheduler)).start();
	    
	    clientPool.startSpareWorkers();
	    */
	}
	
	
	/**
	 * Checks to see if this guid has active code for the specified action.
	 * @param valuesMap 
	 * @param action can be 'read' or 'write'
	 * @return whether or not there is active code
	 */
	public static boolean hasCode(ValuesMap valuesMap, String action) {
		try {
            return valuesMap.has(ActiveCode.getCodeField(action));
		} catch (Exception e) {
			return false;
		}
	}
	
	
	/**
	 * @param code
	 * @param guid
	 * @param field
	 * @param action
	 * @param valuesMap
	 * @param activeCodeTTL
	 * @return executed result
	 */
	public static ValuesMap runCode(String code, String guid, String field, String action, ValuesMap valuesMap, int activeCodeTTL) {
		try {
			return handler.runCode(guid, field, code, valuesMap, activeCodeTTL);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return valuesMap;
	}
	
	/**
	 * Runs the active code. Returns a {@link ValuesMap}.
     * 
	 * @param code64 base64 encoded active code, as stored in the db
	 * @param guid the guid
	 * @param field the field
	 * @param action either 'read' or 'write'
         * @param valuesMap
	 * @param activeCodeTTL the remaining active code TTL
	 * @return a Valuesmap
	 */
	public static ValuesMap runCode_old(String code64, String guid, String field, String action, ValuesMap valuesMap, int activeCodeTTL) {
		long startTime = System.nanoTime();		
		//Construct Value parameters
		String code = new String(Base64.decode(code64));
		String values = valuesMap.toString();
		
		ActiveCodeParams acp = new ActiveCodeParams(guid, field, action, code, values, activeCodeTTL);
		ActiveCodeFutureTask futureTask = new ActiveCodeFutureTask(new ActiveCodeTask(acp));
		if(!noop_code.equals(code)){
			futureTask.setMalicious(true);
		}
		
		DelayProfiler.updateDelayNano("activeHandlerPrepare", startTime);		

		Throwable thrown = null;
		ValuesMap result = null;
		
		
		if(ActiveCodeHandler.enableDebugging)
			logger.log(Level.INFO, "ActiveCodeHandler: prepare to submit the request to scheduler for guid "+guid);
		
		
		try {

			scheduler.submit(futureTask, guid);

			result = futureTask.get();
			
		} catch (ExecutionException ee) {
			//thrown = ee;
			
			scheduler.finish(guid);
			
			return valuesMap;
		} catch(CancellationException ce) {
			//thrown = ce;
			
			scheduler.finish(guid);
			
			return valuesMap;
		} catch(InterruptedException ie) {
			//thrown = ie;
			
			if(ActiveCodeHandler.enableDebugging)
				logger.log(Level.WARNING, ActiveCodeHandler.class.getSimpleName()
					+ " got interrupt for task " + futureTask + " thread "
					+ Thread.currentThread() + "; activeCount = "
					+ executorPool.getActiveCount() );
					//this synchronized call would affect the performance
					//+ "; actualActiveCount = "+ ActiveCodeTask.getActiveCount());
						
			try {
				ActiveCodeGuardian.cancelTask(futureTask);
			} catch(Exception | Error e) {
				//thrown = e;
				e.printStackTrace();
			}
			
			if(ActiveCodeHandler.enableDebugging)
				logger.log(Level.WARNING, ActiveCodeHandler.class.getSimpleName() + " after canceling " + futureTask + " getActiveCount = "
					+ executorPool.getActiveCount() );
			
			scheduler.finish(guid);
			
			return valuesMap;
		} finally {
			
			if(ActiveCodeHandler.enableDebugging)
				logger.log(Level.INFO , ActiveCodeHandler.class.getSimpleName()
					+ " finally block: " + futureTask + " thread "
					+ Thread.currentThread() + " activeCount = "
					+ executorPool.getActiveCount() );
					//+ "; actualActiveCount = "+ ActiveCodeTask.getActiveCount());
			
			if(thrown !=null)
				thrown.printStackTrace();
		}

		// if thrown != null, we never come here
		assert(thrown==null);
		
		if(ActiveCodeHandler.enableDebugging)
			logger.log(Level.INFO, ActiveCodeHandler.class.getSimpleName()
				+ ".runCode before final scheduler.finish(), activeCount = "
				+ executorPool.getActiveCount());
		
		try {
			scheduler.finish(guid);
		} catch (Exception | Error e) {
			e.printStackTrace();
		}
		
		DelayProfiler.updateDelayNano("activeHandler", startTime);
		
		if(ActiveCodeHandler.enableDebugging)
			logger.log(Level.INFO, ActiveCodeHandler.class.getSimpleName()
				+ ".runCode returning, activeCount = "
				+ executorPool.getActiveCount());
		
		if (result == null){
			result = valuesMap;
		}
		
	    return result;
	}
	
	protected ClientPool getClientPool(){
		return clientPool;
	}
	
	protected ActiveCodeExecutor getExecutor(){
		return executorPool;
	}
	
	protected static Logger getLogger(){
		return logger;
	}
	
	/***************************** TEST CODE *********************/
	public static void main(String[] args) throws InterruptedException, ExecutionException, IOException, JSONException {
		new ActiveCodeHandler(null);
		
		// initialize the parameters used in the test 
		JSONObject obj = new JSONObject();
		obj.put("testGuid", "success");
		ValuesMap valuesMap = new ValuesMap(obj);
		final String guid1 = "guid";
		final String field1 = "testGuid";
		final String read_action = "read";
		
		String noop_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/noop.js"))); 
		String noop_code64 = Base64.encodeToString(noop_code.getBytes("utf-8"), true);
		ActiveCodeHandler.runCode(noop_code64, guid1, field1, read_action, valuesMap, 100);
		
		int n = 1000000;
		long t = System.currentTimeMillis();
		for(int i=0; i<n; i++){
			ValuesMap result = ActiveCodeHandler.runCode(noop_code64, guid1, field1, read_action, valuesMap, 100);
		}
		long elapsed = System.currentTimeMillis() - t;
		System.out.println(String.format("it takes %d ms, avg_latency = %f us", elapsed, elapsed*1000.0/n));
		
	}
	
	/**
	 * @param args
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws IOException 
	 * @throws JSONException 
	 */
	public static void main1(String[] args) throws InterruptedException, ExecutionException, IOException, JSONException {
		// Initialize the handler and get the executor for instrument
		ActiveCodeHandler handler = new ActiveCodeHandler(null);
		ActiveCodeExecutor executor = handler.getExecutor();
		Thread.sleep(2000);
		
		// The variable to record the total number of tasks that should be completed
		int completed = 0;
		
		// initialize the parameters used in the test 
		JSONObject obj = new JSONObject();
		obj.put("testGuid", "success");
		ValuesMap valuesMap = new ValuesMap(obj);
		final String guid1 = "guid1";
		final String field1 = "testGuid";
		final String read_action = "read";
		
		try{		
		
			/************** Test normal code *************/
			System.out.println("################# Start testing normal active code ... ###################");
			String noop_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/noop.js"))); 
			String noop_code64 = Base64.encodeToString(noop_code.getBytes("utf-8"), true);
			
			ValuesMap result = ActiveCodeHandler.runCode(noop_code64, guid1, field1, read_action, valuesMap, 100);
			completed++;
			Thread.sleep(2000);
			System.out.println("Active count number is "+executor.getActiveCount()+
					", the number of completed tasks is "+executor.getCompletedTaskCount());
			
			// test result, # of completed tasks, and # of active threads
			System.out.println(result);
			
			assert(executor.getActiveCount() == 0);
			assert(executor.getCompletedTaskCount() == completed);
			System.out.println("############# TEST FOR NOOP PASSED! ##############\n\n");
			Thread.sleep(1000);
			
			
			/************** Test malicious code *************/
			System.out.println("################# Start testing malicious active code ... ###################");
			String mal_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/mal.js")));
			String mal_code64 = Base64.encodeToString(mal_code.getBytes("utf-8"), true);
					
			result = ActiveCodeHandler.runCode(mal_code64, "guid1", "testGuid", "read", valuesMap, 100);
			completed++;
			result = ActiveCodeHandler.runCode(noop_code64, guid1, field1, read_action, valuesMap, 100);
			completed++;
			Thread.sleep(2000);
			
			assert(executor.getActiveCount() == 0);
			assert(executor.getCompletedTaskCount() == completed);
			System.out.println("############# TEST FOR MALICOUS PASSED! ##############\n\n");
			Thread.sleep(1000);
			
			/************** Test chain code *************/
			System.out.println("################# Start testing chain active code ... ###################");
			String chain_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/chain-test.js")));
			String chain_code64 = Base64.encodeToString(chain_code.getBytes("utf-8"), true);
					
			result = ActiveCodeHandler.runCode(chain_code64, "guid1", "testGuid", "read", valuesMap, 100);
			completed++;
			completed++;
			result = ActiveCodeHandler.runCode(noop_code64, guid1, field1, read_action, valuesMap, 100);
			completed++;
			
			int count = 0;
			while(count <10){
				System.out.println("" + executor.getCompletedTaskCount() + " "
						+ executor.getActiveCount() + "; actualActiveCount = "
						+ ActiveCodeTask.getActiveCount());
				if(executor.getActiveCount()==0) break;
				Thread.sleep(1000);
				count++;
			}
			assert(executor.getActiveCount() == 0) : executor.getActiveCount();
			assert(executor.getCompletedTaskCount() == completed);
			System.out.println("############# TEST FOR CHAIN PASSED! ##############\n\n");
			Thread.sleep(1000);
		}catch(Exception | Error e){
			e.printStackTrace();
		}finally{
			clientPool.shutdown();
		}
		
		System.exit(0);
	}


	/**
	 * @return gnsApp
	 */
	public ActiveDBInterface getGnsApp() {
		return gnsApp;
	}


	/**
	 * @param gnsApp
	 */
	public void setGnsApp(ActiveDBInterface gnsApp) {
		this.gnsApp = gnsApp;
	}
}
