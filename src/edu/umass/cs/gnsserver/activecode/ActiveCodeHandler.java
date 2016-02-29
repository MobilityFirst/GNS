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

import edu.umass.cs.gnscommon.utils.Base64;

//import org.apache.commons.codec.binary.Base64;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeParams;
import edu.umass.cs.gnsserver.exceptions.FieldNotFoundException;
import edu.umass.cs.gnsserver.gnsApp.AppReconfigurableNodeOptions;
import edu.umass.cs.gnsserver.gnsApp.GnsApplicationInterface;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.ActiveCode;
import edu.umass.cs.gnsserver.gnsApp.recordmap.NameRecord;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.DelayProfiler;

/**
 * This class is the entry of activecode, it provides
 * the interface for GNS to run active code. It's creates
 * a threadpool to connect the real isolated active worker 
 * to run active code. It also handles the misbehaviours.
 * 
 * @author Zhaoyu Gao
 */
public class ActiveCodeHandler {	
	private GnsApplicationInterface<?> gnsApp;
	private static ClientPool clientPool;
	private static ThreadFactory threadFactory;
	private static ActiveCodeExecutor executorPool;
	private static ActiveCodeGuardian guard;
	private static ActiveCodeScheduler scheduler;
	
	
	/**
	 * Initializes an ActiveCodeHandler
	 * @param app
	 */
	public ActiveCodeHandler(GnsApplicationInterface<String> app) {
		int numProcesses = AppReconfigurableNodeOptions.activeCodeWorkerCount;
		gnsApp = app;
	    
		clientPool = new ClientPool(app); 
		(new Thread(clientPool)).start();
		
		
		guard = new ActiveCodeGuardian(clientPool);
		if (AppReconfigurableNodeOptions.activeCodeEnableTimeout){
			(new Thread(guard)).start();
		}
		
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
	    System.out.println("##################### "+numThread+" threads have been started! ##################");
	    //System.out.println("There are "+executorPool.getActiveCount() + " threads running, and " + executorPool.getPoolSize()+" threads available.");
	    
	    scheduler = new ActiveCodeScheduler(executorPool);
	    (new Thread(scheduler)).start();
	    
	    clientPool.startSpareWorkers();
	}
	
	
	/**
	 * Checks to see if this guid has active code for the specified action.
	 * @param nameRecord
	 * @param action can be 'read' or 'write'
	 * @return whether or not there is active code
	 */
	public static boolean hasCode(NameRecord nameRecord, String action) {
		try {
            return nameRecord.getValuesMap().has(ActiveCode.codeField(action));
		} catch (FieldNotFoundException e) {
			return false;
		}
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
	public static ValuesMap runCode(String code64, String guid, String field, String action, ValuesMap valuesMap, int activeCodeTTL) {
		long startTime = System.nanoTime();		
		//Construct Value parameters
		String code = new String(Base64.decode(code64));
		String values = valuesMap.toString();
		
		//System.out.println("Got the request from guid "+guid+" for the field "+field+" with original value "+valuesMap);
		
		ActiveCodeParams acp = new ActiveCodeParams(guid, field, action, code, values, activeCodeTTL);
		ActiveCodeFutureTask futureTask = new ActiveCodeFutureTask(new ActiveCodeTask(acp));
		
		DelayProfiler.updateDelayNano("activeHandlerPrepare", startTime);		

		Throwable thrown = null;
		ValuesMap result = null;

		try {

			scheduler.submit(futureTask, guid);

			result = futureTask.get();
		} catch (ExecutionException ee) {
			//System.out.println("Execution "+guid+" Task "+futureTask);
			//ee.printStackTrace();
			//thrown = ee;
			try {
			scheduler.finish(guid);
			} catch (Exception | Error e) {
				e.printStackTrace();
			}
			return valuesMap;
		} catch(CancellationException ce) {
			//thrown = ce;
			//System.out.println("Cancel "+guid+" Task "+futureTask);
			//ce.printStackTrace();
			try {
				scheduler.finish(guid);
			} catch (Exception | Error e) {
				e.printStackTrace();
			}
			return valuesMap;
		} catch(InterruptedException ie) {
			//thrown = ie;
			System.out.println(ActiveCodeHandler.class.getSimpleName()
					+ " got interrupt for task " + futureTask + " thread "
					+ Thread.currentThread() + "; activeCount = "
					+ executorPool.getActiveCount() + "; actualActiveCount = "
					+ ActiveCodeTask.getActiveCount());
			try {
				guard.cancelTask(futureTask, true);
			} catch(Exception | Error e) {
				//thrown = e;
				e.printStackTrace();
			}
			System.out.println(ActiveCodeHandler.class.getSimpleName() + " after canceling " + futureTask + " getActiveCount = "
					+ executorPool.getActiveCount() + "; actualActiveCount = " + ActiveCodeTask.getActiveCount());
			try {
			scheduler.finish(guid);
			} catch (Exception | Error e) {
				//thrown = e;
				e.printStackTrace();
			}
			return valuesMap;
		} catch (Exception | Error e){
			thrown = e;
			e.printStackTrace();
		} finally {
			System.out.println(ActiveCodeHandler.class.getSimpleName()
					+ " finally block: " + futureTask + " thread "
					+ Thread.currentThread() + " activeCount = "
					+ executorPool.getActiveCount() + "; actualActiveCount = "
					+ ActiveCodeTask.getActiveCount());
			if(thrown !=null)
				thrown.printStackTrace();
		}

		// if thrown != null, we never come here
		assert(thrown==null);
		
		System.out.println(ActiveCodeHandler.class.getSimpleName()
				+ ".runCode before final scheduler.finish(), activeCount = "
				+ executorPool.getActiveCount());
		try {
		scheduler.finish(guid);
		} catch (Exception | Error e) {
			e.printStackTrace();
		}
		
		DelayProfiler.updateDelayNano("activeHandler", startTime);
		System.out.println(ActiveCodeHandler.class.getSimpleName()
				+ ".runCode returning, activeCount = "
				+ executorPool.getActiveCount());
		 
	    return result;
		
	}
	
	public ActiveCodeExecutor getExecutor(){
		return executorPool;
	}
	
	
	/***************************** TEST CODE *********************/
	
	
	/**
	 * @param args
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws IOException 
	 * @throws JSONException 
	 */
	public static void main(String[] args) throws InterruptedException, ExecutionException, IOException, JSONException {
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
			Thread.sleep(2000);
			completed++;
			
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
			Thread.sleep(2000); 
			completed++;
			
			assert(executor.getActiveCount() == 0);
			assert(executor.getCompletedTaskCount() == completed);
			System.out.println("############# TEST FOR MALICOUS PASSED! ##############\n\n");
			Thread.sleep(1000);
			
			/************** Test chain code *************/
			System.out.println("################# Start testing chain active code ... ###################");
			String chain_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/chain.js")));
			String chain_code64 = Base64.encodeToString(chain_code.getBytes("utf-8"), true);
					
			result = ActiveCodeHandler.runCode(chain_code64, "guid1", "testGuid", "read", valuesMap, 100);
			Thread.sleep(2000); 
			completed++;
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
}
