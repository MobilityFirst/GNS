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


import java.util.concurrent.Callable;

import org.json.JSONObject;

import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeParams;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.DelayProfiler;

/**
 * This is the task sent to active code worker.
 * 
 * @author Zhaoyu Gao
 */
public class ActiveCodeTask implements Callable<ValuesMap> {
	
    private final ActiveCodeParams acp;
    private ActiveCodeClient client;
    
    /**
     * Initialize a ActiveCodeTask
     * @param acp
     */
    public ActiveCodeTask(ActiveCodeParams acp) {
        this.acp = acp; 
    }
    
    protected ActiveCodeClient setClient(ActiveCodeClient client){
    	ActiveCodeClient prev = this.client;
    	this.client = client;
    	return prev;
    }
    
    protected ActiveCodeClient getClient(){
    	/*
    	 * If call() is called, client can not be null
    	 */
    	assert(client != null);
    	return client;
    }
    
    public String toString() {
    	return "    " +  this.getClass().getSimpleName() + this.client.myID + ":"+this.acp.toString();
    }
    
	private static int activeCount = 0;
	synchronized static void incrNumActiveCount() {
		activeCount++;
	}
	synchronized static void decrNumActiveCount() {
		activeCount--;
	}
	synchronized static int getActiveCount() {
		return activeCount;
	}

    private static final long MAX_CLIENT_READY_WAIT_TIME = 500;
    @Override
    /**
     * Called by the ThreadPoolExecutor to run the active code task
     */
    public ValuesMap call() throws InterruptedException{
    	incrNumActiveCount();
    	if(ActiveCodeHandler.enableDebugging)
    		System.out.println(this + " STARTING");
    	
    	ValuesMap result = null;
    	Throwable thrown = null;
    	try {
	    	long startTime = System.nanoTime();
	    	//System.out.println("Start running the task with the thread "+Thread.currentThread());
	    	if(ActiveCodeHandler.enableDebugging)
	    		System.out.println(this + " waiting on client to be ready");
	    	//check the state of the client's worker
	    	if(!client.isReady()){
	    		// wait until it's ready
	    		synchronized(client){
	    			client.wait(MAX_CLIENT_READY_WAIT_TIME);
	    		}
	    	}
	    	if (!client.isReady()){
	    		/*
	    		 *  if client is still not ready, then we are done, don't submit the
	    		 *  task to the worker. 
	    		 */
	    		return new ValuesMap(new JSONObject(acp.getValuesMapString()));
	    	}
	    	
	    	
	    	/*
	    	 * Invariant: client must be ready to proceed
	    	 */
	    	assert(client.isReady());
	    	
	    	if(ActiveCodeHandler.enableDebugging)
	    		System.out.println( this + " client ready; before runActiveCode");
	    	
	    	DelayProfiler.updateDelayNano("activeCodeBeforeSending", startTime);
	  	
	    	if(acp != null) {
	    		result = client.runActiveCode(acp);
	    	}
	    	if(ActiveCodeHandler.enableDebugging)
	    		System.out.println(this + " after runActiveCode");
	    	
	    	DelayProfiler.updateDelayNano("activeCodeTask", startTime);
	    	
    	} catch(Exception | Error e) {
    		thrown = e;
    		if(ActiveCodeHandler.enableDebugging)
    			System.out.println(this + " re-throwing uncaught exception/error " + e);
    		e.printStackTrace();
    		try {
				throw e;
			} catch (Throwable e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
    	}
   		finally {
    		// arun
    		//assert(System.currentTimeMillis() - this.startTime < 2000);
    		if(thrown != null) GNS.getLogger().severe(thrown.toString());
    		decrNumActiveCount();
    		if(ActiveCodeHandler.enableDebugging){
    			System.out.println(this + " finally block just before returning result " + result );
    			System.out.println(this + " ENDING");
    		}
    	}
    	return result;    	
    }
    
}
