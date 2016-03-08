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


import java.net.DatagramSocket;
import java.util.concurrent.Callable;
import java.util.logging.Level;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeMessage;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeParams;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeQueryRequest;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeQueryResponse;
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
    
    //For instrument only
    protected ActiveCodeParams getParams(){
    	return acp;
    }
    
    protected ActiveCodeClient setClient(ActiveCodeClient client){
    	ActiveCodeClient prev = this.client;
    	this.client = client;
    	return prev;
    }
    
    /**
	 * client could be null, be careful when use this method
	 */
    protected ActiveCodeClient getClient(){    	
    	return client;
    }
    
    public String toString() {
    	return "    " +  this.getClass().getSimpleName() + (this.client!=null?this.client.myID:-1) + ":"+this.acp.toString();
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

    @Override
    /**
     * Called by the ThreadPoolExecutor to run the active code task
     */
    public ValuesMap call() throws InterruptedException{
    	
    	if(ActiveCodeHandler.enableDebugging){
    		//incrNumActiveCount();
    		ActiveCodeHandler.getLogger().log(Level.INFO, this + " STARTING");
    	}
    	
    	ValuesMap result = null;
    	Throwable thrown = null;
    	try {
	    	long startTime = System.nanoTime();
	  	
	    	if(acp != null) {
	    		ActiveCodeMessage acm = new ActiveCodeMessage();
	    		acm.setAcp(acp);
	    		
	    		result = client.submitRequest(acm);
	    	}
	    	
	    	if(ActiveCodeHandler.enableDebugging)
	    		ActiveCodeHandler.getLogger().log(Level.INFO, this + " after runActiveCode");
	    	
	    	DelayProfiler.updateDelayNano("activeCodeTask", startTime);
	    	
	    	
    	} catch(Exception | Error e) {
    		thrown = e;
    		
    		if(ActiveCodeHandler.enableDebugging)
    			ActiveCodeHandler.getLogger().log(Level.SEVERE, this + " re-throwing uncaught exception/error " + e);
    			
    		
    		e.printStackTrace();
			throw e;			
    	}
   		finally {
    		if(thrown != null) GNS.getLogger().severe(thrown.toString());
    		
    		if(ActiveCodeHandler.enableDebugging){
    			//decrNumActiveCount();
    			ActiveCodeHandler.getLogger().log(Level.INFO, this + " finally block just before returning result " + result );
    			ActiveCodeHandler.getLogger().log(Level.INFO, this + " ENDING");
    		}
    		
    	}
    	return result;    	
    }
    
    
}
