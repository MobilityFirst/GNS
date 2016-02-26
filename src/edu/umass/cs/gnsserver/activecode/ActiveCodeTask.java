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

import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeParams;
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
    private long startTime = System.currentTimeMillis();
    
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
    
    @Override
    /**
     * Called by the ThreadPoolExecutor to run the active code task
     */
    public ValuesMap call() throws InterruptedException{ 
    	try {
	    	long startTime = System.nanoTime();
	    	//System.out.println("Start running the task with the thread "+Thread.currentThread());
	    	
	    	ValuesMap result = null;
	    		    	
	    	//check the state of the client's worker
	    	while(!client.isReady()){
	    		// wait until it's ready
	    		synchronized(client){
	    			client.wait();
	    		}
	    	}
	    	DelayProfiler.updateDelayNano("activeCodeBeforeSending", startTime);
	  	
	    	if(acp != null) {
	    		result = client.runActiveCode(acp);
	    	}
	    	
	    	DelayProfiler.updateDelayNano("activeCodeTask", startTime);
	    	
	    	return result;    	
    	} finally {
    		// arun
    		assert(System.currentTimeMillis() - this.startTime < 2000);
    	}
    }
    
    
    protected ActiveCodeClient getClient(){
    	/*
    	 * If call() is called, client can not be null
    	 */
    	assert(client != null);
    	return client;
    }
}
