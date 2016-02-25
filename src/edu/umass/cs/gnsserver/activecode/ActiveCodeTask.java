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

import edu.umass.cs.gnsserver.activecode.interfaces.ClientPoolInterface;
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
    private final ClientPoolInterface clientPool;
    private ActiveCodeClient client;
    private boolean running = false;
    private long startTime = System.currentTimeMillis();
    
    /**
     * Initialize a ActiveCodeTask
     * @param acp
     * @param clientPool
     */
    public ActiveCodeTask(ActiveCodeParams acp, ClientPoolInterface clientPool) {
        this.acp = acp; 
        this.clientPool = clientPool;
    }
    
    
    @Override
    /**
     * Called by the ThreadPoolExecutor to run the active code task
     */
    public ValuesMap call() throws InterruptedException{ 
    	try {
	    	running = true;
	
	    	long startTime = System.nanoTime();
	    	long pid = Thread.currentThread().getId();
	    	//System.out.println("Start running the task with the thread "+Thread.currentThread());
	    	
	    	ValuesMap result = null;
	    	client = clientPool.getClient(pid);
	    	
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
    
    protected Thread getThread(){
    	//Invariant: this task must be running when being called this method 
    	assert(running == true);
    	return Thread.currentThread();
    }
    
    protected ActiveCodeClient getClient(){
    	assert(client != null);
    	return client;
    }
    
    protected void interrupt(){
    	
    }
}
