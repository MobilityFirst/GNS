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
	
    private ActiveCodeParams acp;
    private ClientPool clientPool;
    private ActiveCodeGuardian guard;
    /**
     * Initialize a ActiveCodeTask
     * @param acp
     * @param clientPool
     */
    public ActiveCodeTask(ActiveCodeParams acp, ClientPool clientPool, ActiveCodeGuardian guard) {
        this.acp = acp; 
        this.clientPool = clientPool;
        this.guard = guard;
    }
    
    protected void deregisterTask(){
    	guard.removeThread(this);
    }
    
    @Override
    /**
     * Called by the ThreadPoolExecutor to run the active code task
     */
    public ValuesMap call() throws InterruptedException{  
    	
    	long startTime = System.nanoTime();
    	long pid = Thread.currentThread().getId();
    	//System.out.println("Start running the task with the thread "+Thread.currentThread());
    	
    	ValuesMap result = null;
    	ActiveCodeClient client = clientPool.getClient(pid);
    	int port = client.getPort();
    	long startWait = System.nanoTime();
    	
    	//check the state of the client's worker
    	while(!ClientPool.getClientState(port)){
    		// wait until it's ready
    		clientPool.waitFor();
    	}
    	if(System.nanoTime() - startWait > 1000){
    		DelayProfiler.updateDelayNano("activeCodeTaskWait", startWait);
    	}
    	
    	guard.registerThread(this, Thread.currentThread());   	
    	if(acp != null) {
    		result = client.runActiveCode(acp);
    	}    	
    	guard.removeThread(this);
    	
    	DelayProfiler.updateDelayNano("activeTask", startTime);
    	
    	return result;
    }
}
