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
    /**
     * Initialize a ActiveCodeTask
     * @param acp
     * @param clientPool
     */
    public ActiveCodeTask(ActiveCodeParams acp, ClientPool clientPool) {
        this.acp = acp; 
        this.clientPool = clientPool;
    }

    @Override
    /**
     * Called by the ThreadPoolExecutor to run the active code task
     */
    public ValuesMap call() throws ActiveCodeException{  
    	long startTime = System.nanoTime();
    	long pid = Thread.currentThread().getId();
    	
    	ActiveCodeClient client = clientPool.getClient(pid);
    	ValuesMap result = null;
    	synchronized(client){
    		while(!client.isReady()){   		
    			client.waitLock();
    		}
    	}
    	System.out.println("Get the worker!" + client);
    	if(acp != null) {
    		result = client.runActiveCode(acp, false);
    	}
    	DelayProfiler.updateDelayNano("activeTask", startTime);
    	return result;
    }
}
