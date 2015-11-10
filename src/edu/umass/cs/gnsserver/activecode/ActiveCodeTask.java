/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 */
package edu.umass.cs.gnsserver.activecode;

import java.util.concurrent.Callable;

import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeParams;
import edu.umass.cs.gnsserver.utils.ValuesMap;


public class ActiveCodeTask implements Callable<ValuesMap> {

    private ActiveCodeParams acp;
    private ClientPool clientPool;
    
    public ActiveCodeTask(ActiveCodeParams acp, ClientPool clientPool) {
        this.acp = acp; 
        this.clientPool = clientPool;
    }

    @Override
    /**
     * Called by the ThreadPoolExecutor to run the active code task
     */
    public ValuesMap call() throws ActiveCodeException {
    	ActiveCodeClient client = clientPool.getClient(Thread.currentThread());
    	ValuesMap result = null;
    	
    	if(acp != null) {	
    		result = client.runActiveCode(acp, false);
    	}
    	
    	return result;
    }
}
