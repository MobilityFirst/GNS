package edu.umass.cs.gns.activecode;

import java.io.IOException;
import java.util.concurrent.Callable;

import edu.umass.cs.gns.activecode.protocol.ActiveCodeParams;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.ValuesMap;

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
