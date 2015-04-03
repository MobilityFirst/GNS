package edu.umass.cs.gns.activecode;

import edu.umass.cs.gns.activecode.protocol.ActiveCodeParams;
import edu.umass.cs.gns.activecode.protocol.ActiveCodeResult;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.ValuesMap;

public class ActiveCodeTask implements Runnable {

    private ActiveCodeParams acp;
    private ActiveCodeResult acr;
    private ClientPool clientPool;
    
    public ActiveCodeTask(ActiveCodeParams acp, ActiveCodeResult acr, ClientPool clientPool) {
        this.acp = acp; 
        this.acr = acr;
        this.clientPool = clientPool;
    }

    @Override
    public void run() {
    	ActiveCodeClient client = clientPool.getClient(Thread.currentThread());
    	
    	if(acp != null && acr != null) {
    		ValuesMap result = client.runActiveCode(acp);
	        acr.setResult(result);
	        acr.finished();
    	}
    }
}
