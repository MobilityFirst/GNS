package edu.umass.cs.gnsserver.activecode.prototype.unblocking;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage;

import javax.script.ScriptException;
import java.util.concurrent.Callable;
import java.util.logging.Level;


public class ActiveWorkerTask implements Callable<ActiveMessage>  {
	
	final ActiveNonBlockingRunner runner;
	final ActiveMessage request;
	
	ActiveWorkerTask(ActiveNonBlockingRunner runner, ActiveMessage request){
		this.runner = runner;
		this.request = request;
	}
		
	@Override
	public ActiveMessage call() {
		ActiveMessage response = null;
		try {
			response = new ActiveMessage(request.getId(), 
					runner.runCode(request.getGuid(), request.getAccessor(), request.getCode(), request.getValue(), request.getTtl(), request.getId()),
					null);
		} catch (NoSuchMethodException | ScriptException e) {	
			//e.printStackTrace();
			ActiveNonBlockingWorker.getLogger().log(Level.FINE, 
					"get an exception {0} when executing request {1} with code {2}", 
					new Object[]{e, request, request.getCode()});
			response = new ActiveMessage(request.getId(), null, e.getMessage());
		}

		return response;
	}

}
