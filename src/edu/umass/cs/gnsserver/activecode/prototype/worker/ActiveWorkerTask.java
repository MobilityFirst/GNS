package edu.umass.cs.gnsserver.activecode.prototype.worker;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import javax.script.ScriptException;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Channel;

/**
 * @author gaozy
 *
 */
public class ActiveWorkerTask implements Callable<ActiveMessage>  {
	
	final ActiveRunner runner;
	final ActiveMessage request;
	
	ActiveWorkerTask(ActiveRunner runner, ActiveMessage request){
		this.runner = runner;
		this.request = request;
	}
		
	@Override
	public ActiveMessage call() {
		ActiveMessage response = null;
		try {
			response = new ActiveMessage(request.getId(), 
					runner.runCode(request.getGuid(), request.getField(), request.getCode(), request.getValue(), request.getTtl(), request.getId()),
					null);
		} catch (NoSuchMethodException | ScriptException e) {
			response = new ActiveMessage(request.getId(), null, e.getMessage());
			e.printStackTrace();
		}

		return response;
	}

}
