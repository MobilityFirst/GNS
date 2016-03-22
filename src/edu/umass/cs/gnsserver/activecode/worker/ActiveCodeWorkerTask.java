package edu.umass.cs.gnsserver.activecode.worker;

import javax.script.ScriptException;

import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeMessage;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeParams;

/**
 * @author gaozy
 *
 */
public class ActiveCodeWorkerTask implements Runnable{
	private ActiveCodeParams params;
	private ActiveCodeRunner runner;
	private ActiveCodeGuidQuerier querier;
	
	
	protected ActiveCodeWorkerTask(ActiveCodeParams params, ActiveCodeRunner runner, ActiveCodeGuidQuerier querier){
		this.params = params;
		this.runner = runner;
		this.querier = querier;
	}
	
	public void run() {
		JSONObject result = null;
		Throwable thr = null;
		try {
			result = runner.submitRequest(params, querier);
		} catch (NoSuchMethodException | ParseException | ScriptException e) {
			e.printStackTrace();
			thr = e;
		} finally{
			// Send the response back no matter what
			ActiveCodeMessage acmResp = null;
			if(thr == null){			  
				acmResp = new ActiveCodeMessage();
				acmResp.setFinished(true);
				acmResp.setCrashed(querier.getError());
				acmResp.setValuesMapString(result == null ? null : result.toString());	
				runner.sendResponse(acmResp);
			} else{
				// No need to send back a response, as client already 
				thr.printStackTrace();
			}
			
		}
	}
	
	private ActiveCodeMessage crashedMessage(String errMsg){
		ActiveCodeMessage acmResp = new ActiveCodeMessage();
		acmResp.setFinished(true);
	    acmResp.setCrashed(errMsg);
	    return acmResp;
	}
}
