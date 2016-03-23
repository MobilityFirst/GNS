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
			//System.out.println("The executed result is "+result+" with param "+params);
			
			if(result != null){
				ActiveCodeMessage acmResp = new ActiveCodeMessage();
				acmResp.setFinished(true);
				acmResp.setValuesMapString(result == null ? null : result.toString());	
				runner.sendResponse(acmResp);
			}
			
			
		} catch ( NoSuchMethodException | ParseException | ScriptException e) {
			e.printStackTrace();
			thr = e;
		} finally{			
			if(thr != null){
				// No need to send back a response, as client already 
				thr.printStackTrace();
				ActiveCodeMessage acmResp = crashedMessage(thr.getMessage());
				runner.sendResponse(acmResp);
			}			
		}
	}
	
	private ActiveCodeMessage crashedMessage(String err){
		ActiveCodeMessage acm = new ActiveCodeMessage();
		acm.setCrashed(err);
		acm.setFinished(true);
		return acm;
	}
}
