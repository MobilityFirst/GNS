package edu.umass.cs.gnsserver.activecode.worker;

import java.util.concurrent.Callable;

import javax.script.Invocable;
import javax.script.ScriptException;

import org.json.simple.JSONObject;

/**
 * @author gaozy
 *
 */
public class ActiveCodeWorkerTask implements Callable<JSONObject>{
	private Invocable inv;
	private JSONObject value;
	private String field;
	private ActiveCodeGuidQuerier querier;
	
	protected ActiveCodeWorkerTask(Invocable inv, JSONObject value, String field, ActiveCodeGuidQuerier querier){
		this.inv = inv;
		this.value = value;
		this.field = field;
		this.querier = querier;
	}
	
	public JSONObject call() throws NoSuchMethodException, ScriptException{
		JSONObject obj = null;
		obj = (JSONObject) inv.invokeFunction("run", value, field, querier);		
		return obj;
	}
}
