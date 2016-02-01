package edu.umass.cs.gnsserver.activecode.worker;

import java.util.concurrent.Callable;

import javax.script.Invocable;
import javax.script.ScriptException;

import org.json.simple.JSONObject;

public class ActiveCodeWorkerTask implements Callable<JSONObject>{
	private Invocable inv;
	private JSONObject value;
	private String field;
	private ActiveCodeGuidQuerier querier;
	
	public ActiveCodeWorkerTask(Invocable inv, JSONObject value, String field, ActiveCodeGuidQuerier querier){
		this.inv = inv;
		this.value = value;
		this.field = field;
		this.querier = querier;
	}
	
	public JSONObject call(){
		JSONObject obj = null;
		try{
			obj = (JSONObject) inv.invokeFunction("run", value, field, querier);
		}catch(ScriptException e){
			e.printStackTrace();
		}catch(NoSuchMethodException e){
			e.printStackTrace();
		}
		return obj;
	}
}
