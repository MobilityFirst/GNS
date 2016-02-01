package edu.umass.cs.gnsserver.activecode.worker;

import java.util.concurrent.Callable;

import javax.script.Invocable;
import javax.script.ScriptException;

import org.json.JSONObject;

public class ActiveCodeWorkerTask implements Callable<JSONObject>{
	private Invocable inv;
	
	public ActiveCodeWorkerTask(Invocable inv, JSONObject value, String field, ActiveCodeGuidQuerier qerier){
		this.inv = inv;
	}
	
	public JSONObject call(){
		JSONObject obj = null;
		try{
			obj = (JSONObject) inv.invokeFunction("run");
		}catch(ScriptException e){
			e.printStackTrace();
		}catch(NoSuchMethodException e){
			e.printStackTrace();
		}
		return obj;
	}
}
