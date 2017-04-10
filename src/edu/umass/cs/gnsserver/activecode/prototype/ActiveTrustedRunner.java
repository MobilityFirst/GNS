package edu.umass.cs.gnsserver.activecode.prototype;

import java.util.HashMap;

import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;

import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Querier;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Runner;
import jdk.nashorn.api.scripting.NashornScriptEngineFactory;
import jdk.nashorn.api.scripting.ScriptObjectMirror;

/**
 * This runner is used to run trusted code inside GNS process.
 * 
 * @author gaozy
 *
 */
public class ActiveTrustedRunner implements Runner {
	
	final private ScriptEngine engine;
	final private Invocable invocable;
	
	private final ScriptObjectMirror JSON;
	
	final private Querier querier;
	
	private final HashMap<String, ScriptContext> contexts = new HashMap<String, ScriptContext>();
	private final HashMap<String, Integer> codeHashes = new HashMap<String, Integer>();
	
	protected ActiveTrustedRunner(Querier querier){
		this.querier = querier;
		
		// Initialize an script engine without extensions and java
		NashornScriptEngineFactory factory = new NashornScriptEngineFactory();
		engine = factory.getScriptEngine("-strict", "--no-java", "--no-syntax-extensions");
		
		invocable = (Invocable) engine;
		
		try {
			JSON = (ScriptObjectMirror) engine.eval("JSON");
		} catch (ScriptException e) {
			e.printStackTrace();
			throw new RuntimeException("Can not eval JSON");
		}
	}
	
	private ScriptObjectMirror string2JS(String str){
		return (ScriptObjectMirror) JSON.callMember("parse", str);
	}
	
	private String js2String(ScriptObjectMirror obj){
		return (String) JSON.callMember("stringify", obj);
	}
	
	private synchronized void updateCache(String codeId, String code) throws ScriptException {
	    if (!contexts.containsKey(codeId)) {
	      // Create a context if one does not yet exist and eval the code
	      ScriptContext sc = new SimpleScriptContext();
	      contexts.put(codeId, sc);
	      codeHashes.put(codeId, code.hashCode());
	      engine.eval(code, sc);
	    } else if ( codeHashes.get(codeId) != code.hashCode()) {
	      // The context exists, but we need to eval the new code
	      ScriptContext sc = contexts.get(codeId);
	      codeHashes.put(codeId, code.hashCode());
	      engine.eval(code, sc);
	    }
	}
	
	@Override
	public String runCode(String guid, String accessor, String code, String value, int ttl, long id)
			throws ScriptException, NoSuchMethodException {
		updateCache(guid, code);
		engine.setContext(contexts.get(guid));
		
		String valuesMap = null;
		
		/**
		 * This cast is for active code usability:
		 * cast String to JS JSON, then cast JS JSON back to String,
		 * no need for customer to cast the value by himself.
		 */
		valuesMap = js2String( (ScriptObjectMirror) invocable.invokeFunction("run", string2JS(value), accessor, querier));
		
		return valuesMap;
	}

}
