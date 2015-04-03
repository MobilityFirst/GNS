package edu.umass.cs.gns.activecode.worker;

import java.util.HashMap;
import java.util.Map;

import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;

import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.nsdesign.recordmap.BasicRecordMap;
import edu.umass.cs.gns.nsdesign.recordmap.MongoRecordMap;
import edu.umass.cs.gns.nsdesign.recordmap.NameRecord;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.ValuesMap;

public class ActiveCodeRunner {
	private ScriptEngine engine;
	private Invocable invocable;
	private HashMap<String, ScriptContext> contexts;
	private HashMap<String, Integer> codeHashes;

	public ActiveCodeRunner() {
		ScriptEngineManager engineManager = new ScriptEngineManager();
		engine = engineManager.getEngineByName("nashorn");
		invocable = (Invocable) engine;
		contexts = new HashMap<String, ScriptContext>();
		codeHashes = new HashMap<String, Integer>();
	}
	
	/**
	 * Initializes the script context and re-evals the code when a chance is detected
	 * @param guid
	 * @param code
	 * @throws ScriptException
	 */
	private void updateCache(String codeId, String code) throws ScriptException {
		if(!contexts.containsKey(codeId)) {
			// Create a context if one does not yet exist and eval the code
			ScriptContext sc = new SimpleScriptContext();
			contexts.put(codeId, sc);
			codeHashes.put(codeId, code.hashCode());
			engine.eval(code, sc);
		} else if (codeHashes.get(codeId) != code.hashCode()) {
			// The context exists, but we need to eval the new code
			ScriptContext sc = contexts.get(codeId);
			codeHashes.put(codeId, code.hashCode());
			engine.eval(code, sc);
		}
	}
	
	/**
	 * Runs the specified active code
	 * @param guid
	 * @param field
	 * @param code
	 * @param value
	 * @param querier
	 * @return the output of the code
	 */
	public ValuesMap runCode(String guid, String action, String field, String code, ValuesMap value, ActiveCodeGuidQuerier querier) {
		try {
			// Create a guid + action pair
			String codeId = guid + "_" + action;
			// Update the script context if needed
			updateCache(codeId, code);
			// Set the context
			ScriptContext sc = contexts.get(codeId);
			engine.setContext(sc);
			
			// Run the code
			return (ValuesMap)invocable.invokeFunction("run", value, field, querier);
			
		} catch (NoSuchMethodException | ScriptException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}
}
