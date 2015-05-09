package edu.umass.cs.gns.activecode.worker;

import java.util.HashMap;
import java.util.Map;

import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;

import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaFunction;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.jse.CoerceJavaToLua;
import org.luaj.vm2.lib.jse.CoerceLuaToJava;
import org.luaj.vm2.lib.jse.JsePlatform;
import org.luaj.vm2.luajc.LuaJC;
import org.luaj.vm2.script.LuajContext;

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
		// engine = engineManager.getEngineByName("luaj");
		invocable = (Invocable) engine;
		contexts = new HashMap<String, ScriptContext>();
		codeHashes = new HashMap<String, Integer>();
		
		// uncomment to enable the lua-to-java bytecode compiler 
        // (require bcel library in class path)
		// Globals globals = JsePlatform.standardGlobals();
		// LuaJC.install(globals);
	}
	
	/**
	 * Initializes the script context and re-evals the code when a change is detected
	 * @param guid the guid
	 * @param code the code
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
	
	/*
	private void updateCacheLua(String codeId, String code) throws ScriptException {
		if(!contexts.containsKey(codeId)) {
			// Create a context if one does not yet exist and eval the code
			ScriptContext sc = new LuajContext();
			CompiledScript script = ((Compilable) engine).compile(code);
			contexts.put(codeId, sc);
			codeHashes.put(codeId, code.hashCode());
			script.eval(sc);
		} else if (codeHashes.get(codeId) != code.hashCode()) {
			// The context exists, but we need to eval the new code
			ScriptContext sc = contexts.get(codeId);
			CompiledScript script = ((Compilable) engine).compile(code);
			codeHashes.put(codeId, code.hashCode());
			script.eval(sc);
		}
	}
	*/
	
	/*
	private ValuesMap runLuaCode(String guid, String action, String field, String code, ValuesMap value, ActiveCodeGuidQuerier querier) {
		String codeId = guid + "_" + action;
		
		try {
			updateCacheLua(codeId, code);
		} catch (ScriptException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		ScriptContext sc = contexts.get(codeId);
		engine.setContext(sc);
				
		LuaValue[] params = {
				CoerceJavaToLua.coerce(value),
				LuaValue.valueOf(field),
				CoerceJavaToLua.coerce(querier)
		};
		
		LuaFunction luaFunc = (LuaFunction) sc.getAttribute("run");
		Varargs result = luaFunc.invoke(params);
		LuaValue resultValue = result.arg1();
		return (ValuesMap)CoerceLuaToJava.coerce(resultValue, ValuesMap.class);
	}
	*/
	
	/**
	 * Runs the specified active code
	 * @param guid the guid
	 * @param field the field
	 * @param code the code
	 * @param value the original value read or written
	 * @param querier the querier object used for active code reads/writes
	 * @return the output of the code
	 */
	public ValuesMap runCode(String guid, String action, String field, String code, ValuesMap value, ActiveCodeGuidQuerier querier) {
		// if(true)
		//	return runLuaCode(guid, action, field, code, value, querier);
		
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
