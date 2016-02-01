/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Misha Badov, Westy
 *
 */
package edu.umass.cs.gnsserver.activecode.worker;

import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;

import org.json.simple.JSONObject;

import edu.umass.cs.gnsserver.activecode.ActiveCodeException;
import edu.umass.cs.utils.DelayProfiler;

/**
 * This class is used to run active code
 * 
 * @author Zhaoyu Gao
 */
public class ActiveCodeRunner {

  private final ScriptEngine engine;
  private final Invocable invocable;
  private final HashMap<String, ScriptContext> contexts;
  private final HashMap<String, Integer> codeHashes;
  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  /**
   * Initialize an ActiveCodeRunner with nashorn script engine
   * by default.
   * To get the script engine takes hundreds of milliseconds. 
   */
  public ActiveCodeRunner() {
	//long start = System.currentTimeMillis();
    ScriptEngineManager engineManager = new ScriptEngineManager();
    //System.out.println("It takes "+(System.currentTimeMillis()-start)+"ms to initialize the manager.");
    engine = engineManager.getEngineByName("nashorn");
    //System.out.println("It takes "+(System.currentTimeMillis()-start)+"ms to get the engine.");
    // engine = engineManager.getEngineByName("luaj");
    invocable = (Invocable) engine;
    //System.out.println("It takes "+(System.currentTimeMillis()-start)+"ms to get the invocable.");
    contexts = new HashMap<>();
    codeHashes = new HashMap<>();
    //sc = new SimpleScriptContext();

	// uncomment to enable the lua-to-java bytecode compiler 
    // (require bcel library in class path)
    // Globals globals = JsePlatform.standardGlobals();
    // LuaJC.install(globals);
  }

  /**
   * Initializes the script context and re-evals the code when a change is detected
   *
   * @param guid the guid
   * @param code the code
   * @throws ScriptException
   */
  private void updateCache(String codeId, String code) throws ScriptException {
    if (!contexts.containsKey(codeId)) {
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
   *
   * @param guid the guid
   * @param action
   * @param field the field
   * @param code the code
   * @param value the original value read or written
   * @param querier the querier object used for active code reads/writes
   * @return the output of the code
   */
  public JSONObject runCode(String guid, String action, String field, String code, JSONObject value, ActiveCodeGuidQuerier querier) {
	JSONObject ret = null;
	  //	return runLuaCode(guid, action, field, code, value, querier);
	long startTime = System.nanoTime();
    try {    	
      // Create a guid + action pair
      String codeId = guid + "_" + action;
      // Update the script context if needed
      updateCache(codeId, code);
      
      // Set the context
      ScriptContext sc = contexts.get(codeId);
      //engine.eval(code, sc);      
      engine.setContext(sc);

      // ret = (JSONObject) invocable.invokeFunction("run", value, field, querier);
      FutureTask<JSONObject> task = new FutureTask<JSONObject>(new ActiveCodeWorkerTask(invocable, value, field, querier));
      executor.execute(task);
      //ret = task.get(1000, TimeUnit.MILLISECONDS);
      ret = task.get();
      
    } catch(ScriptException e){
    	e.printStackTrace();
    } catch(InterruptedException e) {
    	e.printStackTrace();
    } catch(ExecutionException e){
    	e.printStackTrace();
    }
    
    DelayProfiler.updateDelayNano("activeWorkerEngineExecution", startTime);
    //System.out.println("It takes " + (System.nanoTime() - startTime) + " to run the active code.");
    return ret;
  }
}
