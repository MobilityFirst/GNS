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
package edu.umass.cs.gnsserver.activecode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveHandler;
import edu.umass.cs.gnsserver.gnsapp.GNSApp;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.ActiveCode;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * This class is the entry of activecode, it provides
 * the interface for GNS to run active code. It's creates
 * a threadpool to connect the real isolated active worker
 * to run active code. It also handles the misbehaviours.
 *
 * @author Zhaoyu Gao, Westy
 */

public class ActiveCodeHandler {
	
	private static final Logger logger = Logger.getLogger("ActiveGNS");
	
	private static ActiveHandler handler;
	
	/**
	 * enable debug output
	 */
	public static final boolean enableDebugging = false; 
	
	/**
	 * Initializes an ActiveCodeHandler
	 */
	public ActiveCodeHandler() {
		String configFile = System.getProperty("activeFile");
		if(configFile != null){
			try {
				new ActiveCodeConfig(configFile);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
		
		handler = new ActiveHandler(new ActiveCodeDB(), ActiveCodeConfig.activeCodeWorkerCount, ActiveCodeConfig.activeWorkerThreads);
	}
	
	
	/**
	 * Checks to see if this guid has active code for the specified action.
	 * @param valuesMap 
	 * @param action can be 'read' or 'write'
	 * @return whether or not there is active code
	 */
	public static boolean hasCode(ValuesMap valuesMap, String action) {
		try {
            return valuesMap.has(ActiveCode.getCodeField(action));
		} catch (Exception e) {
			return false;
		}
	}
	
	
	/**
	 * @param header 
	 * @param code
	 * @param guid
	 * @param field
	 * @param action
	 * @param valuesMap
	 * @param activeCodeTTL current default is 10
	 * @return executed result
	 */
	public static ValuesMap runCode(InternalRequestHeader header, String code, String guid, String field, String action, ValuesMap valuesMap, int activeCodeTTL) {
		try {
			return handler.runCode(header, guid, field, code, valuesMap, activeCodeTTL);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return valuesMap;
	}
	
	/**
	 * @return logger
	 */
	public static Logger getLogger(){
		return logger;
	}
	
	/***************************** TEST CODE *********************/
	/**
	 * @param args 
	 * @throws InterruptedException 
	 * @throws ExecutionException 
	 * @throws IOException 
	 * @throws JSONException 
	 */
	public static void main(String[] args) throws InterruptedException, ExecutionException, IOException, JSONException {
		new ActiveCodeHandler();
		
		// initialize the parameters used in the test 
		JSONObject obj = new JSONObject();
		obj.put("testGuid", "success");
		ValuesMap valuesMap = new ValuesMap(obj);
		final String guid1 = "guid";
		final String field1 = "testGuid";
		final String read_action = "read";
		
		String noop_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/noop.js"))); 
		String noop_code64 = Base64.encodeToString(noop_code.getBytes("utf-8"), true);
		ActiveCodeHandler.runCode(null, noop_code64, guid1, field1, read_action, valuesMap, 100);
		
		int n = 1000000;
		long t = System.currentTimeMillis();
		for(int i=0; i<n; i++){
			ActiveCodeHandler.runCode(null, noop_code64, guid1, field1, read_action, valuesMap, 100);
		}
		long elapsed = System.currentTimeMillis() - t;
		System.out.println(String.format("it takes %d ms, avg_latency = %f us", elapsed, elapsed*1000.0/n));
		
	}


	/**
	 * @param header
	 * @param fields
	 * @param guid
	 * @param valuesMap
	 * @param app
	 * @return a values map
	 */
	public ValuesMap handleActiveCode(InternalRequestHeader header,
			List<String> fields, String guid, ValuesMap valuesMap, GNSApp app) {
		throw new RuntimeException("Unimplemented");
	}
}
