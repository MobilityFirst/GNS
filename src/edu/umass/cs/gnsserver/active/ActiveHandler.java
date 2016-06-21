package edu.umass.cs.gnsserver.active;

import javax.script.ScriptException;

import org.json.simple.JSONObject;

import edu.umass.cs.gnsserver.active.worker.ActiveWorker;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.DelayProfiler;

public class ActiveHandler {
	
	private static ActiveWorker worker;

	
	/**
	 * @param pfile
	 */
	public ActiveHandler(String pfile){		
		worker = new ActiveWorker(pfile);		
	}
	
	
	public static ValuesMap runCode(String code, String guid, String field, String action, ValuesMap valuesMap, int ttl){
		long t = System.nanoTime();
		try {
			worker.runCode(guid, field, code, valuesMap );
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (ScriptException e) {
			e.printStackTrace();
		}
		
		DelayProfiler.updateDelayNano("", t);
		return valuesMap;
	}
	
}
