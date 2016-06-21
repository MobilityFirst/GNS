package edu.umass.cs.gnsserver.active.worker;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;

import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;

import org.json.JSONException;

import edu.umass.cs.gnsserver.active.ActiveProperties;
import edu.umass.cs.gnsserver.active.protocol.channel.ActiveChannel;
import edu.umass.cs.gnsserver.active.protocol.channel.ActivePipe;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.DelayProfiler;

/**
 * @author gaozy
 *
 */
public class ActiveWorker {
	
	ActiveChannel channel = null;
	byte[] buffer;
	private static ScriptEngine engine;
	private static Invocable invocable;
	
	private static HashMap<String, ScriptContext> contexts;
	private static HashMap<String, Integer> codeHashes;
	
	/**
	 * @param pfile
	 */
	public ActiveWorker(String pfile){
		channel = new ActivePipe(pfile);
		buffer = new byte[ActiveProperties.bufferSize];
		
		engine = new ScriptEngineManager().getEngineByName("nashorn");
		invocable = (Invocable) engine;
		
		contexts = new HashMap<String, ScriptContext>();
		codeHashes = new HashMap<String, Integer>();
	}
	
	protected ActiveChannel getChannel(){
		return channel;
	}
	
	protected static void updateCache(String codeId, String code) throws ScriptException {
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
	
	/**
	 * @param guid
	 * @param field
	 * @param code
	 * @param valuesMap 
	 * @return
	 * @throws ScriptException
	 * @throws NoSuchMethodException
	 */
	public static ValuesMap runCode(String guid, String field, String code, ValuesMap valuesMap) throws ScriptException, NoSuchMethodException {
		long t = System.nanoTime();
		updateCache(guid, code);
		DelayProfiler.updateDelayNano("activeUpdateCache", t);
		
		engine.setContext(contexts.get(guid));
		DelayProfiler.updateDelayNano("activeEngineSetContext", t);
		
		ValuesMap value = (ValuesMap) invocable.invokeFunction("run", valuesMap, field, null);
		DelayProfiler.updateDelayNano("activeEngineInvoke", t);
		return value;
	}
	
	
	
	/**
	 * @param args
	 * @throws JSONException
	 * @throws IOException
	 * @throws ClassNotFoundException 
	 * @throws ScriptException 
	 * @throws NoSuchMethodException 
	 */
	public static void main(String[] args) throws JSONException, IOException, ClassNotFoundException, NoSuchMethodException, ScriptException{
		final String pfile = "tmp";
		ActiveWorker worker = new ActiveWorker(pfile);
		ActivePipe pipe = new ActivePipe(pfile);
		byte[] buf1 = new byte[1024];
		byte[] buf2 = new byte[1024];
		
		String noop_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/noop.js")));
		
		ValuesMap value = new ValuesMap();
		value.put("hello", "world");
		
		String guid = "1";
		String field = "hello";

		int n = 1000000;
		
		/**
		 * test latency of runCode method
		 */
		long t = System.currentTimeMillis();
		for(int i=0;i<n; i++){
			try {
				worker.runCode(guid, field, noop_code, value);
			} catch (NoSuchMethodException | ScriptException e) {
				e.printStackTrace();
			}
		}
		long elapsed = System.currentTimeMillis() - t;
		
		System.out.println("Calling "+n+" runCode() methods takes "+elapsed+"ms, and the average latency for each operation is "+(elapsed*1000.0/n)+"us");
		
		
		/** 
		 * Test The whole process by integrating communication and script engine
		 */
		/*
		ActiveMessage obj2 = null;
		ActiveMessage obj3 = null;
		// warm up for one round
		pipe.write(new ActiveMessage(guid, field, noop_code, 0, value));		
		obj2 = (ActiveMessage) worker.getChannel().read(buf1);
		worker.getChannel().write(obj2);
		obj3 = (ActiveMessage) pipe.read(buf2);
		
		t = System.currentTimeMillis();
		for (int i=0; i<n; i++){
			pipe.write(new ActiveMessage(guid, field, noop_code, 0, value));
			obj2 = (ActiveMessage) worker.getChannel().read(buf1);
			worker.runCode(obj2.getGuid(), obj2.getField(), obj2.getCode(), obj2.getJson());
			worker.getChannel().write(obj2);
			obj3 = (ActiveMessage) pipe.read(buf2);
		}
		elapsed = System.currentTimeMillis() - t;
		System.out.println("Communication with active code execution takes "+elapsed+"ms, and the average latency for each operation is "+(elapsed*1000.0/n)+"us");		
		*/
		
		File f = new File(pfile);
		f.delete();
	}
	
}
