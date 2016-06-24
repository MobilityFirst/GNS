package edu.umass.cs.gnsserver.activecode.prototype;

import java.util.Arrays;
import java.util.HashMap;

import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;

import org.json.simple.JSONObject;

import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * @author gaozy
 *
 */
public class ActiveWorker {
		
	private ScriptEngine engine;
	private Invocable invocable;
	
	private final HashMap<String, ScriptContext> contexts;
	private final HashMap<String, Integer> codeHashes;
	
	private final ActiveChannel channel;
	private final ActiveSerializer serializer;
	private final String cfile;
	private final String sfile;
	
	protected final static int bufferSize = 1024;
	
	public ActiveWorker(String cfile, String sfile) {		
		this.cfile = cfile;
		this.sfile = sfile;
		
		channel = new ActiveMappedBus(sfile, cfile);
		serializer = new ActiveSerializer();
		
		engine = new ScriptEngineManager().getEngineByName("nashorn");
		invocable = (Invocable) engine;
		
		contexts = new HashMap<String, ScriptContext>();
		codeHashes = new HashMap<String, Integer>();
		
		try {
			runWorker();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (ScriptException e) {
			e.printStackTrace();
		} catch (Exception e){
			e.printStackTrace();
		} finally {
			channel.shutdown();
		}
	}
	
	protected void updateCache(String codeId, String code) throws ScriptException {
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
	 * @param value
	 * @return ValuesMap result 
	 * @throws ScriptException
	 * @throws NoSuchMethodException
	 */
	public ValuesMap runCode(String guid, String field, String code, ValuesMap value) throws ScriptException, NoSuchMethodException {
		
		updateCache(guid, code);
		
		engine.setContext(contexts.get(guid));

		return (ValuesMap) invocable.invokeFunction("run", value, field, null);
	}

	
	private void runWorker() throws NoSuchMethodException, ScriptException{
		
		byte[] buffer = new byte[bufferSize];
		System.out.println("Start running worker by listening on "+cfile+", and write to "+sfile);
		
		while( !Thread.currentThread().isInterrupted()){
			if(channel.read(buffer)!= -1){
				ActiveMessage msg = serializer.deserialize(buffer);
				//System.out.println("Worker received:"+msg.getGuid()+" "+msg.getField() );
				//msg.setJson((JSONObject) runCode(msg.getGuid(), msg.getField(), msg.getCode(), msg.getValue()));
				Arrays.fill(buffer, (byte) 0);
				
				// echo the message 
				byte[] buf = serializer.serialize(msg);
				channel.write(buf, 0, buf.length);
				//System.out.println("Worker echo:"+msg.getGuid()+" "+msg.getField() );
			}
		}
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args){
		String cfile = args[0];
		String sfile = args[1];
		new ActiveWorker(cfile, sfile);
	}
}
