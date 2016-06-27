package edu.umass.cs.gnsserver.activecode.prototype;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;

import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;

import org.json.JSONException;

import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * @author gaozy
 *
 */
public class ActiveWorker {
		
	private ScriptEngine engine;
	private Invocable invocable;
	
	private final HashMap<String, ScriptContext> contexts = new HashMap<String, ScriptContext>();
	private final HashMap<String, Integer> codeHashes = new HashMap<String, Integer>();
	
	private final ActiveChannel channel;
	private final String ifile;
	private final String ofile;
	
	protected final static int bufferSize = 1024;
	
	/**
	 * @param ifile
	 * @param ofile
	 * @param isTest
	 */
	public ActiveWorker(String ifile, String ofile, boolean isTest) {		
		this.ifile = ifile;
		this.ofile = ofile;
					
		engine = new ScriptEngineManager().getEngineByName("nashorn");
		invocable = (Invocable) engine;
		if(!isTest){
			channel = new ActivePipe(ifile, ofile);
			System.out.println("Worker's channel is ready!");
			
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
		} else {
			channel = null;
		}
	}
	
	/**
	 * @param ifile
	 * @param ofile
	 */
	public ActiveWorker(String ifile, String ofile){
		this(ifile, ofile, false);
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

	
	private void runWorker() throws NoSuchMethodException, ScriptException, UnsupportedEncodingException, JSONException{		
		byte[] buffer = new byte[bufferSize];
		System.out.println("Start running worker by listening on "+ifile+", and write to "+ofile);
		
		while((channel.read(buffer))!= -1){
			ActiveMessage msg = new ActiveMessage(buffer);
			System.out.println("Worker received:"+msg );
			Arrays.fill(buffer, (byte) 0);
			msg.setValue(runCode(msg.getGuid(), msg.getField(), msg.getCode(), msg.getValue()));
			// echo the message 
			byte[] buf = msg.toBytes();
			channel.write(buf, 0, buf.length);
			System.out.println("Worker echo:"+msg );
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
