package edu.umass.cs.gnsserver.activecode.scratch;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 *
 */
public class TestNashorn {
	
	 /**
	 * @param args
	 * @throws IOException
	 * @throws ScriptException
	 * @throws NoSuchMethodException
	 */
	public static void main(String[] args) throws IOException, ScriptException, NoSuchMethodException {
		if(args.length < 1){
			System.out.println("Usage: java -cp dist/jars/GNS.jar true/false");
			System.exit(0);
		}
		
		boolean toEval = Boolean.parseBoolean(args[0]);
		
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName("nashorn");
        Invocable invocable = (Invocable) engine;
        String value = "hello world";
        String field = null;
        Object querier = null;
        
        String code = new String(Files.readAllBytes(Paths.get("scripts/activeCode/noop.js")));
        engine.eval(code);
        invocable.invokeFunction("run", value, field, querier);
        
        int n = 1000000;
        long t = System.currentTimeMillis();
        for(int i=0; i<n; i++) {
        	if(toEval)
        		engine.eval(code);
        	invocable.invokeFunction("run", value, field, querier);
    	}
        long elapsed = System.currentTimeMillis() - t;
		System.out.println((toEval?"With":"Without")+String.format(" code eval, it takes %d ms, avg_latency = %.2f us", elapsed, elapsed*1000.0/n));
	 }
}

