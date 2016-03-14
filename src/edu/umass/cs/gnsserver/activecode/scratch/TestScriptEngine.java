package edu.umass.cs.gnsserver.activecode.scratch;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;

public class TestScriptEngine {

    public static void main(String[] args) throws IOException, ScriptException, NoSuchMethodException {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName("nashorn");
        Invocable invocable = (Invocable) engine;
        
        ScriptContext sc = new SimpleScriptContext();
        
        String code1 = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/notActiveCode1.js")));
        String code2 = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/notActiveCode2.js")));
        
        long t = System.currentTimeMillis();
        engine.eval(code1);
        System.out.println("It takes "+(System.currentTimeMillis()-t)+"ms to eval this code");
        t = System.currentTimeMillis();
        Double result = (Double) invocable.invokeFunction("run", 1);
        System.out.println("It takes "+(System.currentTimeMillis()-t)+"ms to run this code");
        System.out.println("The second code returns "+result);
        
        
        t = System.currentTimeMillis();
        engine.eval(code2);
        System.out.println("It takes "+(System.currentTimeMillis()-t)+"ms to eval this code");
        t = System.currentTimeMillis();
        result = (Double) invocable.invokeFunction("run", 1);
        System.out.println("It takes "+(System.currentTimeMillis()-t)+"ms to run this code");
        System.out.println("The second code returns "+result);
        
        int n = 1000;
        // evaluate JavaScript code
        try {
        	// exclude first run coz it takes longer
        	long t1 = System.currentTimeMillis();
        	engine.eval("s = 'Hello, World' + '23'");
        	long eclapsed = System.currentTimeMillis() - t1;
        	System.out.println("The first one takes "+eclapsed+"ms");
        	
        	t = System.nanoTime();
        	for(int i=0; i<n; i++) {
				engine.eval("s = 'Hello, World' + '23'");
        	}
			System.out.println("avg_latency = " + (System.nanoTime() - t)/n/1000 + "us");
		} catch (ScriptException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

}
