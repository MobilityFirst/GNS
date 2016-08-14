package edu.umass.cs.gnsserver.activecode.prototype;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.json.JSONException;
import org.junit.Test;

import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * @author gaozy
 *
 */
public class ActiveClientWithNamedPipeTest {

	/**
	 * @throws JSONException
	 * @throws ActiveException
	 */
	@Test
	public void test01_sequentialRequestThroughput() throws JSONException, ActiveException {
		
		String suffix = "";

		int numThread = 1; //Integer.parseInt(args[0]);
				
		String cfile = "/tmp/client"+suffix;
		String sfile = "/tmp/server"+suffix;		
		/**
		 * Test client performance with named pipe channel
		 */
		ActiveClientWithNamedPipe client = new ActiveClientWithNamedPipe(null, cfile, sfile, 0, numThread);
		
		String guid = "guid";
		String field = "name";
		String noop_code = "";
		try {
			noop_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/noop.js")));
		} catch (IOException e) {
			e.printStackTrace();
		} 
		ValuesMap value = new ValuesMap();
		value.put("string", "hello world!");	
		ValuesMap result = client.runCode(guid, field, noop_code, value, 0);
		assertEquals(result.toString(), value.toString());
		
		int n = 1000000;
		
		long t1 = System.currentTimeMillis();
		
		for (int i=0; i<n; i++){
			client.runCode(guid, field, noop_code, value, 0);
		}
		
		long elapsed = System.currentTimeMillis() - t1;
		System.out.println("It takes "+elapsed+"ms, and the average latency for each operation is "+(elapsed*1000.0/n)+"us");
		System.out.println("The average throughput is "+(n*1000.0/elapsed)*numThread);
		client.shutdown();
	}

}
