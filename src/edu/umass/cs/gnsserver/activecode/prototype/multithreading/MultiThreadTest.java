package edu.umass.cs.gnsserver.activecode.prototype.multithreading;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * @author gaozy
 *
 */
public class MultiThreadTest {
	
	private static final int total = 10;	
	
	/**
	 * @param args
	 */
	public static void main(String[] args){
		
		int numThread = 1; //Integer.parseInt(args[0]);
		String cfile = "/tmp/client";
		String sfile = "/tmp/server";
		int id = 0;
		
		MultiThreadActiveClient client = new MultiThreadActiveClient(null, numThread, cfile, sfile, id);
		
		new Thread(client).start();
		
		
		try{
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
			
			long t = System.currentTimeMillis();
			for(int i=0; i<total; i++){
				//client.sendRequest(guid, field, noop_code, value, 0);
				client.runCode(guid, field, noop_code, value, 0);
			}
			
			while(client.getReceived() < total)
				;
			long elapsed = System.currentTimeMillis() - t;
			System.out.println("It takes "+elapsed+"ms, and the average latency for each operation is "+(elapsed*1000.0/total)+"us");
			System.out.println("The average throughput is "+(total*1000.0/elapsed));
			
		}catch(Exception e){
			e.printStackTrace();
		} finally {		
			client.shutdown();
		}
		
		System.exit(0);
	}
}
