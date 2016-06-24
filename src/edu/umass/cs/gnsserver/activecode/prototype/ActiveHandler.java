package edu.umass.cs.gnsserver.activecode.prototype;

import java.io.File;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.json.simple.JSONObject;

/**
 * @author gaozy
 *
 */
public class ActiveHandler {
	
	private ActiveChannel channel;
	private String cfile;
	private String sfile;
	byte[] buffer = new byte[ActiveWorker.bufferSize];
	
	private Process workerProc;
	private ActiveSerializer serializer;
	
	final static int total = 1000;
	protected static int received = 0;
	synchronized int incr(){
		return ++received;
	}
	
	
	protected ActiveHandler(String cfile, String sfile){
		this.cfile = cfile;
		this.sfile = sfile;
		
		channel = new ActivePipe(cfile, sfile); //new ActiveMappedBus(cfile, sfile);
		try {
			workerProc = startWorker(cfile, sfile);
		} catch (IOException e) {
			e.printStackTrace();
		}
		serializer = new ActiveSerializer();
		
	}
	
	
	protected void shutdown(){
		
		if(workerProc != null){
			workerProc.destroy();
		}
		
		(new File(cfile)).delete();
		(new File(sfile)).delete();
	}
	
	private static Process startWorker(String cfile, String sfile) throws IOException{
		List<String> command = new ArrayList<String>();
		String classpath = System.getProperty("java.class.path");
	    command.add("java");
	    command.add("-Xms64m");
	    command.add("-Xmx64m");
	    command.add("-cp");
	    command.add(classpath);
	    command.add("edu.umass.cs.zhaoyu.prototype.ActiveWorker");
	    command.add(cfile);
	    command.add(sfile);
		
	    ProcessBuilder builder = new ProcessBuilder(command);
		builder.directory(new File(System.getProperty("user.dir")));
		
		//builder.redirectError(Redirect.INHERIT);
		builder.redirectOutput(Redirect.INHERIT);
		//builder.redirectInput(Redirect.INHERIT);
		
		Process process = builder.start();		
		
		return process;
	}
	
	protected ActiveMessage read(){
		return null;
	}
	
	protected boolean write(){
		boolean wSuccess = false;
		return wSuccess;
	}
	
	/**
	 * @param args
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws InterruptedException{
		String cfile = "/tmp/client";
		String sfile = "/tmp/server";
				
		String guid = "guid";
		String field = "name";
		String noop_code = "";
		try {
			noop_code = new String(Files.readAllBytes(Paths.get("./scripts/noop.js")));
		} catch (IOException e) {
			e.printStackTrace();
		} 
		JSONObject value = new JSONObject();
		
		ActiveHandler handler = new ActiveHandler(cfile, sfile);

		
		int n = total;
		long t1 = System.currentTimeMillis();
		for (int i=0; i<n; i++){
			//handler.sendRequest( new ActiveMessage(guid+i, field+i, noop_code, 0, value) );
		}
		
		long elapsed = System.currentTimeMillis() - t1;
		System.out.println("It takes "+elapsed+"ms, and the average latency for each operation is "+(elapsed*1000.0/n)+"us");
		
		handler.shutdown();
	}

	
}
