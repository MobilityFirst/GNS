package edu.umass.cs.gnsserver.activecode.prototype.multithreading;

import java.io.File;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage.Type;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveNamedPipe;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Channel;
import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * @author gaozy
 *
 */
public class MultiThreadActiveClient implements Runnable{
	
	private Channel channel;
	private String ifile;
	private String ofile;
		
	private Process workerProc;
	private int numThread;
	final private int id;
	
	private static int heapSize = 64;
	
	/*****************Test*******************/
	private static final int total = 1000000;
	private static final AtomicInteger counter = new AtomicInteger();
	
	protected MultiThreadActiveClient(int numThread, String ifile, String ofile, int id){
		this.numThread = numThread;
		this.id = id;
		this.ifile = ifile;
		this.ofile = ofile;
		
		Runtime runtime = Runtime.getRuntime();
		try {
			runtime.exec("mkfifo "+ifile);
			runtime.exec("mkfifo "+ofile);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		try {
			workerProc = startWorker(ofile, ifile, id);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		channel = new ActiveNamedPipe(ifile, ofile);
		
		System.out.println("Start "+this+" by listening on "+ifile+", and write to "+ofile);
	}
	
	private Process startWorker(String ifile, String ofile, int id) throws IOException{
		List<String> command = new ArrayList<String>();
		String classpath = System.getProperty("java.class.path");
	    command.add("java");
	    command.add("-Xms"+heapSize+"m");
	    command.add("-Xmx"+heapSize+"m");
	    command.add("-cp");
	    command.add(classpath);
	    command.add("edu.umass.cs.gnsserver.activecode.prototype.multithreading.MultiThreadActiveWorker");
	    command.add(""+numThread);
	    command.add(ifile);
	    command.add(ofile);
	    command.add(""+id);
		
	    ProcessBuilder builder = new ProcessBuilder(command);
		builder.directory(new File(System.getProperty("user.dir")));
		
		builder.redirectError(Redirect.INHERIT);
		builder.redirectOutput(Redirect.INHERIT);
		//builder.redirectInput(Redirect.INHERIT);
		
		Process process = builder.start();		

		return process;
	}
	
	protected void shutdown(){
		if(workerProc != null){
			workerProc.destroy();
		}
		
		(new File(ifile)).delete();
		(new File(ofile)).delete();
	}
	
	@Override
	public String toString(){
		return this.getClass().getSimpleName()+id;
	}
	
	
	protected ActiveMessage receiveMessage() {		
		ActiveMessage am = null;
		try {
			am = (ActiveMessage) channel.receiveMessage();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return am;
	}
	
	protected void sendMessage(ActiveMessage am){
		try {
			channel.sendMessage(am);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * @param guid
	 * @param field
	 * @param code
	 * @param valuesMap
	 * @param ttl
	 */
	public synchronized void sendRequest( String guid, String field, String code, ValuesMap valuesMap, int ttl){
		sendMessage(new ActiveMessage(guid, field, code, valuesMap, ttl));
	}

	@Override
	public void run() {
		ActiveMessage response;
		while( counter.get() < total ){
			response = receiveMessage();
			if(response != null && response.type == Type.RESPONSE){
				counter.incrementAndGet();
			}
		}
		System.out.println("received all messages!");
		
	}
	
	/**
	 * @param args
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws InterruptedException {
		
		int numThread = 1; //Integer.parseInt(args[0]);
		String cfile = "/tmp/client";
		String sfile = "/tmp/server";
		int id = 0;
		
		MultiThreadActiveClient client = new MultiThreadActiveClient(numThread, cfile, sfile, id);
		
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
				client.sendRequest(guid, field, noop_code, value, 0);
			}
			
			while(counter.get() < total)
				;
			long elapsed = System.currentTimeMillis() - t;
			System.out.println("It takes "+elapsed+"ms, and the average latency for each operation is "+(elapsed*1000.0/total)+"us");
			System.out.println("The average throughput is "+(total*1000.0/elapsed));
			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
		
			client.shutdown();
		}
		
		System.exit(0);
	}
}
