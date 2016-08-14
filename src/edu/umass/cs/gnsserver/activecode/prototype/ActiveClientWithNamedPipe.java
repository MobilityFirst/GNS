package edu.umass.cs.gnsserver.activecode.prototype;

import java.io.File;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;


import org.json.JSONException;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage.Type;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Channel;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Client;
import edu.umass.cs.gnsserver.interfaces.ActiveDBInterface;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.DelayProfiler;

/**
 * @author gaozy
 *
 */
public class ActiveClientWithNamedPipe implements Runnable,Client {
	
	private ActiveQueryHandler queryHandler;
	
	private Channel channel;
	private String ifile;
	private String ofile;
	
	
	private Process workerProc;
	final private int id;
	final private boolean pipeEnable;
	
	private static int heapSize = 64;
	
	/**
	 * @param app 
	 * @param ifile
	 * @param ofile
	 * @param id 
	 * @param workerNumThread 
	 */
	public ActiveClientWithNamedPipe(ActiveDBInterface app, String ifile, String ofile, int id, int workerNumThread){
		this.id = id;
		this.ifile = ifile;
		this.ofile = ofile;
		this.pipeEnable = true;
		
		Runtime runtime = Runtime.getRuntime();
		try {
			runtime.exec("mkfifo "+ifile);
			runtime.exec("mkfifo "+ofile);
		} catch (IOException e1) {
			e1.printStackTrace();
		}		
		try {
			workerProc = startWorker(ofile, ifile, id, workerNumThread);
		} catch (IOException e) {
			e.printStackTrace();
		}
		channel = new ActiveNamedPipe(ifile, ofile);				
		System.out.println("Start "+this+" by listening on "+ifile+", and write to "+ofile);
		
		queryHandler = new ActiveQueryHandler(app);
		
	}
	
	/**
	 * Initialize a client with a UDP channel
	 * @param app
	 * @param port
	 * @param serverPort
	 * @param id
	 * @param workerNumThread
	 */
	public ActiveClientWithNamedPipe(ActiveDBInterface app, int port, int serverPort, int id, int workerNumThread){
		this.pipeEnable = false;
		this.id = id;
		
		try {
			// reverse the order of port and serverPort, so that worker 
			workerProc = startWorker(serverPort, port, id, workerNumThread);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		channel = new ActiveDatagramChannel(port, serverPort);
		queryHandler = new ActiveQueryHandler(app);
		
	}
	
	/**
	 * @param app 
	 * @param ifile
	 * @param ofile
	 */
	public ActiveClientWithNamedPipe(ActiveDBInterface app, String ifile, String ofile){
		this(app, ifile, ofile, 0, 1);
	}
	
	@Override
	public void run() {
		/**
		 * This thread is used to poll the worker process status.
		 * If the worker is crashed, initialize a new one.
		 */
		
	}

	
	/**
	 * Destroy the worker process if it's still running,
	 * delete the 
	 */
	@Override
	public void shutdown(){
		if(workerProc != null){
			workerProc.destroy();
		}
		
		if(pipeEnable){
			(new File(ifile)).delete();
			(new File(ofile)).delete();
		}
		
		channel.shutdown();
	}
	
	/**
	 * Create a worker with named pipe
	 * @param ifile
	 * @param ofile
	 * @param id
	 * @param workerNumThread
	 * @return
	 * @throws IOException
	 */
	private Process startWorker(String ifile, String ofile, int id, int workerNumThread) throws IOException{
		List<String> command = new ArrayList<String>();
		String classpath = System.getProperty("java.class.path");
	    command.add("java");
	    command.add("-Xms"+heapSize+"m");
	    command.add("-Xmx"+heapSize+"m");
	    command.add("-cp");
	    command.add(classpath);
	    command.add("edu.umass.cs.gnsserver.activecode.prototype.ActiveWorker");
	    command.add(ifile);
	    command.add(ofile);
	    command.add(""+id);
	    command.add(""+workerNumThread);
	    command.add(Boolean.toString(pipeEnable));
	    
	    ProcessBuilder builder = new ProcessBuilder(command);
		builder.directory(new File(System.getProperty("user.dir")));
		
		builder.redirectError(Redirect.INHERIT);
		builder.redirectOutput(Redirect.INHERIT);
		//builder.redirectInput(Redirect.INHERIT);
		
		Process process = builder.start();
		return process;
	}
	
	/**
	 * Create a worker with UDP channel
	 * 
	 * @param port1
	 * @param id
	 * @param workerNumThread
	 * @return
	 * @throws IOException
	 */
	private Process startWorker(int port1, int port2, int id, int workerNumThread) throws IOException{
		List<String> command = new ArrayList<String>();
		String classpath = System.getProperty("java.class.path");
	    command.add("java");
	    command.add("-Xms"+heapSize+"m");
	    command.add("-Xmx"+heapSize+"m");
	    command.add("-cp");
	    command.add(classpath);
	    command.add("edu.umass.cs.gnsserver.activecode.prototype.ActiveWorker");
	    command.add(""+port1);
	    command.add(""+port2);
	    command.add(""+id);
	    command.add(""+workerNumThread);
		command.add(Boolean.toString(pipeEnable));
		
	    ProcessBuilder builder = new ProcessBuilder(command);
		builder.directory(new File(System.getProperty("user.dir")));
		
		builder.redirectError(Redirect.INHERIT);
		builder.redirectOutput(Redirect.INHERIT);
		//builder.redirectInput(Redirect.INHERIT);
		
		Process process = builder.start();
		return process;
	}
	
	protected ActiveMessage receiveMessage(){		
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
	
	static int numReq = 0;
	synchronized int incr(){
		return ++numReq;
	}
	
	static int received = 0;
	synchronized int getRcv(){
		return ++received;
	}
	
	/**
	 * @param guid
	 * @param field
	 * @param code
	 * @param valuesMap
	 * @param ttl
	 * @return executed result sent back from worker
	 */
	@Override
	public synchronized ValuesMap runCode( String guid, String field, String code, ValuesMap valuesMap, int ttl) throws ActiveException {
		long t1 =System.nanoTime();
		ActiveMessage msg = new ActiveMessage(guid, field, code, valuesMap, ttl);
		sendMessage(msg);
		DelayProfiler.updateDelayNano("activeSendMessage", t1);
		
		long t2 = System.nanoTime();
		ActiveMessage response;
		while((response = receiveMessage()) != null){			
			if(response.type==Type.RESPONSE){
				// this is the response
				break;
			}else{
				//System.out.println("GUID "+guid+" tries to operate on the value "+response.getValue()+" of field "+response.getField()+" from "+response.getTargetGuid());
				ActiveMessage am = queryHandler.handleQuery(response);
				sendMessage(am);
			}
		}
				
		if(response.getError() != null){
			throw new ActiveException();
		}
		
		DelayProfiler.updateDelayNano("activeGetResult", t2);
		
		return response.getValue();
	}
	
	public String toString(){
		return this.getClass().getSimpleName()+id;
	}
	
	/**
	 * @param args
	 * @throws InterruptedException 
	 * @throws JSONException 
	 * @throws ActiveException 
	 */
	public static void main(String[] args) throws InterruptedException, JSONException, ActiveException{
		
		
	}
	
}
