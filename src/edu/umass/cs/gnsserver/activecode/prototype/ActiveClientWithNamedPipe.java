package edu.umass.cs.gnsserver.activecode.prototype;


import java.io.File;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.json.JSONException;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage.Type;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Channel;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Client;
import edu.umass.cs.gnsserver.interfaces.ActiveDBInterface;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.DelayProfiler;

/**
 * This is a Client implementation with unix named pipe as the way
 * to communicate with workers.
 * 
 * This client send requests to its worker and register a local monitor
 * to block the sending thread. Until the receiving thread receives the 
 * response or a query, it wakes up the sending thread. This design relies
 * on the fact that if the writer end of a named pipe is closed, the
 * reader end will also be closed, and return a {@code null} value.
 * Therefore, if the worker is crashed, this client will know immediately.
 *
 * @author gaozy
 *
 */
public class ActiveClientWithNamedPipe implements Runnable,Client {
	
	private ActiveQueryHandler queryHandler;
	
	private Channel channel;
	private String ifile;
	private String ofile;
	
	private ConcurrentHashMap<Long, Monitor> tasks = new ConcurrentHashMap<Long, Monitor>();
	
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
			workerProc = startWorker(ofile, ifile, id);
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
			workerProc = startWorker(serverPort, port, id);
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
		 * This is the receiving thread, it awakes the sending
		 * thread if it receives the response or query from the
		 * worker.
		 * If a NullPointerException is caught, it means the 
		 * worker is crashed, and we need to restart the worker.
		 */
		
		while(!Thread.currentThread().isInterrupted()){
			ActiveMessage response;
			try {
				if( (response = (ActiveMessage) channel.receiveMessage()) != null){
					long id = response.getId();
					Monitor monitor = tasks.get(id);
					monitor.setResult(response, response.type == Type.RESPONSE);
				} else {
					// restart the worker
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
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
	private Process startWorker(String ifile, String ofile, int id) throws IOException{
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
	 * @return
	 * @throws IOException
	 */
	private Process startWorker(int port1, int port2, int id) throws IOException{
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
	 * 
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
		Monitor monitor = new Monitor();
		tasks.put(msg.getId(), monitor);
		sendMessage(msg);
		DelayProfiler.updateDelayNano("activeSendMessage", t1);
		
		long t2 = System.nanoTime();
		ActiveMessage response = null;
		while( !monitor.getDone() ){
			synchronized(monitor){
				try {
					monitor.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			response = monitor.getResult();
			//System.out.println("After setting response, the response is "+response);
			if(response == null){
				/**
				 *  The worker is crashed, resend the request
				 *  FIXME: if it crashed during a query, this handle
				 *  will not work
				 */
				sendMessage(msg);
			} else if (response.type != Type.RESPONSE){
				ActiveMessage result = queryHandler.handleQuery(response);
				sendMessage(result);
			}
		}		
		
		response = monitor.getResult();
		//System.out.println("The responded result is "+response);
		
		if(response.getError() != null){
			throw new ActiveException();
		}
		
		tasks.remove(response.getId());
		DelayProfiler.updateDelayNano("activeGetResult", t2);
		
		return response.getValue();
	}
	
	public String toString(){
		return this.getClass().getSimpleName()+id;
	}
	
	private static class Monitor {
		boolean isDone;
		ActiveMessage response;
		
		Monitor(){
			this.isDone = false;
		}
		
		boolean getDone(){
			return isDone;
		}
		
		synchronized void setResult(ActiveMessage response, boolean isDone){
			//System.out.println("response is set to "+response);
			this.response = response;
			this.isDone = isDone;	
			notifyAll();
		}
		
		ActiveMessage getResult(){
			return response;
		}
	}
	
	/********************* For test **********************/
	/**
	 * @return current worker process
	 */
	public Process getWorker(){
		return workerProc;
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
