package edu.umass.cs.gnsserver.activecode.prototype;


import java.io.File;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.json.JSONException;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage.Type;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Channel;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Client;
import edu.umass.cs.gnsserver.interfaces.ActiveDBInterface;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
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
	
	private final static int DEFAULT_HEAP_SIZE = 128;
	
	private ActiveQueryHandler queryHandler;
	
	private Channel channel;
	private final String ifile;
	private final String ofile;
	private final int workerNumThread;
	
	private ConcurrentHashMap<Long, Monitor> tasks = new ConcurrentHashMap<Long, Monitor>();
	
	private Process workerProc;
	final private int id;
	final private boolean pipeEnable;
	
	private final int heapSize;
	
	
	private AtomicBoolean isRestarting = new AtomicBoolean();
	
	/********************* For test **********************/
	/**
	 * @return current worker process
	 */
	public Process getWorker(){
		return workerProc;
	}
	
	private AtomicInteger counter = new AtomicInteger(0);
	
	/**
	 * 
	 * @return the total number of received responses
	 */
	public int getRecv(){
		return counter.get();
	}
	
	/**
	 * @param app 
	 * @param ifile
	 * @param ofile
	 * @param id 
	 * @param workerNumThread 
	 * @param heapSize 
	 */
	public ActiveClientWithNamedPipe(ActiveDBInterface app, String ifile, String ofile, int id, int workerNumThread, int heapSize){
		this.id = id;
		this.ifile = ifile;
		this.ofile = ofile;
		this.pipeEnable = true;
		this.workerNumThread = workerNumThread;
		this.heapSize = heapSize;
		
		initializeChannelAndStartWorker();
		
		queryHandler = new ActiveQueryHandler(app);
		
	}
	
	/**
	 * @param app
	 * @param ifile
	 * @param ofile
	 * @param id
	 * @param workerNumThread
	 */
	public ActiveClientWithNamedPipe(ActiveDBInterface app, String ifile, String ofile, int id, int workerNumThread){
		this(app, ifile, ofile, id, workerNumThread, DEFAULT_HEAP_SIZE);
	}
	
	private void initializeChannelAndStartWorker(){
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
		this.workerNumThread = workerNumThread;
		this.ifile = null;
		this.ofile = null;
		this.heapSize = DEFAULT_HEAP_SIZE;
		
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
		 * 
		 * If a null value is received, it means the worker is
		 * crashed and the pipe is closed on both end. Therefore,
		 * we need to restart the worker and establish a new
		 * connection.
		 */
		
		while(!Thread.currentThread().isInterrupted()){
			ActiveMessage response;
			try {
				if( (response = (ActiveMessage) channel.receiveMessage()) != null){					
					long id = response.getId();
					Monitor monitor = tasks.get(id);
					assert(monitor != null):"the corresponding monitor is null!";
					monitor.setResult(response, response.type == Type.RESPONSE);
				} else {
					if(!isRestarting.getAndSet(true)){
						// restart the worker
						this.shutdown();
						this.initializeChannelAndStartWorker();
						// resend all the requests that failed
						for(Monitor monitor:this.tasks.values()){
							monitor.setResult(null, false);
						}
						isRestarting.set(false);
					}
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
			//FIXME: forcibly kill the worker
			workerProc.destroyForcibly();						
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
	    command.add("edu.umass.cs.gnsserver.activecode.prototype.unblockingworker.ActiveWorker");
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
	    command.add("edu.umass.cs.gnsserver.activecode.prototype.unblockingworker.ActiveWorker");
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
			//e.printStackTrace();
		}
		return am;
	}
	
	protected synchronized void sendMessage(ActiveMessage am){
		try {
			channel.sendMessage(am);
		} catch (IOException e) {
			//e.printStackTrace();
		}
	}
	
	static int numReq = 0;
	synchronized int incr(){
		return ++numReq;
	}
	
	/**
	 * This runCode method sends the request to worker, and
	 * wait for worker to finish the request. If the worker
	 * crashed during the request execution, this method
	 * will resend the request to a new created worker, and
	 * the new worker will execute this request again. 
	 * <p>If the worker fails to execute the request, it will 
	 * send back an error to inform this method that the execution
	 * gets accomplished with an error. This method will raise
	 * an ActiveException, and the method which calls this method
	 * needs to handle this exception.
	 * 
	 * @param guid
	 * @param field
	 * @param code
	 * @param valuesMap
	 * @param ttl
	 * @return executed result sent back from worker
	 */
	@Override
	public ValuesMap runCode(InternalRequestHeader header, String guid, String field, 
			String code, ValuesMap valuesMap, int ttl, long budget) throws ActiveException {
		
		long t1 =System.nanoTime();
		ActiveMessage msg = new ActiveMessage(guid, field, code, valuesMap, ttl, budget);
		Monitor monitor = new Monitor();
		tasks.put(msg.getId(), monitor);
		
		long t2 = 0;
		ActiveMessage response = null;
		synchronized(monitor){
			while( !monitor.getDone() ){				
				try {
					if(!monitor.getWait()){
						sendMessage(msg);
						DelayProfiler.updateDelayNano("activeSendMessage", t1);
						monitor.setWait();
					}					
					monitor.wait();
				} catch (InterruptedException e) {
					// this thread is interrupted, do nothing
				}				
				t2 = System.nanoTime();
				response = monitor.getResult();	
				
				if(response == null){
					/**
					 *  The worker is crashed, resend the request.
					 *  It would work even for the query.
					 */
					sendMessage(msg);
				} else if (response.type != Type.RESPONSE){
					ActiveMessage result = queryHandler.handleQuery(response, header);
					sendMessage(result);
				}
			}		
		}
		response = monitor.getResult();
		
		if(response.getError() != null){
			throw new ActiveException();
		}
		counter.getAndIncrement();
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
		boolean waited;
		
		Monitor(){
			this.isDone = false;
			this.waited = false;
		}
		
		boolean getDone(){
			return isDone;
		}
		
		synchronized void setResult(ActiveMessage response, boolean isDone){
			this.response = response;
			this.isDone = isDone;
			notifyAll();
		}
		
		ActiveMessage getResult(){
			return response;
		}
		
		void setWait(){
			waited = true;
		}
		
		boolean getWait(){
			return waited;
		}
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
