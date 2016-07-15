package edu.umass.cs.gnsserver.activecode.prototype.multithreading;

import java.io.File;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveDatagramChannel;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveException;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage.Type;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveNamedPipe;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveQueryHandler;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Channel;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Client;
import edu.umass.cs.gnsserver.interfaces.ActiveDBInterface;
import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * @author gaozy
 *
 */
public class MultiThreadActiveClient implements Client, Runnable{
	//TODO: handle query from worker by using queryHandler
	private ActiveQueryHandler queryHandler;
	
	private Channel channel;
	private String ifile;
	private String ofile;
		
	private Process workerProc;
	private final int id;
	private final boolean pipeEnable;
	
	private static int heapSize = 64;
	
	private final ConcurrentHashMap<Long, ActiveMessage> pendingMap = new ConcurrentHashMap<Long, ActiveMessage>();
	
	/*****************Test*******************/
	private static final AtomicInteger counter = new AtomicInteger();
	
	/**
	 * @param app
	 * @param numThread
	 * @param ifile
	 * @param ofile
	 * @param id
	 */
	public MultiThreadActiveClient(ActiveDBInterface app, int numThread, String ifile, String ofile, int id){
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
			workerProc = startWorker(ofile, ifile, id, numThread);
		} catch (IOException e) {
			e.printStackTrace();
		} 
		
		channel = new ActiveNamedPipe(ifile, ofile);
		
		queryHandler = new ActiveQueryHandler(app);
				
		System.out.println("Start "+this+" by listening on "+ifile+", and write to "+ofile);
	}
	
	/**
	 * Initialize a client with a UDP channel
	 * @param app
	 * @param numThread 
	 * @param port
	 * @param serverPort
	 * @param id
	 */
	public MultiThreadActiveClient(ActiveDBInterface app, int numThread, int port, int serverPort, int id){
		this.pipeEnable = false;
		this.id = id;
		
		try {
			// reverse the order of port and serverPort, so that worker 
			workerProc = startWorker(serverPort, port, id, numThread);
		} catch (IOException e) {
			e.printStackTrace();
		} 
		
		channel = new ActiveDatagramChannel(port, serverPort);
		queryHandler = new ActiveQueryHandler(app);
		
	}
	
	
	private Process startWorker(String ifile, String ofile, int id, int numThread) throws IOException{
		List<String> command = new ArrayList<String>();
		String classpath = System.getProperty("java.class.path");
		command.add("nice");
		command.add("-n");
		command.add("20");
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
	    command.add(Boolean.toString(pipeEnable));
	    
	    ProcessBuilder builder = new ProcessBuilder(command);
		builder.directory(new File(System.getProperty("user.dir")));
		
		builder.redirectError(Redirect.INHERIT);
		builder.redirectOutput(Redirect.INHERIT);
		//builder.redirectInput(Redirect.INHERIT);
		
		Process process = builder.start();		

		return process;
	}
	
	private Process startWorker(int serverPort, int port, int id, int numThread) throws IOException{
		List<String> command = new ArrayList<String>();
		String classpath = System.getProperty("java.class.path");
	    command.add("java");
	    command.add("-Xms"+heapSize+"m");
	    command.add("-Xmx"+heapSize+"m");
	    command.add("-cp");
	    command.add(classpath);
	    command.add("edu.umass.cs.gnsserver.activecode.prototype.multithreading.MultiThreadActiveWorker");
	    command.add(""+numThread);
	    command.add(""+serverPort);
	    command.add(""+port);
	    command.add(""+id);
	    command.add(Boolean.toString(pipeEnable));
	    
	    ProcessBuilder builder = new ProcessBuilder(command);
		builder.directory(new File(System.getProperty("user.dir")));
		
		builder.redirectError(Redirect.INHERIT);
		builder.redirectOutput(Redirect.INHERIT);
		
		Process process = builder.start();		

		return process;
	}
	
	@Override
	public ValuesMap runCode(String guid, String field, String code, ValuesMap valuesMap, int ttl)
			throws ActiveException {
		// TODO Auto-generated method stub
		ActiveMessage am = new ActiveMessage(guid, field, code, valuesMap, ttl);
		long id = am.getId();
		
		pendingMap.put(id, am);
		sendMessage(am);
		synchronized(am){
			while(pendingMap.get(id).getGuid() != null){
				try {
					am.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		ValuesMap value = pendingMap.get(id).getValue();
		pendingMap.remove(id);
		return value;
	}
	
	public void shutdown(){		
		workerProc.destroyForcibly();
		
		if(pipeEnable){
			(new File(ifile)).delete();
			(new File(ofile)).delete();
		}
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
	
	protected synchronized void sendMessage(ActiveMessage am){
		//System.out.println("send thread id is "+Thread.currentThread().getName()+" "+Thread.currentThread().getId());
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
	protected void sendRequest( String guid, String field, String code, ValuesMap valuesMap, int ttl){
		sendMessage(new ActiveMessage(guid, field, code, valuesMap, ttl));
	}

	@Override
	public void run() {
		// The thread calling runCode is different from this receive thread
		// System.out.println("receive thread is "+Thread.currentThread().getName()+" "+Thread.currentThread().getId());
		ActiveMessage response;
		try{
			while(!Thread.currentThread().isInterrupted()){
				response = receiveMessage();
				if(response != null){					
					if(response.type == Type.RESPONSE){						
						// wake up the corresponding thread
						long id = response.getId();
						ActiveMessage req = pendingMap.get(id);
						pendingMap.put(id, response);
						synchronized(req){
							req.notify();
						}
					}else{
						ActiveMessage am = queryHandler.handleQuery(response);
						sendMessage(am);
					}
					counter.incrementAndGet();
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			shutdown();
		}
	}
	
	protected int getReceived(){
		return counter.get();
	}
	
	/**
	 * @param args
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws InterruptedException {
		
		
	}
}
