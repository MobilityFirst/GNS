package edu.umass.cs.gnsserver.activecode.prototype.unblocking;

import com.maxmind.geoip2.DatabaseReader;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage.Type;
import edu.umass.cs.gnsserver.activecode.prototype.channels.ActiveNamedPipe;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Channel;
import org.json.JSONException;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;


public class ActiveNonBlockingWorker {
	
	private static final Logger logger = Logger.getLogger(ActiveNonBlockingWorker.class.getName());
	private static final ConsoleHandler handler = new ConsoleHandler();
	static {
		logger.setLevel(Level.ALL);
		handler.setFormatter(new SimpleFormatter());
		logger.addHandler(handler);
	}
	
	
	private final ActiveNonBlockingRunner runner;
	
	private final Channel channel;
	private final int id;
	private final DatabaseReader dbReader;
	
	private final ThreadPoolExecutor executor;
	private final ThreadPoolExecutor taskExecutor;	
	
	
	

	protected ActiveNonBlockingWorker(String ifile, String ofile, int id, int numThread, String geoip_file) {
		this.id = id;
		
		executor = new ThreadPoolExecutor(numThread, numThread, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		executor.prestartAllCoreThreads();
		taskExecutor = new ThreadPoolExecutor(numThread, numThread, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		taskExecutor.prestartAllCoreThreads();
		
		File database = new File(geoip_file);
		
		if(database.exists() && !database.isDirectory()) { 
			try {
				dbReader = new DatabaseReader.Builder(database).build();
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}else{
			dbReader = null;
		}
		
		channel = new ActiveNamedPipe(ifile, ofile);
		runner = new ActiveNonBlockingRunner(channel, dbReader);
				
		ActiveNonBlockingWorker.getLogger().log(Level.FINE, "{0} starts running", new Object[]{this});
		try {
			runWorker();
		} catch (JSONException | IOException e) {			
			ActiveNonBlockingWorker.getLogger().log(Level.WARNING, 
					"{0} catch an exception {1} and terminiates.", 
					new Object[]{this, e});			
		}finally{
			// close the channel and exit
			channel.close();
		}
		
	}

	
	private void runWorker() throws JSONException, IOException {
		
		ActiveMessage msg = null;
		while(!Thread.currentThread().isInterrupted()){
			if((msg = (ActiveMessage) channel.receiveMessage()) != null){
				ActiveNonBlockingWorker.getLogger().log(Level.FINE,
						"receive a message:{0}",
						new Object[]{msg});
				
				if(msg.type == Type.REQUEST){
					taskExecutor.submit(new ActiveWorkerSubmittedTask(executor, runner, msg, channel));					
				} else if (msg.type == Type.RESPONSE ){
					runner.release(msg);					
				} 
			}else{
				// The client is shutdown
				break;
			}
		}
	}
	
	public String toString(){
		return this.getClass().getSimpleName()+id;
	}
	
	

	protected static Logger getLogger(){
		return logger;
	}
	

	public static void main(String[] args){
		boolean pipeEnable = Boolean.parseBoolean(args[5]);
		if(pipeEnable){
			String cfile = args[0];
			String sfile = args[1];
			int id = Integer.parseInt(args[2]);
			int numThread = Integer.parseInt(args[3]);
			String geoip_file = args[4];
			
			new ActiveNonBlockingWorker(cfile, sfile, id, numThread, geoip_file);
		}
	}
}
