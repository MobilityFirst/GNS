package edu.umass.cs.gnsserver.activecode;

import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeMessage;
import edu.umass.cs.utils.DelayProfiler;

/**
 * This class periodically checks the status of each running task,
 * and terminates the task whose execution time exceeds the limitation.
 * 
 * @author Zhaoyu Gao
 */
public class ActiveCodeGuardian {
	
	// transfer it to millisecond
	private final static long timeout = 1000; // 1 second
	private final static long guard_interval = 100;

	/**
	 * fixed port for guardian to query timeout event
	 */
	public final static int guardPort = 60001;
		
	private static ClientPool clientPool;
	private static ConcurrentHashMap<ActiveCodeFutureTask, Long> tasks = new ConcurrentHashMap<ActiveCodeFutureTask, Long>();
	private final static ScheduledExecutorService scheduler =
		     Executors.newScheduledThreadPool(1);
	
	// All these data structures are for instrument only
	private static ConcurrentHashMap<String, Integer> stats = new ConcurrentHashMap<String, Integer>();
	private static long last = 0;
	private static boolean instrument = false;
	
	
	/**
	 * This socket is used for checking whether a worker is timed out.
	 */
	private static DatagramSocket guardSocket;
	private static byte[] buffer = new byte[2048];
	
	/**
	 * @param clientPool
	 */
	public ActiveCodeGuardian(ClientPool clientPool){
		try {
			guardSocket = new DatagramSocket(guardPort);
		} catch (SocketException e) {
			e.printStackTrace();
		}
		
		ActiveCodeGuardian.clientPool = clientPool;
		if (ActiveCodeConfig.activeCodeEnableTimeout){
			scheduler.scheduleAtFixedRate(new CheckAndCancelTask(), 0, guard_interval, TimeUnit.MILLISECONDS);
		}		 
	}
	
	private static class CheckAndCancelTask implements Runnable{
		
		@Override
		public void run() {
			try {
				/*
				 * Invariant: total number of registered tasks should be less than the total number of active code worker
				 */
				assert(tasks.size() <= ActiveCodeConfig.activeCodeWorkerCount);
				
				long now = System.currentTimeMillis();
				synchronized(tasks){
					for(ActiveCodeFutureTask task:tasks.keySet()){
						Long start = tasks.get(task);
						if ( start != null && now - start > timeout){
							instrument = true;
							// This is for instrument only, prevent from false canceling a benign request
							if (!task.isMalicious()){
								return;
							}
							//ActiveCodeHandler.getLogger().log(Level.WARNING, this + " takes "+ (now - start) + "ms and about to cancel timed out task "+task);
							if(task.getWrappedTask().getClient().isWorkerAlive()){
								// If the worker process is still alive, then send a cancel request to the worker
								gentlyCancelTask(task);
							} else{
								// Otherwise, substitute a worker process with a new one
								cancelTask(task);
							}
							//ActiveCodeHandler.getLogger().log(Level.WARNING, this + " cancelled timed out task "+task);
						}
					}		
				}
				
				// For instrument only
				if(instrument && now - last > 2000){
					System.out.println("Stats for all cancelled guid is : "+stats);
					System.out.println("The current tasks are "+tasks);
					last = now;
					instrument = false;
				}		
			} catch(Exception | Error e) {
				e.printStackTrace();
			} 
			
		}
		
	}	
	
	
	protected static void gentlyCancelTask(ActiveCodeFutureTask task){
		synchronized(task){
			ActiveCodeClient client = task.getWrappedTask().getClient();
			if(client != null){
				
				boolean timeout = checkNecessity(client);
				if(timeout){
					/**
					 * If it is really timed out, shutdown the socket without substituting the worker.
					 */
					client.forceShutdownSocket();
					task.cancel(true);	
					tasks.remove(task);
					if(ActiveCodeHandler.enableDebugging)
						ActiveCodeHandler.getLogger().log(Level.INFO, "REOPENED "+client+" socket and cancel the task "+task);
				}
			}
		}
	}
	
	protected static void cancelTask(ActiveCodeFutureTask task){
		
		synchronized(task){
			ActiveCodeClient client = task.getWrappedTask().getClient();
			if(client != null){
				// generate a spare worker in another thread
				long t1 = System.nanoTime();
				clientPool.generateNewWorker();
				DelayProfiler.updateDelayNano("ActiveCodeStartWorkerProcess", t1);
				
				/**
				 * This sequence is very important, otherwise it will incur bugs.
				 * 1. Lock the client by setting its state to not ready.
				 * 2. Shut down the client socket to unblock the I/O for canceling the task.
				 * 3. Update the information for client.
				 */
				client.setReady(false);
				int oldPort = client.getWorkerPort();
				client.forceShutdownSocket();
				int clientPort = client.getClientSocket().getLocalPort();
						
				// get the spare worker and set the client port to the new worker
				int newPort = clientPool.getSpareWorkerPort();
				clientPool.updatePortToClientMap(oldPort, newPort, client);
				Process proc = clientPool.getSpareWorker(newPort);
				client.setNewWorker(newPort, proc);
				
				client.setReady(clientPool.getPortStatus(newPort));
				
				if(ActiveCodeHandler.enableDebugging)
					ActiveCodeHandler.getLogger().log(Level.INFO, client+" is shutdown on port "+clientPort
							+" and starts on new port "+ client.getClientSocket().getLocalPort()+"."+
					"its origianl worker is on port "+oldPort+" and starts on its worker port "+newPort);
				
			}
			// cancel the task
			task.cancel(true);	
			tasks.remove(task);
			
			// For instrument only
			ActiveCodeTask act =  task.getWrappedTask();
			String guid = act.getParams().getGuid();
			if (stats.containsKey(guid)){
				stats.put(guid, stats.get(guid)+1);
			}else{
				stats.put(guid, 1);
			}
		}
		
	}
	
	/**
	 * @param clientPort
	 * @return true if the task has been cancelled by worker, otherwise return false to have guardian forcefully
	 * shutdown the worker.
	 */
	protected static boolean checkNecessity(ActiveCodeClient client){
		// The worker is timed out, let the worker help to check whether it's really timed out.
		ActiveCodeMessage acm = new ActiveCodeMessage();
		String error = "TimedOut";
		acm.setCrashed(error);
		ActiveCodeUtils.sendMessage(guardSocket, acm, client.getWorkerPort());
		
		boolean timeout = false;
		ActiveCodeMessage acmResp = ActiveCodeUtils.receiveMessage(guardSocket, buffer);
		if(acmResp.isCrashed()){
			// It's really timed out on the worker
			timeout = true;
		} else{
			// otherwise, do nothing, let's wait until the next time slot
		}
		return timeout;
	}
	
	public String toString() {
		return this.getClass().getSimpleName();
	}
	
	protected void register(ActiveCodeFutureTask task){
		tasks.put(task, System.currentTimeMillis());
	}
	
	protected Long deregister(ActiveCodeFutureTask task){
		/*
		 * FIXME: it could trigger a deadlock with calling cancelTask together
		 */
		return tasks.remove(task);
	}
}
