package edu.umass.cs.gnsserver.activecode;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import edu.umass.cs.gnsserver.gnsApp.AppReconfigurableNodeOptions;
import edu.umass.cs.utils.DelayProfiler;

/**
 * This class periodically checks the status of each running task,
 * and terminates the task whose execution time exceeds the limitation.
 * 
 * @author Zhaoyu Gao
 */
public class ActiveCodeGuardian {
	private static ClientPool clientPool;
	private static ConcurrentHashMap<ActiveCodeFutureTask, Long> tasks = new ConcurrentHashMap<ActiveCodeFutureTask, Long>();
	private final static ScheduledExecutorService scheduler =
		     Executors.newScheduledThreadPool(1);
	
	// All these data structures are for instrument only
	private static ConcurrentHashMap<String, Integer> stats = new ConcurrentHashMap<String, Integer>();
	private static long last = 0;
	private static boolean instrument = false;
	
	
	/**
	 * @param clientPool
	 */
	public ActiveCodeGuardian(ClientPool clientPool){
		ActiveCodeGuardian.clientPool = clientPool;
		if (AppReconfigurableNodeOptions.activeCodeEnableTimeout){
			scheduler.scheduleAtFixedRate(new CheckAndCancelTask(), 0, 500, TimeUnit.MILLISECONDS);
		}		 
	}
	
	private static class CheckAndCancelTask implements Runnable{
		
		@Override
		public void run() {
			try {
				/*
				 * Invariant: total number of registered tasks should be less than the total number of active code worker
				 */
				assert(tasks.size() <= AppReconfigurableNodeOptions.activeCodeWorkerCount);
				
				long now = System.currentTimeMillis();
				synchronized(tasks){
					for(ActiveCodeFutureTask task:tasks.keySet()){
						Long start = tasks.get(task);
						if ( start != null && now - start > 1000){
							instrument = true;
							//ActiveCodeHandler.getLogger().log(Level.WARNING, this + " takes "+ (now - start) + "ms and about to cancel timed out task "+task);						
							
							cancelTask(task);

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
	
	protected static void cancelTask(ActiveCodeFutureTask task){
		if (!task.isMalicious()){
			return;
		}
		
		//long start = System.currentTimeMillis();
		synchronized(task){
			// shutdown the previous worker process 
			ActiveCodeClient client = task.getWrappedTask().getClient();
			if(client != null){
				/**
				 * This sequence is very important, otherwise it will incur bugs.
				 * 1. Lock the client by setting its state to not ready.
				 * 2. Shut down the client socket to unblock the I/O for canceling the task.
				 * 3. Update the information for client.
				 * 
				 */
				client.setReady(false);
				int oldPort = client.getWorkerPort();
				client.forceShutdownServer();
				int clientPort = client.getClientPort();
						
				// get the spare worker and set the client port to the new worker
				int newPort = clientPool.getSpareWorkerPort();
				clientPool.updatePortToClientMap(oldPort, newPort, client);
				Process proc = clientPool.getSpareWorker(newPort);
				client.setNewWorker(newPort, proc);
				
				client.setReady(clientPool.getPortStatus(newPort));
				if(ActiveCodeHandler.enableDebugging)
					ActiveCodeHandler.getLogger().log(Level.INFO, client+" is shutdown on port "+clientPort
							+" and starts on new port "+ client.getClientPort()+"."+
					"its origianl worker is on port "+oldPort+" and starts on its worker port "+newPort);
				
				// generate a spare worker in another thread
				long t1 = System.nanoTime();
				clientPool.generateNewWorker();
				DelayProfiler.updateDelayNano("ActiveCodeStartWorkerProcess", t1);
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
