package edu.umass.cs.gnsserver.activecode;

import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

import edu.umass.cs.gnsserver.gnsApp.AppReconfigurableNodeOptions;
import edu.umass.cs.utils.DelayProfiler;

/**
 * This class periodically checks the status of each running task,
 * and terminates the task whose execution time exceeds the limitation.
 * 
 * @author Zhaoyu Gao
 */
public class ActiveCodeGuardian implements Runnable {
	private ClientPool clientPool;
	private static ConcurrentHashMap<ActiveCodeFutureTask, Long> tasks = new ConcurrentHashMap<ActiveCodeFutureTask, Long>();
	// All these data structures are for instrument only
	private static ConcurrentHashMap<String, Integer> stats = new ConcurrentHashMap<String, Integer>();
	private long last = 0;
	private boolean instrument = false;
	
	/**
	 * @param clientPool
	 */
	public ActiveCodeGuardian(ClientPool clientPool){
		 this.clientPool = clientPool;
	}
	
	public void run(){
		try {
		ActiveCodeHandler.getLogger().log(Level.INFO, this.getClass().getName()+" runs in thread "+Thread.currentThread());
		while(true){
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
						ActiveCodeHandler.getLogger().log(Level.WARNING, this + " takes "+ (now - start) + "ms and about to cancel timed out task "+task);						
						
						cancelTask(task, true);						

						ActiveCodeHandler.getLogger().log(Level.WARNING, this + " cancelled timed out task "+task);
					}
				}		
			}
			long elapsed = System.currentTimeMillis() - now;
			if(elapsed < 200){
				Thread.sleep(200-elapsed);
			}
			// For instrument only
			if(instrument && now - last > 2000){
				System.out.println("Stats for all cancelled guid is : "+stats);
				last = now;
				instrument = false;
			}
		}
		} catch(Exception | Error e) {
			e.printStackTrace();
		} 
	}
	
	protected void cancelTask(ActiveCodeFutureTask task, boolean remove){
		
		long start = System.currentTimeMillis();
		synchronized(task){
			// shutdown the previous worker process 
			ActiveCodeClient client = task.getWrappedTask().getClient();
			if(client != null){
				int oldPort = client.getWorkerPort();
				client.forceShutdownServer();
															
				// get the spare worker and set the client port to the new worker
				int newPort = clientPool.getSpareWorkerPort();
				client.setReady(clientPool.getPortStatus(newPort));
				clientPool.updatePortToClientMap(oldPort, newPort, client);
				Process proc = clientPool.getSpareWorker(newPort);
				client.setNewWorker(newPort, proc);
				
				// generate a spare worker in another thread
				long t1 = System.nanoTime();
				clientPool.generateNewWorker();
				DelayProfiler.updateDelayNano("ActiveCodeStartWorkerProcess", t1);
			}
			// cancel the task
			task.cancel(true);	
			if(remove)
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
		
		DelayProfiler.updateDelay("ActiveCodeCancel", start);
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
