package edu.umass.cs.gnsserver.activecode;

import java.util.concurrent.ConcurrentHashMap;

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
			
	/**
	 * @param clientPool
	 */
	public ActiveCodeGuardian(ClientPool clientPool){
		 this.clientPool = clientPool;
	}
	
	public void run(){
		try {
		System.out.println(this.getClass().getName()+" runs in thread "+Thread.currentThread());
		while(true){
			/*
			 * Invariant: total number of registered tasks should be less than the total number of active code worker
			 */
			assert(tasks.size() <= AppReconfigurableNodeOptions.activeCodeWorkerCount);
			long now = System.currentTimeMillis();
			synchronized(tasks){
				for(ActiveCodeFutureTask task:tasks.keySet()){
					if (now - tasks.get(task) > 1000){
						if(ActiveCodeHandler.enableDebugging)
							System.out.println(this + " about to cancel timed out task "+task);
						
						cancelTask(task, true);
						
						if(ActiveCodeHandler.enableDebugging)
							System.out.println(this + " canceled timed out task "+task);
					}
				}		
			}
			
		}
		} catch(Exception | Error e) {
			e.printStackTrace();
		} 
	}
	
	protected void cancelTask(ActiveCodeFutureTask task, boolean remove){
		
		long start = System.currentTimeMillis();
		synchronized(task){
			if(task.isRunning()){
				// shutdown the previous worker process x
				ActiveCodeClient client = task.getWrappedTask().getClient();
				int oldPort = client.getPort();
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
				deregister(task);
		}
		
		DelayProfiler.updateDelay("ActiveCodeRestart", start);
	}
	
	public String toString() {
		return this.getClass().getSimpleName();
	}
	
	protected void register(ActiveCodeFutureTask task){
		synchronized(tasks){
			tasks.put(task, System.currentTimeMillis());
		}
	}
	
	protected Long deregister(ActiveCodeFutureTask task){
		/*
		 * FIXME: it could trigger a deadlock with calling cancelTask together
		 */
		synchronized(tasks){
			return tasks.remove(task);
		}
	}
	
}
