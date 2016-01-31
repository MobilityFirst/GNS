package edu.umass.cs.gnsserver.activecode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * This class is used to do a fair queue across all the GUIDs
 * 
 * @author Zhaoyu Gao
 */
public class ActiveCodeScheduler implements Runnable{
	private ThreadPoolExecutor executorPool;
	
	private ArrayList<String> guidList = new ArrayList<String>();
	private HashMap<String, LinkedList<FutureTask<ValuesMap>>> fairQueue = new HashMap<String, LinkedList<FutureTask<ValuesMap>>>();
	private ConcurrentHashMap<String, Integer> runningGuid = new ConcurrentHashMap<String, Integer>();
	private int ptr = 0;
	//private HashMap<FutureTask<ValuesMap>, Long> timeMap = new HashMap<FutureTask<ValuesMap>, Long>();
	
	private Lock lock = new ReentrantLock();
	private Lock queueLock = new ReentrantLock();
	
	protected ActiveCodeScheduler(ThreadPoolExecutor executorPool){
		this.executorPool = executorPool;
	}
	
	public void run(){		
		while(true){
			//System.out.println("The number of running threads is "+executorPool.getActiveCount()+"/"+executorPool.getPoolSize());
			/**
			 * If the guidList is empty, wait until there is a task comes in
			 */
			while(guidList.isEmpty()){
				synchronized (lock){
					try{
						lock.wait();
					}catch(InterruptedException e){
						e.printStackTrace();
					}
				}
			}
			FutureTask<ValuesMap> futureTask = getNextTask();
			if (futureTask != null){				
				executorPool.execute(futureTask);
				//for instrument only
				//DelayProfiler.updateDelayNano("activeQueued", timeMap.get(futureTask));
			}
		}
	}
	
	/**
	 * Called when a task is done
	 */
	protected void release(){
		synchronized(lock){
			lock.notify();
		}
	}
	
	protected String getNextGuid(){
		String guid = null;
		if (guidList.size() > ptr){
			if (ptr > 0){
				guid = guidList.get(ptr);
			}else{ 
				if(guidList.isEmpty()){
					guid = null;
					return null;
				}else{
					assert(ptr == 0);
					guid = guidList.get(ptr);
				}
			}
		}else{
			ptr = 0;
			if(guidList.isEmpty()){
				guid = null;
				return null;
			}else{
				assert(ptr == 0);
				guid = guidList.get(ptr);
			}
		}
		
		ptr++;
		return guid;
	}
	
	protected FutureTask<ValuesMap> getNextTask(){
		FutureTask<ValuesMap> futureTask = null;
		String guid = null;
		synchronized(queueLock){
			guid = getNextGuid();
			while(guid == null){
				guid = getNextGuid();
			}
			/*
			while(runningGuid.containsKey(guid) && runningGuid.get(guid)>0){
				guid = getNextGuid();
			}
			*/
			if (runningGuid.containsKey(guid)){
				runningGuid.put(guid, runningGuid.get(guid)+1);
			} else{
				runningGuid.put(guid, 1);
			}
			
			if (!fairQueue.containsKey(guid)){
				return null;
			}
			futureTask = fairQueue.get(guid).pop();
			if(fairQueue.get(guid).isEmpty()){
				remove(guid);
			}
		}		
		
		return futureTask;
	}
	
	protected void submit(FutureTask<ValuesMap> futureTask, String guid){
		//System.out.println("Submit task to run "+futureTask+" by guid "+guid);
		//timeMap.put(futureTask, System.nanoTime());
		synchronized(queueLock){
			if(fairQueue.containsKey(guid)){
				fairQueue.get(guid).add(futureTask);
				
			}else{
				LinkedList<FutureTask<ValuesMap>> taskList = new LinkedList<FutureTask<ValuesMap>>();
				taskList.add(futureTask);
				fairQueue.put(guid, taskList);
				guidList.add(guid);
			}	
			release();
		}
	}
	
	protected void remove(String guid){
		synchronized(guid){
			guidList.remove(guid);
			fairQueue.remove(guid);
		}
	}
	
	protected void finish(String guid){
		runningGuid.put(guid, runningGuid.get(guid)-1);
		release();
	}
}
