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
import edu.umass.cs.utils.DelayProfiler;

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
	private HashMap<FutureTask<ValuesMap>, Long> timeMap = new HashMap<FutureTask<ValuesMap>, Long>();
	
	private Lock lock = new ReentrantLock();
	private Lock queueLock = new ReentrantLock();
	
	protected ActiveCodeScheduler(ThreadPoolExecutor executorPool){
		this.executorPool = executorPool;
	}
	
	public void run(){		
		while(true){
			//executorPool.getQueue().remainingCapacity()<=0
			while(executorPool.getActiveCount() < executorPool.getPoolSize() || guidList.isEmpty()){
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
				//for instrument only
				System.out.println("66666666666666666>> To execute task "+futureTask+", there are "+executorPool.getActiveCount()
				+" threads running, and there are "+executorPool.getPoolSize()+" threads.");
				executorPool.execute(futureTask);
				DelayProfiler.updateDelayNano("activeQueued", timeMap.get(futureTask));
			}
		}
	}
	
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
			
			while(runningGuid.containsKey(guid) && runningGuid.get(guid)>0){
				guid = getNextGuid();
			}
			if (runningGuid.containsKey(guid)){
				runningGuid.put(guid, runningGuid.get(guid)+1);
			} else{
				runningGuid.put(guid, 1);
			}
			
			futureTask = fairQueue.get(guid).pop();
			if(fairQueue.get(guid).isEmpty()){
				remove(guid);
			}
		}		
		
		return futureTask;
	}
	
	protected void submit(FutureTask<ValuesMap> futureTask, String guid){
		System.out.println("Submit task to run "+futureTask+" by guid "+guid);
		timeMap.put(futureTask, System.nanoTime());
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
		guidList.remove(guid);
		fairQueue.remove(guid);
	}
	
	protected void finish(String guid){
		runningGuid.put(guid, runningGuid.get(guid)-1);
		release();
	}
}
