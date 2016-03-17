package edu.umass.cs.gnsserver.activecode;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.concurrent.FutureTask;

import org.junit.Test;

import edu.umass.cs.gnsserver.activecode.ActiveCodeFutureTask;
import edu.umass.cs.gnsserver.activecode.ActiveCodeScheduler;
import edu.umass.cs.gnsserver.activecode.ActiveCodeTask;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeParams;
import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * @author gaozy
 *
 */
public class ActiveCodeSchedulerTest {

	/**
	 * 
	 */
	@Test
	public void testSubmit() {
		// Initialize for test only
		ActiveCodeScheduler scheduler = new ActiveCodeScheduler(null);
		
		//submit the first task
		String guid = "1";
		ActiveCodeFutureTask task1 = new ActiveCodeFutureTask(new ActiveCodeTask(null));
		scheduler.submit(task1, guid);
		assertEquals(1, scheduler.getGuidList().size());
		assertEquals(1, scheduler.getFairQueue().size());
		
		//submit the second task
		guid = "2";
		ActiveCodeFutureTask task2 = new ActiveCodeFutureTask(new ActiveCodeTask(null));
		scheduler.submit(task2, guid);
		assertEquals(2, scheduler.getGuidList().size());
		assertEquals(2, scheduler.getFairQueue().size());
		
		//submit the third task with an existing guid
		guid = "1"; 
		ActiveCodeFutureTask task3 = new ActiveCodeFutureTask(new ActiveCodeTask(null));
		scheduler.submit(task3, guid);
		assertEquals(2, scheduler.getGuidList().size());
		assertEquals(2, scheduler.getFairQueue().size());
	}

	/**
	 * 
	 */
	@Test
	public void testRemove() {
		// Initialize for test only
		ActiveCodeScheduler scheduler = new ActiveCodeScheduler(null);
		
		//submit the first task
		String guid = "1";
		ActiveCodeFutureTask task1 = new ActiveCodeFutureTask(new ActiveCodeTask(null));
		scheduler.submit(task1, guid);
		assertEquals(1, scheduler.getGuidList().size());
		assertEquals(1, scheduler.getFairQueue().size());
		
		//submit the second task
		guid = "2";
		ActiveCodeFutureTask task2 = new ActiveCodeFutureTask(new ActiveCodeTask(null));
		scheduler.submit(task2, guid);
		assertEquals(2, scheduler.getGuidList().size());
		assertEquals(2, scheduler.getFairQueue().size());
		
		//submit the third task with an existing guid
		guid = "1"; 
		ActiveCodeFutureTask task3 = new ActiveCodeFutureTask(new ActiveCodeTask(null));
		scheduler.submit(task3, guid);
		assertEquals(2, scheduler.getGuidList().size());
		assertEquals(2, scheduler.getFairQueue().size());
		
		//remove the first guid
		scheduler.removeGuid("1");
		assertEquals(1, scheduler.getGuidList().size());
		assertEquals(1, scheduler.getFairQueue().size());
		
		//remove the second guid
		scheduler.removeGuid("2");
		assertEquals(0, scheduler.getGuidList().size());
		assertEquals(0, scheduler.getFairQueue().size());
	}
		
	/**
	 * 
	 */
	@Test
	public void testGetNextGuid() {
		// Initialize for test only
		ActiveCodeScheduler scheduler = new ActiveCodeScheduler(null);
		String[] guids = {"1", "2", "3"};
		ArrayList<ActiveCodeFutureTask> tasks = new ArrayList<ActiveCodeFutureTask>();
		
		ActiveCodeParams acp = new ActiveCodeParams("", "", "", "", null, 0);
		
		for (int i=0; i<guids.length; i++){
			ActiveCodeFutureTask task = new ActiveCodeFutureTask(new ActiveCodeTask(acp));
			tasks.add(task);
		}
		
		//System.out.println(tasks);
		
		for (int i=0; i<tasks.size(); i++){
			int index = i%guids.length;
			//System.out.println(tasks.get(index)+ " "+guids[index]);
			scheduler.submit(tasks.get(index), guids[index]);
		}
		
		for (int i=0; i<tasks.size(); i++){
			int index = i%guids.length;
			String guid = scheduler.getNextGuid();
			//System.out.println(guids[index]+" "+guid);
			assertEquals(guids[index], guid);
		}
	}

	/**
	 * 
	 */
	@Test
	public void testGetNextTask() {
		ActiveCodeScheduler scheduler = new ActiveCodeScheduler(null);
		String[] guids = {"1", "2", "3"};
		ArrayList<ActiveCodeFutureTask> tasks = new ArrayList<ActiveCodeFutureTask>();
		
		for (int i=0; i<guids.length*2; i++){
			tasks.add(new ActiveCodeFutureTask(new ActiveCodeTask(null)));
		}
		ActiveCodeFutureTask task = new ActiveCodeFutureTask(new ActiveCodeTask(null));
		
		for (int i=0; i<tasks.size(); i++){
			int index = i%guids.length;
			scheduler.submit(tasks.get(i), guids[index]);
			//System.out.println(tasks.get(i)+" "+index);
		}
		scheduler.submit(task, guids[1]);
		
		for (int i=0; i<tasks.size(); i++){
			FutureTask<ValuesMap> t = scheduler.getNextTask(); 
			assertEquals(tasks.get(i), t);
			scheduler.finish(guids[i%guids.length]);
		}
		
		assertEquals(task, scheduler.getNextTask());
		
		assertEquals(null, scheduler.getNextGuid());
		
	}
	
}
