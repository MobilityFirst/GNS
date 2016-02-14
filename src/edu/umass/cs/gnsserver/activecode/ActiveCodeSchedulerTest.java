package edu.umass.cs.gnsserver.activecode;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import org.junit.Test;

import edu.umass.cs.gnsserver.activecode.ActiveCodeScheduler;
import edu.umass.cs.gnsserver.utils.ValuesMap;

public class ActiveCodeSchedulerTest {

	@Test
	public void testSubmit() {
		// Initialize for test only
		ActiveCodeScheduler scheduler = new ActiveCodeScheduler(null);
		
		//submit the first task
		String guid = "1";
		FutureTask<ValuesMap> task1 = new FutureTask<ValuesMap>(new FakeTask());
		scheduler.submit(task1, guid);
		assertEquals(1, scheduler.getGuidList().size());
		assertEquals(1, scheduler.getFairQueue().size());
		
		//submit the second task
		guid = "2";
		FutureTask<ValuesMap> task2 = new FutureTask<ValuesMap>(new FakeTask());
		scheduler.submit(task2, guid);
		assertEquals(2, scheduler.getGuidList().size());
		assertEquals(2, scheduler.getFairQueue().size());
		
		//submit the third task with an existing guid
		guid = "1"; 
		FutureTask<ValuesMap> task3 = new FutureTask<ValuesMap>(new FakeTask());
		scheduler.submit(task3, guid);
		assertEquals(2, scheduler.getGuidList().size());
		assertEquals(2, scheduler.getFairQueue().size());
	}

	@Test
	public void testRemove() {
		// Initialize for test only
		ActiveCodeScheduler scheduler = new ActiveCodeScheduler(null);
		
		//submit the first task
		String guid = "1";
		FutureTask<ValuesMap> task1 = new FutureTask<ValuesMap>(new FakeTask());
		scheduler.submit(task1, guid);
		assertEquals(1, scheduler.getGuidList().size());
		assertEquals(1, scheduler.getFairQueue().size());
		
		//submit the second task
		guid = "2";
		FutureTask<ValuesMap> task2 = new FutureTask<ValuesMap>(new FakeTask());
		scheduler.submit(task2, guid);
		assertEquals(2, scheduler.getGuidList().size());
		assertEquals(2, scheduler.getFairQueue().size());
		
		//submit the third task with an existing guid
		guid = "1"; 
		FutureTask<ValuesMap> task3 = new FutureTask<ValuesMap>(new FakeTask());
		scheduler.submit(task3, guid);
		assertEquals(2, scheduler.getGuidList().size());
		assertEquals(2, scheduler.getFairQueue().size());
		
		//remove the first guid
		scheduler.remove("1");
		assertEquals(1, scheduler.getGuidList().size());
		assertEquals(1, scheduler.getFairQueue().size());
		
		//remove the second guid
		scheduler.remove("2");
		assertEquals(0, scheduler.getGuidList().size());
		assertEquals(0, scheduler.getFairQueue().size());
	}
		
	@Test
	public void testGetNextGuid() {
		// Initialize for test only
		ActiveCodeScheduler scheduler = new ActiveCodeScheduler(null);
		String[] guids = {"1", "2", "3"};
		ArrayList<FutureTask<ValuesMap>> tasks = new ArrayList<FutureTask<ValuesMap>>();
		
		for (int i=0; i<guids.length; i++){
			tasks.add(new FutureTask<ValuesMap>(new FakeTask()));
		}
		
		for (int i=0; i<tasks.size(); i++){
			int index = i%guids.length;
			scheduler.submit(tasks.get(index), guids[index]);
		}
		
		for (int i=0; i<tasks.size(); i++){
			int index = i%guids.length;
			assertEquals(guids[index], scheduler.getNextGuid());
		}
	}

	@Test
	public void testGetNextTask() {
		ActiveCodeScheduler scheduler = new ActiveCodeScheduler(null);
		String[] guids = {"1", "2", "3"};
		ArrayList<FutureTask<ValuesMap>> tasks = new ArrayList<FutureTask<ValuesMap>>();
		
		for (int i=0; i<guids.length*2; i++){
			tasks.add(new FutureTask<ValuesMap>(new FakeTask()));
		}
		FutureTask<ValuesMap> task = new FutureTask<ValuesMap>(new FakeTask());
		
		for (int i=0; i<tasks.size(); i++){
			int index = i%guids.length;
			scheduler.submit(tasks.get(i), guids[index]);
		}
		scheduler.submit(task, guids[1]);
		
		for (int i=0; i<tasks.size(); i++){
			FutureTask<ValuesMap> t = scheduler.getNextTask(); 
			assertEquals(tasks.get(i), t);
		}
		
		assertEquals(task, scheduler.getNextTask());
		
		assertEquals(null, scheduler.getNextGuid());
	}
	
	private class FakeTask implements Callable<ValuesMap>{
		public ValuesMap call(){
			ValuesMap result = null;
			return result;
		}
	}
}
