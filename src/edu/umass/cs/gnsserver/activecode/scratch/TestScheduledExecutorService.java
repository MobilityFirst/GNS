package edu.umass.cs.gnsserver.activecode.scratch;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.*;

public class TestScheduledExecutorService {
	private final static ScheduledExecutorService scheduler =
		     Executors.newScheduledThreadPool(1);
	
	public static void main(String[] args){
		final Runnable beeper = new Runnable() {
		       public void run() { System.out.println("beep"); }
		     };
		     final ScheduledFuture<?> beeperHandle =
		       scheduler.scheduleAtFixedRate(beeper, 1, 1, TimeUnit.SECONDS);
		     
		     scheduler.schedule(new Runnable() {
		       public void run() { beeperHandle.cancel(true); }
		     }, 10, SECONDS);
		     
	}
}
