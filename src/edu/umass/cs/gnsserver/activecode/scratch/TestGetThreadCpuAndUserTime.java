package edu.umass.cs.gnsserver.activecode.scratch;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

public class TestGetThreadCpuAndUserTime {
	static ThreadMXBean bean;
	
	/** Get user time in nanoseconds. */
	private static long getUserTime( ) {
	    return bean.isCurrentThreadCpuTimeSupported( ) ?
	        bean.getCurrentThreadUserTime( ) : 0L;
	}

	/** Get system time in nanoseconds. */
	private static long getSystemTime( ) {
	    return bean.isCurrentThreadCpuTimeSupported( ) ?
	        (bean.getCurrentThreadCpuTime() - bean.getCurrentThreadUserTime( )) : 0L;
	}
	
	/** Get Cpu time in nanoseconds. */
	private static long getCpuTime( ) {
	    return bean.isCurrentThreadCpuTimeSupported( ) ?
	        bean.getCurrentThreadCpuTime()  : 0L;
	}
	
	
	static class Monitor implements Runnable{
		ThreadMXBean mxbean;
		
		private Monitor(){
			mxbean = ManagementFactory.getThreadMXBean( );
		}
		
		@Override
		public void run() {
			long last = mxbean.getCurrentThreadUserTime();
			while(true){
				long now = mxbean.getCurrentThreadUserTime();
				System.out.println(now-last);
				last = now;
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}		
	}
	
	public static void main(String[] args){
		(new Thread(new Monitor())).start(); 
		//(new Thread(new Monitor())).start();
	}
}
