package edu.umass.cs.gnsserver.activecode.scratch;

import java.lang.management.ManagementFactory;

public class TestMXBean {
	
	static class Monitor implements Runnable{
		java.lang.management.OperatingSystemMXBean mxbean;
		private int id = 0;
		
		Monitor(java.lang.management.OperatingSystemMXBean mxbean, int id){
			this.mxbean = mxbean;
			this.id = id;
		}
		
		@Override
		public void run() {
			// TODO Auto-generated method stub
			//long last = mxbean.getProcessCpuTime();
			long last = 0;
			int count = 0;
			int cnt_limit = 100;
			long total = 0;
			while (count < cnt_limit){
				long now = 0; //mxbean.getProcessCpuTime();
				System.out.println(this.id+ " "+(now - last)+" ");
				total += (now - last);
				last = now;
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				count++;
			}
			System.out.println(total/cnt_limit+" ");
		}
	}
	
	public static void main(String[] args){
		int id = 0;
		java.lang.management.OperatingSystemMXBean mxbean =
				  (java.lang.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
		(new Thread(new Monitor(mxbean, id++))).start();
	}
}
