package edu.umass.cs.gigapaxos.paxosutil;

/**
 * @author V. Arun
 * 
 *         A utility class to do some action repeatedly but in a rate limited
 *         manner.
 */
public class RateLimiter {

	private static final double MIN_DELAY = 0.000001; // 1us
	private static final long MIN_SLEEP_DELAY = 5; // 5 ms

	private final double rate; // per second

	private long lastRecordedTime = 0;
	private int count = 0;

	/**
	 * @param r The rate limit.
	 */
	public RateLimiter(double r) {
		this.rate = r;
	}
	/**
	 * To be invoked each time the action is done.
	 */
	public void record() {
		long curTime = System.currentTimeMillis();
		if(count++==0) this.lastRecordedTime = curTime;
		double timeSinceLast = (curTime - this.lastRecordedTime)/1000.0;
		if(timeSinceLast==0) timeSinceLast = MIN_DELAY;
		double instaRate = count*1.0/timeSinceLast;
		double accumulatedTime = count*1.0/rate - timeSinceLast;
		if(instaRate > this.rate && accumulatedTime >= MIN_SLEEP_DELAY/1000.0) {
			try{
				count=0;
				Thread.sleep((long)(accumulatedTime*1000));
			} catch(InterruptedException e) {e.printStackTrace();}
		}
	}

	static class Main {
		public static void main(String[] args) {
			int million = 1000000;
			int size = 1 * million;
			RateLimiter rl = new RateLimiter(million / 10);
			long t1 = System.currentTimeMillis();
			for (int i = 0; i < size; i++) {
				if (Math.random() * Math.random() > 0) {
				}
				;
				rl.record();
			}
			System.out.println("Finished " + size + " ops in "
					+ (System.currentTimeMillis() - t1) / 1000.0 + " secs.");
		}
	}
}
