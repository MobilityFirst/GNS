/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.gigapaxos.paxosutil;

/**
 * @author V. Arun
 * 
 *         A utility class to do some action repeatedly but in a rate limited
 *         manner.
 */
public class RateLimiter {

	private static final long MIN_SLEEP_DELAY = 10;

	private final double rate; // per second
	private long beginTime = 0;
	private int count = 0;

	/**
	 * @param r
	 *            The rate limit.
	 */
	public RateLimiter(double r) {
		this.rate = r;
	}

	/**
	 * To be invoked each time the action is done.
	 */
	public void record() {
		long curTime = System.currentTimeMillis();
		if (count++ == 0)
			this.beginTime = curTime;
		double totalTime = (curTime - this.beginTime) / 1000.0;
		double accumulatedTime = count * 1.0 / rate - totalTime;
		if (accumulatedTime >= MIN_SLEEP_DELAY / 1000.0) {
			try {
				Thread.sleep((long)(accumulatedTime*1000*0.9));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * To be invoked to start a new send spell.
	 */
	public void refresh() {
		this.count = 0;
		this.beginTime = 0;
	}

	static class Main {
		public static void main(String[] args) {
			int million = 1000000;
			int size = 1 * million;
			RateLimiter rl = new RateLimiter(size / 10);
			long t1 = System.currentTimeMillis();
			for (int i = 0; i < size; i++) {
				if (Math.random() * Math.random() > 0);
				rl.record();
			}
			System.out.println("Finished " + size + " ops in "
					+ (System.currentTimeMillis() - t1) / 1000.0 + " secs.");
		}
	}
}
