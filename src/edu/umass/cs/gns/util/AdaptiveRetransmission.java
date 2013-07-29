package edu.umass.cs.gns.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;


import edu.umass.cs.gns.main.GNS;

public class AdaptiveRetransmission {

	static final double DEFAULT_DELTA = 0.05;
	
	static final double DEFAULT_MU = 1.0;
	
	static final double DEFAULT_PHI = 6.0;
	
	/**
	 * Moving average of estimatedRTT calculated using Jacobson/Karels formula.
	 */
  static double estimatedRTT = GNS.DEFAULT_QUERY_TIMEOUT;
  
  /**
   * Moving average of deviation in RTT calculated using Jacobson/Karels formula.
   */
  static double deviation = 0;
  
  /**
   * Weight assigned to latest sample in calculating moving average. 
   */
  public static double delta = DEFAULT_DELTA;
  
  /**
   * Co-efficient of estimated RTT in calculating timeout.
   */
  public static double mu = DEFAULT_MU;
  
  /**
   * Co-efficient of deviation in calculating timeout.
   */
  public static double phi = DEFAULT_PHI;
  
  
  /**
   * Get current estimate of timeout interval.
   * In future, we may return different timeout interval for different nameServers,
   * Currently, we are only implementing same timeout for all name servers. 
   * @param latestNameServerQueried
   * @return
   */
	public static synchronized long getTimeoutInterval(int latestNameServerQueried) {
		long timeout = (long) (mu * estimatedRTT + phi * deviation);
		//if (timeout > 200) timeout = 200;
		return timeout;
	}

	/**
	 * Add this estimate of response time to moving averages of estimatedRTT and deviation. 
	 * @param responseTimeSample
	 */
	public static synchronized void addResponseTimeSample(long responseTimeSample) {
		double difference = responseTimeSample - estimatedRTT;
		estimatedRTT += delta*difference;
		if (difference < 0) difference = -difference;
		deviation += delta * (difference - deviation);
	}
	
	public static void main(String args[]) {
		String filePath = "/Users/abhigyan/Documents/workspace/GNRS-westy/rttSamples2.txt";
		try {
			BufferedReader br = new BufferedReader(new FileReader(filePath));
			int count = 0;
			while(br.ready()) {
				count++;
				long val  = Long.parseLong(br.readLine());
				addResponseTimeSample(val);
//				if (count%10 == 0)
				System.out.println("Count\t" + count + "\tSample\t" + val + "\tTimeout\t" + getTimeoutInterval(0));
			}
			
			br.close();
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
}
