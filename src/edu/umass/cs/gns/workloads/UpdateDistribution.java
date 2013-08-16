package edu.umass.cs.gns.workloads;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/*************************************************************
 * This class implements update distribution of names using
 * their update rate. The distribution is used to generate
 * updates.
 * 
 * @author Hardeep Uppal
 ************************************************************/
public class UpdateDistribution {
	
	/** Size of the workload **/
	private int size;
	
	/** UpdateTrace Rate: number of updates per seconds **/
	public double avgUpdateRate; 
	
	/** Average inter-arrival time (seconds) between updates **/
	public int avgUpdateInterArrivalTime;
	
	/** Map contains update rates for each name **/
	public Map<String, UpdateInfo> updateInfoMap; 
	
	private Random random;
	
	/** Denominator **/
	public double denominator = 0.0; 
	
	/*************************************************************
	 * Constructs a new UpdateDistribution for generating updates.
	 * @param size Workload size
	 * @param avgUpdateInterArrivalTime Average inter-arrival time
	 * between updates.
	 * @param startRange Starting range for names
	 ************************************************************/
	public UpdateDistribution( int size, int avgUpdateInterArrivalTime ) {
		this.size = size;
		this.avgUpdateInterArrivalTime = avgUpdateInterArrivalTime;		
		this.avgUpdateRate = 1d / (double) avgUpdateInterArrivalTime;	//UpdateTrace Rate = 1/interArrivalTime
		this.random = new Random( System.currentTimeMillis() );
		this.updateInfoMap = new HashMap<String, UpdateInfo>();
		
		generateUpdateRate();
	}
	
	/*************************************************************
	 * Generates a map of update rates and update inter-arrival
	 * time for each name.
	 ************************************************************/
	private void generateUpdateRate() {
		//We use the update inter-arrival time as the average update time across all names
		int updateInterArrivalTime;
		double updateRate;
		for( int rank = 1; rank <= size; rank++ ) {
			updateInterArrivalTime = 1 + random.nextInt( ( avgUpdateInterArrivalTime * 2 ) - 1 );
			updateRate = 1d / (double) updateInterArrivalTime;
			updateInfoMap.put( Integer.toString( rank ), new UpdateInfo( updateInterArrivalTime, updateRate) );
			//Sume of UpdateTrace Rates over all bame: Sum(over n) Ui
			denominator += updateRate;
		}
	}
	
	public int next(){
		int rank;
		double prob; //prob is a uniform random number (0 < prob < 1)
		prob = random.nextDouble();
		
		//Map prob to the rank
		double sumProb = 0.0;
		for( rank = 1; rank <= size; rank++ ) {
			sumProb = sumProb + ( getUpdateRate( rank ) / denominator );
			if( sumProb >= prob )
				break;
		}
		return rank;
	}
	
	/**
	 * Returns the update rate given <i>rank</i>
	 * @param rank
	 * @return
	 */
	private double getUpdateRate( int rank ) {
		return updateInfoMap.get( Integer.toString( rank ) ).updateRate;
	}
	
	class UpdateInfo {
		int updateInterArrivalTime;
		double updateRate;
		
		public UpdateInfo( int updateInterArrivalTime, double updateRate ) {
			this.updateInterArrivalTime = updateInterArrivalTime;
			this.updateRate = updateRate;
		}
		
		public String toString() {
			return "InterArrivalTime:" + updateInterArrivalTime + "  UpdateRate:" + updateRate;
		}
	}
	
	/** Test **/
	public static void main(String[] args) {
		UpdateDistribution ud = new UpdateDistribution( 5, 5 );
		System.out.println( ud.avgUpdateInterArrivalTime );
		System.out.println( ud.avgUpdateRate );
		System.out.println( ud.denominator );
		
		double count = 0.0;
		int time = 0;
		double rate = 0.0; 
		for( Map.Entry<String, UpdateInfo> entry : ud.updateInfoMap.entrySet() ) {
			count++;
			UpdateInfo info = entry.getValue();
			time += info.updateInterArrivalTime;
			rate += info.updateRate;
			System.out.println( entry.getKey() + "\t" + info.updateInterArrivalTime + "\t" + info.updateRate
					+ "\t" + (info.updateRate/ud.denominator));
		}
		System.out.println("AvgTime: " + (time/count));
		System.out.println("AvgRate: " + (rate/count));
		System.out.println();
		
		Map<String, Integer> map = new HashMap<String, Integer>();
		for( int i = 1; i <= 100; i++ ) {
			String name = Integer.toString(ud.next());
			
			if( map.containsKey( name ) ) {
				int c = map.get( name ) + 1;
				map.put( name, c );
			}
			else
				map.put( name, 1 );
//			System.out.println(name);
		}
		System.out.println();
		for(Map.Entry<String, Integer> entry: map.entrySet() ) {
			System.out.println(entry.getKey() + "\t" + entry.getValue());
		}
	}
}
