package edu.umass.cs.gns.workloads;

import java.util.Random;

public class ExponentialDistribution {

	/** Random number generator **/
	private Random random = new Random();
	
	private double mean;
	private double lambda;
	
	public ExponentialDistribution( double mean ) {
		this.mean = mean;
		
		this.lambda = 1d/mean;
	}
	
	public double nextEvent( int x ) {
		double dice;
		double probability;
		
		do{
			probability = lambda * Math.exp( -1d * lambda * x );  
			dice = random.nextDouble();
		} while( !( dice < probability ) );
		return probability;
	}
	
	/*************************************************************
     * Returns a real number from an exponential distribution with
     * rate lambda.
     ************************************************************/
    public  double exponential() {
        return -Math.log( 1 - Math.random() ) / lambda;
    }
	
	public static void main(String[] args) {
		ExponentialDistribution ed = new ExponentialDistribution( 1000 );
		double sum = 0;
		for( int i = 0; i < 1000; i++) {
			double exp = ed.exponential();
			sum += exp;
			System.out.println(ed.exponential());
		}
		System.out.println("Avg: " + sum / 1000);
	}
}
