/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy, Emmanuel Cecchet
 *
 */
package edu.umass.cs.gnsclient.client.util.distributions;

import java.util.Random;

public class ExponentialDistribution implements ProbabilityDistribution {

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

  @Override
  public double getNextArrivalDelay() {
    return exponential();
  }

  @Override
  public double getMean() {
    return mean;
  }
}
