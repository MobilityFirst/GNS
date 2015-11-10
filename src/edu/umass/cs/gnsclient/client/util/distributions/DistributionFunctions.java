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

/*************************************************************************
 *  This class presents methods to generate random numbers from
 *  different distributions (bernoulli, uniform, gaussian,
 *  discrete, and exponential). The class also includes methods for
 *  shuffling an array.
 *  
 *  @author Hardeep Uppal
 *
 *************************************************************************/
public class DistributionFunctions {
	
	private static Random random = new Random( System.currentTimeMillis() );
	
	
	/*************************************************************
     * Set the seed of the psedurandom number generator.
     ************************************************************/
    public static void setSeed( long seed ) {
        random = new Random( seed );
    }

    
    /*************************************************************
     * Returns a real number from a uniform distribution in [0, 1).
     ************************************************************/
    public static double uniform() {
        return random.nextDouble();
    }
    

    /*************************************************************
     * Returns an integer between 0 and N from a uniform 
     * distribution
     ************************************************************/
    public static int uniform( int N ) {
        return random.nextInt( N + 1 );
    }

    
    /*************************************************************
     * Returns an integer in [a, b) from a uniform distribution
     ************************************************************/
    public static int uniform( int a, int b ) {
        return a + uniform( b - a );
    }

    
    /*************************************************************
     * Returns real number in [a, b) from a uniform distribution
     ************************************************************/
    public static double uniform( double a, double b ) {
        return a + uniform() * ( b - a );
    }


    /*************************************************************
     * Returns a real number with a standard Gaussian distribution
     ************************************************************/
    public static double gaussian() {
        // use the polar form of the Box-Muller transform
        double r, x, y;
        do {
            x = uniform( -1.0, 1.0 );
            y = uniform( -1.0, 1.0 );
            r = x*x + y*y;
        } while ( r >= 1 || r == 0 );
        
        return x * Math.sqrt(-2 * Math.log(r) / r);

        // Remark:  y * Math.sqrt(-2 * Math.log(r) / r)
        // is an independent random gaussian
    }

    
    /*************************************************************
     * Returns a real number from a Gaussian distribution with 
     * given mean and standard deviation
     * @param mean The mean
     * @param stddev The standard deviation
     ************************************************************/
    public static double gaussian( double mean, double stddev ) {
        return mean + stddev * gaussian();
    }

    
    /*************************************************************
     * Returns an integer with a geometric distribution with 
     * mean 1/p.
     ************************************************************/
    public static int geometric( double p ) {
        // using algorithm given by Knuth
        return (int) Math.ceil( Math.log( uniform() ) / Math.log( 1.0 - p ) );
    }
    
    
    /*************************************************************
     * Returns an integer with a Poisson distribution with mean 
     * lambda.
     ************************************************************/
    public static int poisson( double lambda ) {
        // using algorithm given by Knuth
        // see http://en.wikipedia.org/wiki/Poisson_distribution
        int k = 0;
        double p = 1.0;
        double L = Math.exp( -lambda );
        do {
            k++;
            p *= uniform();
        } while ( p >= L );
        
        return k-1;
    }

    
    /*************************************************************
     * Returns a real number with a Pareto distribution with 
     * parameter alpha.
     ************************************************************/
    public static double pareto( double alpha ) {
        return Math.pow( 1 - uniform(), -1.0/alpha ) - 1.0;
    }

    
    /*************************************************************
     * Returns a real number with a Cauchy distribution.
     ************************************************************/
    public static double cauchy() {
        return Math.tan (Math.PI * ( uniform() - 0.5 ) );
    }

    
    /*************************************************************
     * Return a number from a discrete distribution: i with 
     * probability a[i].
     ************************************************************/
    public static int discrete( double[] a ) {
        // precondition: sum of array entries equals 1
        double r = uniform();
        double sum = 0.0;
        for ( int i = 0; i < a.length; i++ ) {
            sum = sum + a[i];
            if ( sum >= r ) 
            	return i;
        }
        assert (false);
        return -1;
    }
    
    
    /*************************************************************
     * Returns a real number from an exponential distribution with
     * rate lambda.
     ************************************************************/
    public static double exponential( double lambda ) {
        return -Math.log( 1 - Math.random() ) / lambda;
    }


    /*************************************************************
     * Rearrange the elements of an array in random order.
     ************************************************************/
    public static void shuffle( Object[] a ) {
        int N = a.length;
        for ( int i = 0; i < N; i++ ) {
            int r = i + uniform( N - i );     // between i and N-1
            Object temp = a[i];
            a[i] = a[r];
            a[r] = temp;
        }
    }

    
    /*************************************************************
     * Rearrange the elements of a double array in random order.
     ************************************************************/
    public static void shuffle( double[] a ) {
        int N = a.length;
        
        for ( int i = 0; i < N; i++ ) {
            int r = i + uniform( N - i );     // between i and N-1
            double temp = a[i];
            a[i] = a[r];
            a[r] = temp;
        }
    }

    
    /*************************************************************
     * Rearrange the elements of an int array in random order.
     ************************************************************/
    public static void shuffle( int[] a ) {
        int N = a.length;
        
        for ( int i = 0; i < N; i++ ) {
            int r = i + uniform( N - i );     // between i and N-1
            int temp = a[i];
            a[i] = a[r];
            a[r] = temp;
        }
    }


    /*************************************************************
     * Rearrange the elements of the subarray a[lo..hi] in 
     * random order.
     ************************************************************/
    public static void shuffle( Object[] a, int lo, int hi ) {
        if ( lo < 0 || lo > hi || hi >= a.length )
            throw new RuntimeException( "Illegal subarray range" );
        
        for ( int i = lo; i <= hi; i++ ) {
            int r = i + uniform( hi - i + 1 );     // between i and hi
            Object temp = a[i];
            a[i] = a[r];
            a[r] = temp;
        }
    }

    /*************************************************************
     * Rearrange the elements of the subarray a[lo..hi] in 
     * random order.
     ************************************************************/
    public static void shuffle( double[] a, int lo, int hi ) {
        if ( lo < 0 || lo > hi || hi >= a.length )
            throw new RuntimeException("Illegal subarray range");
        
        for ( int i = lo; i <= hi; i++ ) {
            int r = i + uniform( hi - i + 1 );     // between i and hi
            double temp = a[i];
            a[i] = a[r];
            a[r] = temp;
        }
    }
    
    
    /*************************************************************
     * Rearrange the elements of the subarray a[lo..hi] in 
     * random order.
     ************************************************************/
    public static void shuffle( int[] a, int lo, int hi ) {
        if ( lo < 0 || lo > hi || hi >= a.length )
            throw new RuntimeException( "Illegal subarray range" );
        
        for ( int i = lo; i <= hi; i++ ) {
            int r = i + uniform( hi - i + 1 );     // between i and hi
            int temp = a[i];
            a[i] = a[r];
            a[r] = temp;
        }
    }


    /*************************************************************
     * Unit test.
     ************************************************************/
    public static void main( String[] args ) {
        int N = Integer.parseInt( args[0] );
        if ( args.length == 2 ) 
        	DistributionFunctions.setSeed( Long.parseLong( args[1] ) );

        double[] t = { .5, .3, .1, .1 };

        for ( int i = 0; i < N; i++ ) {
            System.out.println( uniform( 100 ) );
            System.out.println( uniform( 10.0, 99.0 ) );
            System.out.println( gaussian( 9.0, .2 ) );
            System.out.println( discrete( t ) );
            System.out.println();
        }
    }

}
