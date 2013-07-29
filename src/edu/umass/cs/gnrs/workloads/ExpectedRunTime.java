package edu.umass.cs.gnrs.workloads;

public class ExpectedRunTime {
	
	private final static double NUM_MOBILE_NAMES = 400;
	
	private final static int UNIFORM_DIST_MEAN = 50;
	
	public static void main( String[] args ) {
	
		//Total number of updates across all names per unit of time T.
		double totalUpdate = 0;
		
		for( double namei = 1; namei <= NUM_MOBILE_NAMES; namei++ ) {
			totalUpdate += DistrubutionFunctions.uniform( 1, ( UNIFORM_DIST_MEAN * 2 ) + 1 );
		}
		
		System.out.println( "Total Updates: " + totalUpdate );
	}
}
