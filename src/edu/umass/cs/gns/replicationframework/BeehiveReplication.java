package edu.umass.cs.gns.replicationframework;

import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.Util;

import java.util.HashMap;
import java.util.Map;

public class BeehiveReplication {
	
	private static double M;
	private static double base;
	
	private static Map<Integer, Double> replicationLevelMap = new HashMap<Integer, Double>();
	
	private static double xi( int i, double C, double M, double alpha, double base, int kPrime ) {
		double d_power = (1 - alpha) / alpha;
		double D = Math.pow( base, d_power );
		double CPrime = C * ( 1 - ( 1 / Math.pow(M, 1 - alpha ) ) );
		
		double xi_num = ( Math.pow(D, i) * ( kPrime - CPrime ) );
		double xi_dem = 1;
		
		for(int j = 1; j <= (kPrime - 1); j++ ) {
			xi_dem += Math.pow(D, j);
		}
		
		double xi_power = 1/( 1-alpha );
		return Math.pow( (xi_num / xi_dem), xi_power );
	}
	
	private static int getKPrime( double C, double M, double alpha, double base ) {
		double xkPrime = 0;
		int kPrime = 0;
		
		while( xkPrime < 1 ) {
			kPrime++;
			xkPrime = xi( kPrime - 1, C, M, alpha, base, kPrime );
//			System.out.println("k':" + kPrime + "\tx_" + (kPrime-1) + ":" + xkPrime );
		}
		
		return kPrime -1;
	}
	
	public static void generateReplicationLevel( double C, double M, double alpha, double base ) {
		BeehiveReplication.M = M;
		BeehiveReplication.base = base;
		
		int kPrime = getKPrime( C, M, alpha, base );
		double xi = 0;
		int level = 0;
		
		while( xi < 1) {
			xi = xi( level, C, M, alpha, base, kPrime );
			if( xi > 1 )
				replicationLevelMap.put( level, 1.0 );
			else
				replicationLevelMap.put( level, xi );
			level++;
		}
	}
	
	public static int numActiveNameServers( String name ) {
		double popularity = Integer.parseInt( name ) / M;
		int level = 0;
		for( ; level < replicationLevelMap.size(); level++ ) {
			if( popularity <= replicationLevelMap.get( level ) )
				break;
		}
		
		int numActives = Util.round( ConfigFileInfo.getNumberOfNameServers() / Math.pow( base, level ) );
		if( numActives < 1 )
			numActives = 1;
		
//		System.out.println( name + "\t" + level + "\t" + numActives );
		return numActives;
	}
	
	public static void main(String[] args) {
		int nameServerCount = 10000;
		int nameCount = 20000;
		double hopCount = 1;
		double zipfAlpha = 0.63;
		
		ConfigFileInfo.setNumberOfNameServers(nameServerCount);
		generateReplicationLevel(hopCount, nameCount, zipfAlpha, 16);
		//		System.out.println(replicationLevelMap.toString() );
//        int totalReplicas =
//		for(int i = 0; i <= nameCount; i++) {
//			if (i%100 == 0)
//			System.out.println(i + "\t" + numActiveNameServers(Integer.toString(i)));
//		}
		
		for(double j = 0.5; j <= 10.0; j = j + 0.5) {
			hopCount = j;
			generateReplicationLevel(hopCount, nameCount, 0.91, 16);
//					System.out.println(replicationLevelMap.toString() );
			int sum = 0;
			for(int i = 1; i <= nameCount; i++) {
				sum += numActiveNameServers(Integer.toString(i));
			}
			System.out.println(j + "\t" + sum*1.0/nameCount);
		}
	}
	
}
