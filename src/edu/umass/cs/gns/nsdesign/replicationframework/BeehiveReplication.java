package edu.umass.cs.gns.nsdesign.replicationframework;

import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.Util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

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
		
		int numActives = Util.roundToInt( ConfigFileInfo.getNumberOfNameServers() / Math.pow( base, level ) );
		if( numActives < 1 )
			numActives = 1;
		
//		System.out.println( name + "\t" + level + "\t" + numActives );
		return numActives;
	}
	
	public static void main(String[] args) throws IOException {
		int nameServerCount = 97;
		int nameCount = 11000;
		double hopCount = 0.54;
		double zipfAlpha = 0.63;

    ConfigFileInfo.setNumberOfNameServers(nameServerCount);

    HashMap<Double,Integer> loadAuspiceReplicaCount = new HashMap<Double, Integer>();
    loadAuspiceReplicaCount.put(1.0,213332);
//    loadAuspiceReplicaCount.put(2.0,144858);
//    loadAuspiceReplicaCount.put(3.0,121721);
//    loadAuspiceReplicaCount.put(4.0,110110);
//    loadAuspiceReplicaCount.put(5.0,102362);
//    loadAuspiceReplicaCount.put(6.0, 99000);
//    loadAuspiceReplicaCount.put(7.0, 99000);
//    loadAuspiceReplicaCount.put(8.0, 99000);
    for (Double load:loadAuspiceReplicaCount.keySet()) {

      int auspiceTotalReplicas = loadAuspiceReplicaCount.get(load);
  //		System.exit(2);
      double selectedHopCount = 2.0;

      int codonsTotalReplicas = 200000;
      for(double j = 0.3; j <= 2.0; j = j + 0.02) {
        hopCount = j;
        generateReplicationLevel(hopCount, nameCount, 0.63, 16);
  //					System.out.println(replicationLevelMap.toString() );
        int sum = 0;
        for(int i = 1; i <= nameCount; i++) {
          sum += numActiveNameServers(Integer.toString(i));
        }
        System.out.println("sum = " + sum);
        if (sum < auspiceTotalReplicas) {
          break;
        }
        selectedHopCount = hopCount;
        codonsTotalReplicas = sum;
        System.out.println(j + "\t" + sum * 1.0);
      }
      System.out.println("Selected hop count\t" + selectedHopCount  + "\tCodons total replicas\t" + codonsTotalReplicas);

      generateReplicationLevel(selectedHopCount, nameCount, zipfAlpha, 16);
      //		System.out.println(replicationLevelMap.toString() );
      int NUM_RETRY = nameServerCount;
      FileWriter fw = new FileWriter(new File("nameActives-codons-load"  + load.intValue()));
  //    System.out.println("ns count \t" + ConfigFileInfo.getNumberOfNameServers());

      for(int i = 0; i <= nameCount; i++) {

        int numReplica = numActiveNameServers(Integer.toString(i));

        Set<Integer> newActiveNameServerSet = new HashSet<Integer>();

        if(numReplica == ConfigFileInfo.getNumberOfNameServers()) {
          for( int j = 0; j < ConfigFileInfo.getNumberOfNameServers(); j++ ) {
            newActiveNameServerSet.add(j);
          }
        }
        else {
          for( int j = 1; j <= numReplica; j++ ) {
            Random random = new Random(i);
            boolean added;
            int numTries = 0;
            do {
              numTries += 1;
              int newActiveNameServerId = random.nextInt(ConfigFileInfo.getNumberOfNameServers());
              added = newActiveNameServerSet.add(newActiveNameServerId);
            } while(!added && numTries < NUM_RETRY);
          }
        }

        fw.write(i + " ");
        for (int ns: newActiveNameServerSet) {
          fw.write(" " + ns);
        }
        fw.write("\n");
  //      System.out.println(i + "\t" + numReplica + "\t" + newActiveNameServerSet.size());
      }
      fw.close();

    }


	}


	
}
