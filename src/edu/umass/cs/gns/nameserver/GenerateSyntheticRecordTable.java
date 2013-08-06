package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaController;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaControllerRecord;
import edu.umass.cs.gns.packet.QueryResultValue;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.HashFunction;
import edu.umass.cs.gns.util.Util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
/*************************************************************
 * This class provides method to add synthetic workloads to 
 * the name server's record table. The synthetic workload 
 * consists of integers. The integer value represents the name
 * and its popularity/rank.
 * 
 * @author Hardeep Uppal
 ************************************************************/

public class GenerateSyntheticRecordTable {

    public static long sleepBetweenNames = 25;
    /**
     * This method generates a record table at the name server
     * from a synthetic workload. The workload is a list of
     * integers where the integer's value represents the name
     * and its popularity/rank. The address of the name is the
     * SHA-1 hash of the name
     * @param regularWorkloadSize
     * @param mobileWorkloadSize
     * @param defaultTTLRegularNames
     * @param defaultTTLMobileNames
     * @throws NoSuchAlgorithmException
     */
	public static void generateRecordTable(
            int regularWorkloadSize, int mobileWorkloadSize, int defaultTTLRegularNames,
            int defaultTTLMobileNames) throws NoSuchAlgorithmException {
		Set<Integer> primaryNameserver;


//		InCoreRecordMap recordMap = new InCoreRecordMap();
//		ConcurrentMap<String, NameRecord> recordTable = new ConcurrentHashMap<String, NameRecord>(
//				( regularWorkloadSize + mobileWorkloadSize + 1), 0.75f, 8);
        // reset the database
        NameServer.resetDB();
		MessageDigest sha1 = MessageDigest.getInstance( "SHA-1" );
		
		for( int name = 0; name < ( regularWorkloadSize + mobileWorkloadSize); name++ ) {
			try {
				String strName = Integer.toString( name );
				primaryNameserver = HashFunction.getPrimaryReplicas( strName );

				// Add record into the name server's record table if the name server
				// is the primary replica if this name
				if(StartNameServer.optimalReplication) {
					//Use the SHA-1 hash of the name as its address
					byte[] hash = HashFunction.SHA( strName, sha1 );
					int address = Util.BAToInt( hash );

					//Generate an entry for the name and add its record to the name server record table
                    ReplicaControllerRecord nameRecordPrimary = new ReplicaControllerRecord(strName);
                    NameServer.addNameRecordPrimary(nameRecordPrimary);
                    ValuesMap valuesMap = new ValuesMap();
                    valuesMap.put(NameRecordKey.EdgeRecord.getName(),new QueryResultValue(new ArrayList(Arrays.asList(Integer.toString(address)))));
                    ReplicaController.handleNameRecordAddAtPrimary(nameRecordPrimary, valuesMap);
//					NameRecord recordEntry = new NameRecord( strName, NameRecordKey.EdgeRecord, new ArrayList(Arrays.asList(Integer.toString(address))));
					//Set a default ttl value for regular and mobile names
//					recordEntry.setTTL(( name <= regularWorkloadSize )? defaultTTLRegularNames : defaultTTLMobileNames);
//					NameServer.addNameRecord(recordEntry);
//					recordMap.addNameRecord(recordEntry);
//					recordTable.put( recordEntry.name, recordEntry );
				}
				else if( primaryNameserver.contains( NameServer.nodeID ) )  {
					//Use the SHA-1 hash of the name as its address
					byte[] hash = HashFunction.SHA( strName, sha1 );
					int address = Util.ByteArrayToInt( hash );
					GNS.getLogger().fine("GenerateSynthicRecordTable:\tRECORDADDED\tName:\t" + name);
					//Generate an entry for the name and add its record to the name server record table
                    ReplicaControllerRecord nameRecordPrimary = new ReplicaControllerRecord(strName);
                    NameServer.addNameRecordPrimary(nameRecordPrimary);
                    ValuesMap valuesMap = new ValuesMap();
                    valuesMap.put(NameRecordKey.EdgeRecord.getName(),new QueryResultValue(new ArrayList(Arrays.asList(Integer.toString(address)))));
                    ReplicaController.handleNameRecordAddAtPrimary(nameRecordPrimary, valuesMap);
//					NameRecord recordEntry = new NameRecord(strName, NameRecordKey.EdgeRecord, new ArrayList(Arrays.asList(Integer.toString(address))));
//
//					//Set a default ttl value for regular and mobile names
//					recordEntry.setTTL(( name <= regularWorkloadSize )? defaultTTLRegularNames : defaultTTLMobileNames);
//					NameServer.addNameRecord(recordEntry);
                    GNS.getLogger().fine("Name Record Added: " + nameRecordPrimary.toString());
//					recordTable.put( recordEntry.name, recordEntry );
                    Thread.sleep(sleepBetweenNames);
				}

			} catch ( Exception e ) {
				e.printStackTrace();
			}
		}
		
//		if( !debugMode )
//			writeTable( recordTable );
//		return recordTable;
//		return recordMap;
	}
	
//	private static void writeTable( Map<String, NameRecord> recordTable ) {
//		try {
//			FileWriter fstream = new FileWriter( "ns_table", false );
//			BufferedWriter out = new BufferedWriter( fstream );
//			for( NameRecord nameRecord : recordTable.values() ) {
//				out.write( nameRecord.toString() + "\n" );
//			}
//			out.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}
	
	/** Test 
	 * @throws NoSuchAlgorithmException **/
	public static void main(String[] args) throws NoSuchAlgorithmException {
		HashFunction.initializeHashFunction();
		NameServer.nodeID = 119;
		GNS.numPrimaryReplicas = GNS.DEFAULTNUMPRIMARYREPLICAS;
		ConfigFileInfo.readHostInfo( "/Users/hardeep/Desktop/Workspace/PlanetlabScripts/src/Ping/name_server_ssh_local", 119 );
//		ConcurrentMap<String, NameRecord> table = generateRecordTable( 5000, 10000, 240, 10 );
//		System.out.println( table.size() );
//		System.out.println( table.get("1"));
		
//		NameServer.nodeID = 3;
//		ConcurrentMap<String, NameRecord> table2 = generateRecordTable( 10, 5, 2, 3 );
//		System.out.println( table2 );
	}

}
