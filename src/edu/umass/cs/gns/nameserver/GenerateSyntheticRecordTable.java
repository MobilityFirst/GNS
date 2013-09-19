package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaController;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaControllerRecord;
//import edu.umass.cs.gns.packet.QueryResultValue;
import edu.umass.cs.gns.util.ByteUtils;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.HashFunction;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.Set;

/**
 * **********************************************************
 * This class provides method to add synthetic workloads to
 * the name server's record table. The synthetic workload
 * consists of integers. The integer value represents the name
 * and its popularity/rank.
 *
 * @author Hardeep Uppal
 *         **********************************************************
 */

public class GenerateSyntheticRecordTable {


  public static long sleepBetweenNames = 25;


  static final String CHARACTERS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  static Random rnd = new Random(System.currentTimeMillis());

  static String randomString(int len) {
    StringBuilder sb = new StringBuilder(len);
    for (int i = 0; i < len; i++) {
      sb.append(CHARACTERS.charAt(rnd.nextInt(CHARACTERS.length())));
    }
    return sb.toString();
  }

  private static ValuesMap getValuesMapSynthetic(int numValues) {
    ValuesMap valuesMap =new ValuesMap();
    ResultValue value = new ResultValue();

    for (int i = 0; i < numValues; i++)
      value.add(randomString(10));
    valuesMap.put(NameRecordKey.EdgeRecord.getName(), value);
    return valuesMap;
  }


  public static void addNameRecordsToDB(int regularWorkloadSize, int mobileWorkloadSize) {
    for (int name = 0; name < (regularWorkloadSize + mobileWorkloadSize); name++) {
      String strName = Integer.toString(name);
      int numValues = 1;
      ValuesMap valuesMap = getValuesMapSynthetic(numValues);

      NameRecord nameRecord = new NameRecord(strName,ConfigFileInfo.getAllNameServerIDs(),strName+"-2",valuesMap);
//      nameRecord.handleNewActiveStart(ConfigFileInfo.getAllNameServerIDs(),
//              strName +"-2", valuesMap);
      // first add name record, then create paxos instance for it.
      try {
        NameServer.addNameRecord(nameRecord);
      } catch (RecordExistsException e) {
        GNS.getLogger().severe("Name record already exists. Name = " + strName);
        e.printStackTrace();
      }
      if (name > 0 && name % ((regularWorkloadSize + mobileWorkloadSize)/10) == 0) {
        System.out.println(" Name added = " + name);
      }
    }
  }

  /**
   * This method generates a record table at the name server
   * from a synthetic workload. The workload is a list of
   * integers where the integer's value represents the name
   * and its popularity/rank. The address of the name is the
   * SHA-1 hash of the name
   *
   * @param regularWorkloadSize
   * @param mobileWorkloadSize
   * @param defaultTTLRegularNames
   * @param defaultTTLMobileNames
   * @throws NoSuchAlgorithmException
   */
  public static void generateRecordTable(int regularWorkloadSize, int mobileWorkloadSize,
      int defaultTTLRegularNames,int defaultTTLMobileNames){


//		InCoreRecordMap recordMap = new InCoreRecordMap();
//		ConcurrentMap<String, NameRecord> recordTable = new ConcurrentHashMap<String, NameRecord>(
//				( regularWorkloadSize + mobileWorkloadSize + 1), 0.75f, 8);
    // reset the database
    NameServer.resetDB();
    MessageDigest sha1 = null;
    try {
      sha1 = MessageDigest.getInstance("SHA-1");
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    long t0 = System.currentTimeMillis();
    for (int name = 0; name < (regularWorkloadSize + mobileWorkloadSize); name++) {
      try {
        String strName = Integer.toString(name);
        Set<Integer> primaryNameServer = HashFunction.getPrimaryReplicas(strName);

        // Add record into the name server's record table if the name server
        // is the primary replica if this name
        if (StartNameServer.optimalReplication) {
          //Use the SHA-1 hash of the name as its address
          byte[] hash = HashFunction.SHA(strName, sha1);
          int address = ByteUtils.BAToInt(hash);

          //Generate an entry for the name and add its record to the name server record table
          ReplicaControllerRecord nameRecordPrimary = new ReplicaControllerRecord(strName);
          try {
            NameServer.addNameRecordPrimary(nameRecordPrimary);
          } catch (RecordExistsException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            continue;
          }
          long t1 = System.currentTimeMillis();
          long timeSpent = t1 - t0;
          long initScoutDelay = 0;
          if (StartNameServer.paxosStartMinDelaySec > 0 && StartNameServer.paxosStartMaxDelaySec > 0) {
            initScoutDelay = (StartNameServer.paxosStartMinDelaySec - timeSpent)*1000 + new Random().nextInt(StartNameServer.paxosStartMaxDelaySec*1000 - StartNameServer.paxosStartMinDelaySec*1000);
          }
          ValuesMap valuesMap = new ValuesMap();
          valuesMap.put(NameRecordKey.EdgeRecord.getName(), new ResultValue(Arrays.asList(Integer.toString(address))));
          try {
            ReplicaController.handleNameRecordAddAtPrimary(nameRecordPrimary, valuesMap, initScoutDelay);
          } catch (FieldNotFoundException e) {
            GNS.getLogger().fine("Field not found exception. " + e.getMessage());
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
          }
//					NameRecord recordEntry = new NameRecord( strName, NameRecordKey.EdgeRecord, new ArrayList(Arrays.asList(Integer.toString(address))));
          //Set a default ttl value for regular and mobile names
//					recordEntry.setTTL(( name <= regularWorkloadSize )? defaultTTLRegularNames : defaultTTLMobileNames);
//					NameServer.addNameRecord(recordEntry);
//					recordMap.addNameRecord(recordEntry);
//					recordTable.put( recordEntry.name, recordEntry );
        } else if (primaryNameServer.contains(NameServer.nodeID)) {
          //Use the SHA-1 hash of the name as its address
          byte[] hash = HashFunction.SHA(strName, sha1);
          int address = ByteUtils.ByteArrayToInt(hash);
          if (StartNameServer.debugMode) GNS.getLogger().fine("RecordAdded\tName:\t" + name);
          //Generate an entry for the name and add its record to the name server record table
          ReplicaControllerRecord nameRecordPrimary = new ReplicaControllerRecord(strName, true);
          try {
            NameServer.addNameRecordPrimary(nameRecordPrimary);
          } catch (RecordExistsException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            continue;
          }

//          NameServer.addNameRecord();
//          ReplicaControllerRecord nameRecordPrimary1 = NameServer.getNameRecordPrimary(strName);
//          if (StartNameServer.debugMode) GNS.getLogger().fine("Record checked out: "  + nameRecordPrimary1);

          ValuesMap valuesMap = new ValuesMap();
          valuesMap.put(NameRecordKey.EdgeRecord.getName(), new ResultValue(Arrays.asList(Integer.toString(address))));
          long t1 = System.currentTimeMillis();
          long timeSpent = t1 - t0;
          long initScoutDelay = 0;
          if (StartNameServer.paxosStartMinDelaySec > 0 && StartNameServer.paxosStartMaxDelaySec > 0) {
            initScoutDelay = (StartNameServer.paxosStartMinDelaySec*1000 - timeSpent) + new Random().nextInt(StartNameServer.paxosStartMaxDelaySec*1000 - StartNameServer.paxosStartMinDelaySec*1000);
          }
          try {
            ReplicaController.handleNameRecordAddAtPrimary(nameRecordPrimary, valuesMap, initScoutDelay);
          } catch (FieldNotFoundException e) {
            GNS.getLogger().fine("Field not found exception. " + e.getMessage());
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
          }
          if (name%1000 == 0) {
            GNS.getLogger().severe("Added record " + name + "\tinit scout delay\t" + initScoutDelay);
          }
//					NameRecord recordEntry = new NameRecord(strName, NameRecordKey.EdgeRecord, new ArrayList(Arrays.asList(Integer.toString(address))));
//
//					//Set a default ttl value for regular and mobile names
//					recordEntry.setTTL(( name <= regularWorkloadSize )? defaultTTLRegularNames : defaultTTLMobileNames);
//					NameServer.addNameRecord(recordEntry);
//          if (StartNameServer.debugMode) GNS.getLogger().fine("Name record added: " + nameRecordPrimary);
//					recordTable.put( recordEntry.name, recordEntry );

//          Thread.sleep(sleepBetweenNames);

        }

      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    long t1 = System.currentTimeMillis();
    GNS.getLogger().severe(" Time to add all records " + (t1 - t0)/1000 + " sec");

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


  public static void testMongoLookups(String[] args) {
    int numberRequests = Integer.parseInt(args[1]);
    int names = Integer.parseInt(args[2]);
    //StartNameServer.mongoPort = 12345;
    HashFunction.initializeHashFunction();

    NameServer.nodeID = 0;
    ConfigFileInfo.readHostInfo(args[0],0);
    GNS.numPrimaryReplicas = GNS.DEFAULTNUMPRIMARYREPLICAS;

//    ConfigFileInfo.setNumberOfNameServers(3);
    try{
      new NameServer(0);
    }catch (IOException exception) {
      System.out.println(" IO EXCEPTION _-- " + exception.getMessage());
      exception.printStackTrace();
    }

    addNameRecordsToDB(names,0);
    System.out.println("Name record add complete.");

    Random r = new Random();
    long t0 = System.currentTimeMillis();
    for (int i = 0; i < numberRequests; i++) {
      int name = r.nextInt(names);
      try {
        NameRecord record = NameServer.getNameRecord(Integer.toString(name));
      } catch (RecordNotFoundException e) {
        System.out.println("Name record not found. record = " + name);
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
      if (i > 0 && i % ((numberRequests)/10) == 0) {
        System.out.println(" Request complete = " + i);
        long t1 = System.currentTimeMillis();
        double throughput = (i*1.0)/(t1 - t0)*1000;
        System.out.println("Throughput = " + (int)throughput);
      }
    }
    long t1 = System.currentTimeMillis();
    double throughput = (numberRequests*1.0)/(t1 - t0)*1000;
    System.out.println("\n\nThroughput = " + (int)throughput);

    System.exit(0); // necessary to exit code.

  }


  public static void testMongoUpdates(String[] args) {
    int numberRequests = Integer.parseInt(args[1]);
//    int names = Integer.parseInt(args[2]);
    int names = 1;
    int numValues = 1;
    HashFunction.initializeHashFunction();

    //StartNameServer.mongoPort = 12345;
    NameServer.nodeID = 0;
    ConfigFileInfo.readHostInfo(args[0],0);
    GNS.numPrimaryReplicas = GNS.DEFAULTNUMPRIMARYREPLICAS;

//    ConfigFileInfo.setNumberOfNameServers(3);
    try{
      new NameServer(0);
    }catch (IOException exception) {
      System.out.println(" IO EXCEPTION _-- " + exception.getMessage());
      exception.printStackTrace();
    }

    addNameRecordsToDB(names,0);
    System.out.println("Name record add complete.");

    NameRecord record = null;
    try {
      record = NameServer.getNameRecord(Integer.toString(0));
    } catch (RecordNotFoundException e) {
      System.out.println("Record does not exist");
      System.exit(2);
      e.printStackTrace();
    }
    Random r = new Random();
    long t0 = System.currentTimeMillis();

    for (int i = 0; i < numberRequests; i++) {
//      int name = r.nextInt(names);
      record.setValuesMap(getValuesMapSynthetic(numValues));
      NameServer.updateNameRecord(record);

      if (i > 0 && i % ((numberRequests)/10) == 0) {
        System.out.println(" Request complete = " + i);
        long t1 = System.currentTimeMillis();
        double throughput = (i*1.0)/(t1 - t0)*1000;
        System.out.println("Throughput = " + (int)throughput);
      }
    }

    long t1 = System.currentTimeMillis();
    double throughput = (numberRequests*1.0)/(t1 - t0)*1000;
    System.out.println("\n\nThroughput = " + (int)throughput);

    System.exit(0); // necessary to exit code.

  }


  /**
   * Test
   *
   * @throws NoSuchAlgorithmException *
   */
  public static void main(String[] args) throws NoSuchAlgorithmException {

//    testMongoLookups(args);
    testMongoUpdates(args);

//    ConfigFileInfo.readHostInfo("/Users/hardeep/Desktop/Workspace/PlanetlabScripts/src/Ping/name_server_ssh_local", 119);
//		ConcurrentMap<String, NameRecord> table = generateRecordTable( 5000, 10000, 240, 10 );
//		System.out.println( table.size() );
//		System.out.println( table.get("1"));

//		NameServer.nodeID = 3;
//		ConcurrentMap<String, NameRecord> table2 = generateRecordTable( 10, 5, 2, 3 );
//		System.out.println( table2 );
  }

}
