package edu.umass.cs.gns.nameserver.recordmap;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.nameserver.NameRecordV1;
import edu.umass.cs.gns.nameserver.NameRecordV1;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.nameserver.NameServer;

public class InCoreWithDiskBackupV1  extends BasicRecordMapV1 {
	
  /**
   *  
   */
  private ConcurrentMap<String, ConcurrentMap<NameRecordKey, NameRecordV1>> recordMap;
  
  /**
   * Store all files in this folder.
   */
  String dataFolder;
  
  public InCoreWithDiskBackupV1() {
    recordMap = new ConcurrentHashMap<String, ConcurrentMap<NameRecordKey, NameRecordV1>>();
    dataFolder  = StartNameServer.dataFolder + "/" + NameServer.nodeID;
    // Create a data folder with this path
    File f = new File(dataFolder);
    try {
			delete(f);
		} catch (IOException e) {
			e.printStackTrace();
		}
    f.mkdirs();
  }
  
  void delete(File f) throws IOException {
    if (f.isDirectory()) {
      for (File c : f.listFiles())
        delete(c);
    }
    if (!f.delete())
      throw new FileNotFoundException("Failed to delete file: " + f);
  }
  
  @Override
  public NameRecordV1 getNameRecord(String name, NameRecordKey recordKey) {
    if (!recordMap.containsKey(name)) {
      return null;
    } else {
      return recordMap.get(name).get(recordKey);
    }
  }

  @Override
  public void addNameRecord(NameRecordV1 recordEntry) {
  	
    if (!recordMap.containsKey(recordEntry.getName())) {
      recordMap.put(recordEntry.getName(), new ConcurrentHashMap<NameRecordKey, NameRecordV1>());
    }
    recordMap.get(recordEntry.getName()).put(recordEntry.getRecordKey(), recordEntry);
    GNS.getLogger().fine("DISKDB: Calling add record." + recordEntry.toString());
    backupRecordToDisk(recordEntry);
  }
  
  public void backupRecordToDisk(NameRecordV1 recordEntry) {

    try {
			FileWriter fw = new FileWriter(new File(dataFolder + "/" +recordEntry.getName()));
			String s =  recordEntry.toString().substring(0, 50); 
			fw.write(s);
			fw.flush();
			fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
  }
  
  @Override
  public void updateNameRecord(NameRecordV1 recordEntry) {
  	
    addNameRecord(recordEntry);
  }

  @Override
  public void removeNameRecord(String name, NameRecordKey recordKey) {
    if (recordMap.containsKey(name)) {
      recordMap.get(name).remove(recordKey);
    }
  }

  @Override
  public boolean containsName(String name, NameRecordKey recordKey) {
    if (!recordMap.containsKey(name)) {
      return false;
    } else {
      return recordMap.get(name).containsKey(recordKey);
    }
  }
  
  @Override
  public Set<NameRecordV1> getAllNameRecords() {
    Set<NameRecordV1> result = new HashSet();
    for (Map.Entry<String, ConcurrentMap<NameRecordKey, NameRecordV1>> nameEntry : recordMap.entrySet()) {
      for (Map.Entry<NameRecordKey, NameRecordV1> keyEntry : nameEntry.getValue().entrySet()) {
        result.add(keyEntry.getValue());
      }
    }
    return result;
  }

  public static void main(String[] args) {
  	
    String dataFolder = "/Users/abhigyan/Dropbox/gnrs/scripts/disk/";
    int count = 10000; 
    
    StringBuilder sb  = new StringBuilder();
  	while(sb.toString().length() < 500) 
  		sb.append(dataFolder);
  		
    Random r = new Random();
    long t1 = System.currentTimeMillis();
    for (int i = 0;i  < count; i++) {
	    try {
	    	String name = Integer.toString(r.nextInt());
	    	
	    	String value = sb.toString(); 
	    FileWriter fw = new FileWriter(new File(dataFolder + "/" + name));
	    fw.write(value);
	    fw.flush();
	    fw.close();
	    } catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	    }
    }
    long t2 = System.currentTimeMillis();
    double avg_latency = (t2 - t1)*1.0/count;
    System.out.println("Avg latency for  count = " + count + " is latency = " + avg_latency);
  }
  
  @Override
  public void reset() {
    recordMap.clear();
  }
  
}
