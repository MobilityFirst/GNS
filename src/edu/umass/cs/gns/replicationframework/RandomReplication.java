package edu.umass.cs.gns.replicationframework;

import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.nameserver.recordmap.ReplicaControllerRecord;
import edu.umass.cs.gns.util.ConfigFileInfo;

import java.util.*;
/**************** FIXME Package deprecated by nsdesign/replicationFramework. this will soon be deleted. **/
/*************************************************************
 * This class implements the ReplicationFramework interface
 * and is used to randomly select new active nameservers.
 * 
 * @author Hardeep Uppal
 * @deprecated
 ************************************************************/
public class RandomReplication implements ReplicationFrameworkInterface {

  @Override
  public Set<Integer> newActiveReplica(ReplicaControllerRecord nameRecordPrimary, int numReplica, int count) throws FieldNotFoundException {
    // random replicas will be selected deterministically for each name.

    if (numReplica == ConfigFileInfo.getNumberOfNameServers()) {
      Set<Integer> activeNameServerSet = new HashSet<Integer>();
      for (int i = 0; i < ConfigFileInfo.getNumberOfNameServers(); i++) {
        activeNameServerSet.add(i);
      }
      return activeNameServerSet;
    }

    Set<Integer> activeNameServers = nameRecordPrimary.getActiveNameservers();

    int numActiveNameServers = activeNameServers.size();

    if (numReplica > numActiveNameServers && count > 1) {
      //Randomly add new active name server
      int add = numReplica - numActiveNameServers;
      Set<Integer> newActiveNameServerSet = new HashSet<Integer>(activeNameServers);
      //Randomly choose active name servers from a uniform distribution between
      //0 and N where N is 'add'
      for (int i = 1; i <= add; i++) {
        Random random = new Random(new Integer(nameRecordPrimary.getName().hashCode()));
        boolean added;
        int numTries = 0;
        do {
          numTries += 1;
          int newActiveNameServerId = random.nextInt(ConfigFileInfo.getNumberOfNameServers());
          added = newActiveNameServerSet.add(newActiveNameServerId)
                  && ConfigFileInfo.getPingLatency(newActiveNameServerId) != ConfigFileInfo.INVALID_PING_LATENCY;
        } while (!added && numTries < NUM_RETRY);
      }

      return newActiveNameServerSet;
    } else if (numReplica < numActiveNameServers && count > 1) {
      //Randomly remove old active name server

      int sub = numActiveNameServers - numReplica;
      List<Integer> oldActiveNameServerSet = new ArrayList<Integer>(activeNameServers);

      // remove elements from the end of list.
      for (int i = 1; i <= sub; i++) {
        oldActiveNameServerSet.remove(oldActiveNameServerSet.size() - 1);
      }

      return new HashSet<Integer>(oldActiveNameServerSet);
    } else {
      if (count == 1) {
        Set<Integer> newActiveNameServerSet = new HashSet<Integer>();
        for (int i = 1; i <= numReplica; i++) {
          Random random = new Random(new Integer(nameRecordPrimary.getName().hashCode()));
          boolean added;
          int numTries = 0;
          do {
            numTries += 1;
            int newActiveNameServerId = random.nextInt(ConfigFileInfo.getNumberOfNameServers());
            added = newActiveNameServerSet.add(newActiveNameServerId)
                    && ConfigFileInfo.getPingLatency(newActiveNameServerId) != ConfigFileInfo.INVALID_PING_LATENCY;
          } while (!added && numTries < NUM_RETRY);
        }

        return newActiveNameServerSet;
      } else {
        //Return the old set of active name servers
        return activeNameServers;
      }
    }

  }
//	
//	@Override
//	public Set<Integer> newActiveReplica( NameRecord nameRecord, int numReplica, int count ) {
//		// random replicas will be selected deterministically for each name.
//		Random random = new Random( new Integer(nameRecord.getName()));
//		
//		if( numReplica == ConfigFileInfo.getNumberOfNameServers() ) {
//			Set<Integer> activeNameServerSet = new HashSet<Integer>();
//			for( int i = 0; i < ConfigFileInfo.getNumberOfNameServers(); i++ ) {
//				activeNameServerSet.add( i );
//			}
//			return activeNameServerSet;
//		}
//		
//		int numActiveNameServers = nameRecord.numActiveNameservers();
//		
//		if( numReplica > numActiveNameServers && count > 1 ) {
//			//Randomly add new active name server
//			int add = numReplica - numActiveNameServers;
//			Set<Integer> newActiveNameServerSet = new HashSet<Integer>( nameRecord.copyActiveNameServers() );
//			//Randomly choose active name servers from a uniform distribution between
//			//0 and N where N is 'add'
//			for( int i = 1; i <= add; i++ ) {
//				boolean added = false;
//				int numTries = 0;
//				do {
//					numTries += 1;
//					int newActiveNameServerId = random.nextInt( ConfigFileInfo.getNumberOfNameServers());
//					added = newActiveNameServerSet.add( newActiveNameServerId )
//								&& ConfigFileInfo.getPingLatency( newActiveNameServerId ) != -1;
//				} while( !added && numTries < NUM_RETRY );
//			}
//
//			return newActiveNameServerSet;
//		}
//		else if ( numReplica < numActiveNameServers && count > 1 ) {
//			//Randomly remove old active name server
//			int sub = numActiveNameServers - numReplica;
//			List<Integer> oldActiveNameServerSet = new ArrayList<Integer>( nameRecord.copyActiveNameServers() );
//			
//			for( int i = 1; i <= sub; i++) {
//				int randomIndex = random.nextInt( oldActiveNameServerSet.size() );
//				oldActiveNameServerSet.remove( randomIndex ); 
//			}
//			
//			return new HashSet<Integer>( oldActiveNameServerSet );
//		}
//		else {
//			if( count == 1 ) {
//				Set<Integer> newActiveNameServerSet = new HashSet<Integer>();
//				for( int i = 1; i <= numReplica; i++ ) {
//					boolean added = false;
//					int numTries = 0;
//					do {
//						numTries += 1;
//						int newActiveNameServerId = random.nextInt( ConfigFileInfo.getNumberOfNameServers());
//						added = newActiveNameServerSet.add( newActiveNameServerId )
//									&& ConfigFileInfo.getPingLatency( newActiveNameServerId ) != -1;
//					} while( !added && numTries < NUM_RETRY );
//				}
//
//				return newActiveNameServerSet;
//			}
//			else {
//				//Return the old set of active name servers
//				return nameRecord.copyActiveNameServers();
//			}
//		}
//		
//	}
}
