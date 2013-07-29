package edu.umass.cs.gnrs.replicationframework;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;


import edu.umass.cs.gnrs.nameserver.replicacontroller.ReplicaControllerRecord;
import edu.umass.cs.gnrs.util.ConfigFileInfo;

/*************************************************************
 * This class implements the ReplicationFramework interface
 * and is used to randomly select new active nameservers.
 * 
 * @author Hardeep Uppal
 ************************************************************/
public class RandomReplication implements ReplicationFramework {

	@Override
	public Set<Integer> newActiveReplica(ReplicaControllerRecord nameRecordPrimary, int numReplica, int count ) {
		// random replicas will be selected deterministically for each name.
		
		if( numReplica == ConfigFileInfo.getNumberOfNameServers() ) {
			Set<Integer> activeNameServerSet = new HashSet<Integer>();
			for( int i = 0; i < ConfigFileInfo.getNumberOfNameServers(); i++ ) {
				activeNameServerSet.add( i );
			}
			return activeNameServerSet;
		}
		
		int numActiveNameServers = nameRecordPrimary.numActiveNameServers();
		
		if( numReplica > numActiveNameServers && count > 1 ) {
			//Randomly add new active name server
			int add = numReplica - numActiveNameServers;
			Set<Integer> newActiveNameServerSet = new HashSet<Integer>( nameRecordPrimary.copyActiveNameServers() );
			//Randomly choose active name servers from a uniform distribution between
			//0 and N where N is 'add'
			for( int i = 1; i <= add; i++ ) {
				Random random = new Random( new Integer(nameRecordPrimary.getName().hashCode()));
				boolean added;
				int numTries = 0;
				do {
					numTries += 1;
					int newActiveNameServerId = random.nextInt( ConfigFileInfo.getNumberOfNameServers());
					added = newActiveNameServerSet.add( newActiveNameServerId )
								&& ConfigFileInfo.getPingLatency( newActiveNameServerId ) != -1;
				} while( !added && numTries < NUM_RETRY );
			}

			return newActiveNameServerSet;
		}
		else if ( numReplica < numActiveNameServers && count > 1 ) {
			//Randomly remove old active name server
			
			int sub = numActiveNameServers - numReplica;
			List<Integer> oldActiveNameServerSet = new ArrayList<Integer>( nameRecordPrimary.copyActiveNameServers() );
			
			// remove elements from the end of list.
			for( int i = 1; i <= sub; i++) {
				oldActiveNameServerSet.remove( oldActiveNameServerSet.size() - 1); 
			}
			
			return new HashSet<Integer>( oldActiveNameServerSet );
		}
		else {
			if( count == 1 ) {
				Set<Integer> newActiveNameServerSet = new HashSet<Integer>();
				for( int i = 1; i <= numReplica; i++ ) {
					Random random = new Random( new Integer(nameRecordPrimary.getName().hashCode()));
					boolean added = false;
					int numTries = 0;
					do {
						numTries += 1;
						int newActiveNameServerId = random.nextInt( ConfigFileInfo.getNumberOfNameServers());
						added = newActiveNameServerSet.add( newActiveNameServerId )
									&& ConfigFileInfo.getPingLatency( newActiveNameServerId ) != -1;
					} while( !added && numTries < NUM_RETRY );
				}
				
				return newActiveNameServerSet;
			}
			else {
				//Return the old set of active name servers
				return nameRecordPrimary.copyActiveNameServers();
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
