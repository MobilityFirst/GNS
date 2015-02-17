package edu.umass.cs.gns.reconfiguration.reconfigurationutils;

import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.hash.Hashing;

/**
@author V. Arun
 */

/* A utility class with mostly static methods to help with
 * consistent hashing related functions.
 */
public class ConsistentHashing<NodeIDType> {
	
	private static final int DEFAULT_NUM_REPLICAS = 3;
	private int numReplicas;
	private SortedSet<Object> servers = new TreeSet<Object>();
	private Object[] serversArray; // servers as array to avoid toArray() calls
	
	public ConsistentHashing(Object[] servers) {
		this.refresh(servers, DEFAULT_NUM_REPLICAS);
	}
	public ConsistentHashing(Object[] servers, int numReplicas) {
		this.refresh(servers, numReplicas);
	}
	
	public void refresh(Object[] servers, int numReplicas) {
		for(Object obj : servers) this.servers.add(obj);
		this.serversArray = this.servers.toArray();
		this.numReplicas = numReplicas;		
	}
	public void refresh(Object[] servers) {
		refresh(servers, DEFAULT_NUM_REPLICAS);
	}
	
	public Set<NodeIDType> getReplicatedServers(String name) {
		return this.getReplicatedServers(name, this.numReplicas);
	}
	@SuppressWarnings("unchecked")
	private Set<NodeIDType> getReplicatedServers(String name, int k) {
		assert(k <= this.serversArray.length);
		int bucket = consistentHash(name, this.serversArray.length);
		TreeSet<NodeIDType> replicas = new TreeSet<NodeIDType>();
		for(int i=0; i<k; i++) {
			replicas.add((NodeIDType)this.serversArray[(bucket+i) % this.serversArray.length]);
		}

		return replicas;
	}
	
	/* Hash a name on to the nearest (following) node 
	 * on the ring. In a separate method only to make
	 * it easier to change if needed later.
	 */
	private static int consistentHash(String name, int buckets) {
		return Hashing.consistentHash(name.hashCode(), buckets);
	}
        
        // NOT USED IN NEW APP. FOR BACKWARDS COMPATIBILITY WITH OLD APP.
        // WILL BE REMOVED AFTER NEW APP IS TESTED.
        /**
         * Returns the hash for this name.
         */
        @Deprecated
        public NodeIDType getHash(String name) {
          int bucket = consistentHash(name, this.serversArray.length);
          return (NodeIDType) this.serversArray[bucket % this.serversArray.length];
        }

	// Testing
	private Set<?> getServers() {return this.servers;}
	
	public static void main(String[] args) {
		System.out.println("Consistent hash of 'Hello World' = " + Hashing.consistentHash("Hello World".hashCode(), 24));
		String[] names = {"World", "Hello", "Hello World", "1", "10", "12", "9", "34"};
		ConsistentHashing<String> ch = new ConsistentHashing<String>(names);
		System.out.println("Lexicographic ordering = " + ch.getServers());
		String name = "Hi";
		System.out.println("Replicated servers for " + name + " = " + ch.getReplicatedServers(name, 4));

	}
}
