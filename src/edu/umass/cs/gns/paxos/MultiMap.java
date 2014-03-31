package edu.umass.cs.gns.paxos;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import edu.umass.cs.gns.paxos.paxospacket.PValuePacket;
import edu.umass.cs.gns.paxos.paxospacket.RequestPacket;

/**
@author V. Arun
 */

/* This class implements an integer hashmap where keys are integers
 * and values are of type PaxosRequestState. This MultiMap 
 * aggregates separate hashmaps into one, i.e., instead of having
 * a separate hashmap for each member of PaxosRequestState, it has
 * a single hashmap. 
 * 
 * It is straightforward to extend MultiMap to have generic keys. 
 * 
 * For each member in PaxosRequestState, it is necessary to create
 * a set of methods (like get, put, remove, keySet, values(), etc.)
 * as exemplified below.
 * */

public class MultiMap {

	public class PaxosRequestState {
		public RequestPacket rp=null;
		public PValuePacket pvp=null; // Supporting methods not implemented yet
		public ProposalStateAtCoordinator psc=null; // Supporting methods not implemented yet
		public Double d=null;
		
		public PaxosRequestState(PaxosRequestState s) {
			if(s!=null) {
				rp = s.rp;
				pvp = s.pvp;
				psc = s.psc;
				d = s.d;
			}
		}
		public PaxosRequestState() {d=0.0;}

		public RequestPacket getRequestPacket() {return rp;}
		public void setRequestPacket(RequestPacket p) {rp=p;}

		public PValuePacket getPValuePacket() {return pvp;}
		public void setPValuePacket(PValuePacket p) {pvp=p;}

		public ProposalStateAtCoordinator getProposalStateAtCoordinator() {return psc;}
		public void setProposalStateAtCoordinator(ProposalStateAtCoordinator s) {psc=s;}

		public Double getDoubleValue() {return d;}
		public void setDoubleValue(Double d1) {d=d1;}		

	}
	
	private Map<Integer,PaxosRequestState> map=null;
	
	public MultiMap() {
		map = new TreeMap<Integer,PaxosRequestState>();
	}
		
	/* The following methods need to be added for each new member type
	 * in PaxosRequestState.
	 */
	
	/* RequestPacket methods */
	public RequestPacket getRequestPacket(int slot) { // This line needs to be customized
		return ((PaxosRequestState)map.get(slot)).rp; // This line needs to be customized
	}
	public RequestPacket put(Integer slot, RequestPacket p) { // This line needs to be customized
		PaxosRequestState prsOld = (PaxosRequestState)map.get(slot);
		PaxosRequestState prs = new PaxosRequestState(prsOld);
		prs.rp = p; // This line needs to be customized
		map.put(slot, prs);
		return prsOld!=null?prsOld.rp:null; // This line needs to be customized
	}
	public Set keySetRequestPacket() { // This line needs to be customized
		Set ksAll = map.keySet();
		Set ks = new TreeSet<RequestPacket>(); // This line needs to be customized
		for(Object o : ksAll) {
			PaxosRequestState prs = map.get(o);
			if(prs!=null && prs.rp!=null) ks.add(o); // This line needs to be customized
		}
		return ks;	
	}
	public Collection valuesRequestPacket() { // This line needs to be customized
		Collection valuesAll = map.values(); 
		Collection values = new TreeSet<Double>();
		for(Object o : valuesAll) {
			PaxosRequestState prs = (PaxosRequestState)o;
			if(prs!=null && prs.rp!=null) values.add(prs.rp);  // This line needs to be customized 
		}
		return values;
	}
	public RequestPacket removeRequestPacket(Integer slot) { // This line needs to be customized
		PaxosRequestState prsOld = map.get(slot);
		RequestPacket rp=null; // This line needs to be customized
		if(prsOld!=null && prsOld.rp!=null) { // This line needs to be customized
			rp = prsOld.rp; // This line needs to be customized
			prsOld.rp=null; // This line needs to be customized
			map.put(slot, prsOld);
		} 
		return rp; // This line needs to be customized
	}

	/* End of RequestPacket methods */

	/* DoubleValue methods */
	public Double getDoubleValue(int slot) { // This line needs to be customized
		return ((PaxosRequestState)map.get(slot)).d;
	}
	public Double put(Integer slot, Double d1) { // This line needs to be customized
		PaxosRequestState prsOld = (PaxosRequestState)map.get(slot);
		PaxosRequestState prs = new PaxosRequestState(prsOld);
		prs.d = d1; // This line needs to be customized
		map.put(slot, prs);
		return prsOld!=null?prsOld.d:null; // This line needs to be customized
	}
	public Set keySetDoubleValue() { // This line needs to be customized
		Set ksAll = map.keySet();
		Set ks = new TreeSet<Double>();
		for(Object o : ksAll) {
			PaxosRequestState prs = map.get(o);
			if(prs!=null && prs.d!=null) ks.add(o);  // This line needs to be customized 
		}
		return ks;
	}
	public Collection valuesDoubleValue() { // This line needs to be customized
		Collection valuesAll = map.values(); 
		Collection values = new TreeSet<Double>(); // This line needs to be customized
		for(Object o : valuesAll) {
			PaxosRequestState prs = (PaxosRequestState)o;
			if(prs!=null && prs.d!=null) values.add(prs.d);  // This line needs to be customized 
		}
		return values;
	}
	public Double removeDoubleValue(Integer slot) { // This line needs to be customized
		PaxosRequestState prsOld = map.get(slot);
		Double d=null; // This line needs to be customized
		if(prsOld!=null && prsOld.d!=null) { // This line needs to be customized
			d = prsOld.d; // This line needs to be customized
			prsOld.d=null; // This line needs to be customized
			map.put(slot, prsOld);
		}
		return d; // This line needs to be customized
	}
	/* End of DoubleValue methods */

	public static void main(String[] args) {
		MultiMap imap = new MultiMap();
		imap.put(3, new Double(3.42));
		System.out.println(imap.getDoubleValue(3));
		imap.put(4, new Double(43.47));
		System.out.println(imap.getDoubleValue(4));

		System.out.print("Printing double keySet: ");
		for(Object o: imap.keySetDoubleValue()) {
			System.out.print((Integer)o + ", ");
		}
		
		System.out.print("\nPrinting double valuest: ");
		for(Object o: imap.valuesDoubleValue()) {
			System.out.print((Double)o + ", ");
		}
		
		int size=1000000;

		MultiMap[] mmap = new MultiMap[size];
		for(int i=0; i<size; i++) {
			mmap[i] = new MultiMap();

			/* Each entry below seems to cost about 100B 
			 * 
			 */
			mmap[i].put(29, 33.47);
			RequestPacket p1 = new RequestPacket(3, "str", 56, true);
			mmap[i].put(93,p1);
			RequestPacket p2 = new RequestPacket(4, "str", 56, true);
			mmap[i].put(99,p2);
			RequestPacket p3 = new RequestPacket(5, "str", 56, true);
			mmap[i].put(9,p3);
			RequestPacket p4 = new RequestPacket(6, "str", 56, true);
			mmap[i].put(23,p4);

			/* Each additional Double also costs about 100B. It doesn't
			 * seem to matter if the value is a Double or a custom Object
			 * as presumably the hashmap is only storing references.
			 */
			/*
			mmap[i].put(31, 35.93);
			mmap[i].put(34, 55.32);
			mmap[i].put(91, 335.93);
			*/
			if(i%1000==0) System.out.println("Created " + i + " instances");
		}
		try {
			Thread.sleep(10000);
		} catch(Exception e){}
	}
}
