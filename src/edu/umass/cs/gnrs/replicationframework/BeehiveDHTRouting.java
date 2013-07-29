package edu.umass.cs.gnrs.replicationframework;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import edu.umass.cs.gnrs.localnameserver.LocalNameServer;
import edu.umass.cs.gnrs.main.GNS;
import edu.umass.cs.gnrs.main.StartLocalNameServer;
import edu.umass.cs.gnrs.util.ConfigFileInfo;

//import latencygenerator.LatencyGeneratorPlanetlab;
//import requestgenerator.Request;
//import requestgenerator.RequestStat;
//import util.Config;
//import util.HashFunction;
//import util.Util;

public class BeehiveDHTRouting {
	
	private Random random;
	private IDInformation[] nameId;
	private IDInformation[] nameserverId;
//	private int[] homenode;
//	private int[] replicationlevel; //for each name
//	private int[] destinationNS; //maintain the destination name server for each (lns, name) pair
	private RoutingTable[] routingtable; //for each name server
	
	int numns = 50;
	int numnames = 100;
//	int numlns = 100;
	public static int beehive_DHTbase = 16;
//	double beehive_avgHopC = 0.5;
	public static int beehive_DHTleafsetsize = 8;
//	double beehive_ZIPFalpha = 1.9;
	
	public BeehiveDHTRouting(){
		
		numns = ConfigFileInfo.getNumberOfNameServers();
		numnames = StartLocalNameServer.regularWorkloadSize + StartLocalNameServer.mobileWorkloadSize;
		
		random = new Random(System.currentTimeMillis());
		nameId = new IDInformation[numnames];
		nameserverId = new IDInformation[numns];
//		homenode = new int[numnames];
//		replicationlevel = new int[numnames];
		routingtable = new RoutingTable[numns];
//		destinationNS = new int[numnames];
//		
//		for(int name = 0; name < numnames; name++)
//			destinationNS[name] = -1;
//		for(int lns = 0; lns < numlns; lns ++)
			
		
//		Config.actives = new HashMap<Integer, ArrayList<Integer>>();
//		for (int i = 0; i < numnames; i++) 			
//			Config.actives.put(i, new ArrayList<Integer>());
		
//		Util.findNearestNsPerLns();
		
		String[] nsname = new String[numns];
		for(int i = 0; i < ConfigFileInfo.getNumberOfNameServers(); i++) {
			nsname[i] = ConfigFileInfo.getIPAddress(i + 1).getHostName();
		}
		getIDInformation(nsname);
//		getActives(); // add actives according to replication level
//		getHomenode();	//add homenode to actives	
//		writeNumActivesToFile();
		buildRoutingTable();
		
		// pre-compute all destination NS
		
	}
	
	public void replicate(){
	}
	
	public void replicateBasedOnPopularity(){	
		
	}
	
	public int getDestNS(int name, int nearestNS, ArrayList<Integer> actives) {
		// Abhigyan: names are : 0, 1, 2, ... 
		// Abhigyan: name servers are: 0, 1, 2 ...
		
		if (actives.size() == 0) {
			GNS.getLogger().fine("Active size = 0 ");
			return -1;
		}
		int destNS = -1;
		
		// don't cache responses.
//		if( (destNS = destinationNS[name]) != -1 ){
//			GNRS.getLogger().fine("Answer in destinationNS: Selected NS " + destinationNS[name] + " Name:" + name);
//		}
//		else{
//			nearestNS = Config.nearestNsPerLns[lns];
			if(actives.contains(nearestNS)){
//				destinationNS[name] = nearestNS;
				destNS = nearestNS;
				GNS.getLogger().fine("Nearest NS is in actives: Selected NS " + destNS + " Name:" + name);
			}
			else{
				GNS.getLogger().fine("Before calling Pastry routing: Selected NS " + destNS + " Name:" + name);
				destNS = pastryRouting(nearestNS, name, actives);
//				destinationNS[name] = destNS;
				GNS.getLogger().fine("Pastry returned answer: Selected NS " + destNS + " Name:" + name);
			}
//		}
		return destNS;
	}
	
//	public void updateRequestLatency(Request req){
//		int lns, nearestNS, destNS, name;
//		double latency;
//		
//		lns = req.lns;
//		name = req.name;		
//
//		if( (destNS = destinationNS[lns][name]) != -1 ){
//		}
//		else{
//			nearestNS = Config.nearestNsPerLns[lns];
//			if(Config.actives.get(name).contains(nearestNS)){
//				destinationNS[lns][name] = nearestNS;
//				destNS = nearestNS;
//			}
//			else{
//				destNS = pastryRouting(nearestNS, name);
//				destinationNS[lns][name] = destNS;
//			}
//		}
//		
//		latency = getLoadawareLatency(lns, destNS); 
//		if(!Config.requestStats.containsKey(name)){
//			ArrayList<RequestStat> arraylist = new ArrayList<RequestStat>();
//			arraylist.add(new RequestStat(lns, destNS, latency));
//			Config.requestStats.put(name, arraylist);
//		}
//		else{
//			Config.requestStats.get(name).add(new RequestStat(lns, destNS, latency));
//		}
//		
//		Config.numreqPerNs[destNS] += 1;
//	}
	
//	public double getLoadawareLatency(int lns, int ns){
//		double rtt, readload, writeload, loadawarelatency;
//		rtt = 2 * LatencyGeneratorPlanetlab.getLatency_LNSNS(lns, ns);
//		readload = Config.numreqPerNs[ns] * 1.0/Config.expduration;
//		writeload = Config.writeloadPerNs[ns];
//		
//		if( readload + writeload < Config.nscapacity )
//			loadawarelatency = rtt + 2.5/(1- (readload + writeload)/Config.nscapacity);
//		else
//			loadawarelatency = Util.LATENCY_INFINITY;
//		
//		if(loadawarelatency > Util.LATENCY_INFINITY)
//			loadawarelatency = Util.LATENCY_INFINITY;
//		
//		return loadawarelatency;
//	}
	
	private void getIDInformation(String[] nsname) {
		int i;
		byte[] bytes = new byte[16];
		String[] sitename = new String[numnames];
//		String[] nsname = new String[numns];
			
		for(i = 0; i < numnames; i ++)
			sitename[i] = Integer.toString(i);
		
		//read nameserver name
//		try{
//			/*FileReader fr = new FileReader(Config.sitenamefile);
//			BufferedReader br = new BufferedReader( fr);
//			while (br.ready()){
//				String[] tokens = br.readLine().split("\t");
//				if (tokens[0].contains("#") || tokens.length < 2)
//					continue;
//				sitename[ Integer.parseInt(tokens[0]) ] = tokens[1];
//			}		
//			br.close();
//			fr.close();*/
//			
//			i = 0;
//			FileReader fr = new FileReader(Config.nsnamefile);
//			BufferedReader br = new BufferedReader( fr);
//			while (br.ready()){				
//				nsname[ i ] = br.readLine();
//				i ++;
//			}		
//			br.close();
//			fr.close();
//		}catch (Exception e){
//			GNRS.getLogger().fine("Error in getSHAId read files" + e.getMessage());
//			System.exit(1);
//		}
		
		//get the 128-bit identifier using MD5 hashing algorithm for name and nameserver
		//The SHA-1 hashing returns 160bit identifier which is not suitable for us
		try{
			MessageDigest md = MessageDigest.getInstance("MD5");
			for (i = 0; i < numnames; i ++){
				md.update( sitename[i].getBytes(), 0, sitename[i].length() );
				bytes = md.digest();
				nameId[i] = new IDInformation(bytes);
			}
			
			for(i = 0; i < numns; i ++){
				md.update( nsname[i].getBytes(), 0, nsname[i].length());
				bytes = md.digest();
				nameserverId[i] = new IDInformation(bytes);
			}
			
		}catch(Exception e){
			GNS.getLogger().fine("Error in getSHAId " + e.getMessage());
			System.exit(1);
		}
		
	}
	
//	//store each name at the closest name server in the identifier space
//	private void getHomenode(){
//		int name, node, nearestnode;
//		BigInteger nameBiginteger, nodeBiginteger, diff, smallestdiff;
//		
//		for(name = 0; name < numnames; name ++){
//			nameBiginteger = nameId[name].bigInteger;
//			nearestnode = 0;
//			nodeBiginteger = nameserverId[nearestnode].bigInteger;
//			smallestdiff = nameBiginteger.subtract(nodeBiginteger).abs();
//			
//			for(node = 1; node < numns; node ++){
//				diff = nameBiginteger.subtract( nameserverId[node].bigInteger).abs();
//				if( diff.compareTo(smallestdiff) < 0 ){
//					nearestnode = node;
//					smallestdiff = diff;
//				}
//			}
//			homenode[name] = nearestnode;
//			if(!Config.actives.get(name).contains(nearestnode))
//				Config.actives.get(name).add(nearestnode);
//		}
//	}
	
	//get actives for each name based on replication level, it is based on DHTbase=16
//	private void getActives(){
//		int name, level, ns;
//		String nodePrefix, namePrefix;
//		
//		getReplicationLevel();
//		
//		for(name = 0; name < numnames; name ++){
//			level = replicationlevel[name];
//			if(level == 0){
//				for(ns = 0; ns < numns; ns ++)
//					Config.actives.get(name).add(ns);
//			}
//			else{ //matching hex digit = level
//				namePrefix = nameId[name].hexformat.substring(0, level);
//				for(ns = 0; ns < numns; ns ++){
//					nodePrefix = nameserverId[ns].hexformat.substring(0, level);
//					if( nodePrefix.equals(namePrefix))
//						Config.actives.get(name).add(ns);
//				}
//			}
//		}
//		
//		//add 3 primaries
//		Set<Integer> primarySet;		
//		try{
//			HashFunction.initializeHashFunction();
//			for (name = 0; name < numnames; name ++) {
//				primarySet = HashFunction.getPrimaryReplicas(Integer.toString(name));
//				for( int nsID: primarySet){
//					if(!Config.actives.get(name).contains(nsID))
//						Config.actives.get(name).add(nsID);
//				}			
//			}
//		}catch(Exception e){
//		}
//	}
	
	//build routing table and leaf set for each node
	private void buildRoutingTable(){
		int numrow, numcolumn, row, column, ns;
		
		numrow =  (int) Math.ceil( Math.log(numns)/Math.log(beehive_DHTbase) );
		numcolumn = beehive_DHTbase;
		if( (beehive_DHTleafsetsize %2 != 0) || (beehive_DHTleafsetsize > numns-1 ) ){
			GNS.getLogger().fine("Improper dhtleafsetsize " + beehive_DHTleafsetsize);
			System.exit(1);
		}
		for(ns = 0; ns < numns; ns++){
			routingtable[ns] = new RoutingTable(numrow, numcolumn, beehive_DHTleafsetsize);
			for(row = 0; row < numrow; row ++){
				for(column = 0; column < numcolumn; column ++){
					routingtable[ns].rtable[row][column] = buildRoutingEntry(ns, row, column);
				}
			}
			
			buildLeafset(ns);
//			buildneighborset(ns);
		}				
	}
	
//	private void getReplicationLevel(){
//		int level = 0; 
//		double xi = 0.0, popularity = 0.0;
//		int kPrime = getKPrime( beehive_avgHopC, numnames, beehive_ZIPFalpha, beehive_DHTbase );
//		Map<Integer, Double> replicationLevelMap = new HashMap<Integer, Double>();
//			
//		while( xi < 1) {
//			xi = xi( level, beehive_avgHopC, numnames, beehive_ZIPFalpha, beehive_DHTbase, kPrime );
//			if( xi >= 1 )
//				replicationLevelMap.put( level, 1.0 );
//			else
//				replicationLevelMap.put( level, xi );
//			level++;
//		}
//		
//		//int numActives = 0;
//		for(int name = 0; name < numnames; name ++){
//			popularity =  (name+1.0) / numnames;
//			level = 0;
//			for( ; level < replicationLevelMap.size(); level++ ) {
//				if( popularity <= replicationLevelMap.get( level ) )
//					break;
//			}
//			replicationlevel[name] = level;
//		}		
//	}
	
	private double xi( int i, double C, double M, double alpha, double base, int kPrime ) {
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
	
	private int getKPrime( double C, double M, double alpha, double base ) {
		double xkPrime = 0;
		int kPrime = 0;
		
		while( xkPrime < 1 ) {
			kPrime++;
			xkPrime = xi( kPrime - 1, C, M, alpha, base, kPrime );
		}		
		return kPrime -1;
	}
	
	//build the routing entry for node at row and column
	private int buildRoutingEntry(int node, int row, int column){
		int routingentry, othernode;
		double smallestlatency;
		String nodeprefix, othernodeprefix, nodedigit, othernodedigit;
		ArrayList<Integer> matchingnode = new ArrayList<Integer>();
		
		nodedigit = nameserverId[node].hexformat.substring(row, row+1);
		if ( nodedigit.equals(Integer.toHexString(column)) )
			return node;
		
		for(othernode = 0; othernode < numns; othernode ++){	
			if (othernode == node)
				continue;
			if(row == 0){
				nodeprefix = "";
				othernodeprefix = "";
			}else{
				nodeprefix = nameserverId[node].hexformat.substring(0, row);
				othernodeprefix = nameserverId[othernode].hexformat.substring(0, row);
			}
			if(othernodeprefix.equals(nodeprefix)){
				othernodedigit = nameserverId[othernode].hexformat.substring(row, row+1);
				if( othernodedigit.equals(Integer.toHexString(column)) )
					matchingnode.add(othernode);
			}
		}
		// Abhigyan: 
		if (matchingnode.size() == 0) return -1;
		// return random entry.
		return matchingnode.get(random.nextInt(matchingnode.size()));
		
//		routingentry = -1;
//		smallestlatency = Double.MAX_VALUE;
//		for(int i:matchingnode){
//			if (Config.latencynsns[node][i] < smallestlatency){
//				smallestlatency = Config.latencynsns[node][i];
//				routingentry = i;
//			}
//		}
//		
//		return routingentry;
	}
	
	private void buildLeafset(int ns){
		int otherns, index;
		BigInteger bigInteger_ns, bigInteger_otherns;
		TreeMap<BigInteger, Integer> halfleaf = new TreeMap<BigInteger, Integer>(); 
		
		bigInteger_ns = nameserverId[ns].bigInteger;
		//find leafsize/2 numerically closest smaller nodes
		for(otherns = 0; otherns < numns; otherns ++){
			if(otherns == ns)
				continue;
			bigInteger_otherns = nameserverId[otherns].bigInteger;
			if( bigInteger_otherns.compareTo( bigInteger_ns) < 0)
				halfleaf.put(bigInteger_ns.subtract(bigInteger_otherns), otherns);
		}
		index = 0;
		for(Map.Entry<BigInteger, Integer> entry: halfleaf.entrySet()){
			routingtable[ns].leafset.add( entry.getValue() );
			index ++;
			if(index == (int)(beehive_DHTleafsetsize/2))
				break;
		}
		halfleaf.clear();
		
		//find leafsize/2 numerically closest larger nodes
		for(otherns = 0; otherns < numns; otherns ++){
			if(otherns == ns)
				continue;
			bigInteger_otherns = nameserverId[otherns].bigInteger;
			if( bigInteger_otherns.compareTo( bigInteger_ns) >= 0)
				halfleaf.put(bigInteger_otherns.subtract(bigInteger_ns), otherns);
		}
		index = (int)(beehive_DHTleafsetsize/2);
		for(Map.Entry<BigInteger, Integer> entry: halfleaf.entrySet()){
			routingtable[ns].leafset.add( entry.getValue() );
			index ++;
			if(index == beehive_DHTleafsetsize)
				break;
		}
		halfleaf.clear();	
	}
	
//	private void buildneighborset(int ns){ //maintain the M nodes that are closest to the local node (using proximity metric)
//		int otherns, index;
//		double latency;
//		TreeMap<Double, Integer> neighbor = new TreeMap<Double, Integer>(); //key:latency, value:node 
//		
//		if(Config.beehive_DHTneighborsetsize > numns-1){
//			GNRS.getLogger().fine("DHTneighborsetsize greater than numns-1 :" + Config.beehive_DHTneighborsetsize);
//			System.exit(1);
//		}
//			
//		for(otherns = 0; otherns < numns; otherns ++){
//			if (otherns == ns)
//				continue;
//			latency = Config.latencynsns[ns][otherns];
//			neighbor.put(latency, otherns);
//		}
//		
//		index = 0;
//		for(Map.Entry<Double, Integer> entry:neighbor.entrySet()){
//			routingtable[ns].neighborset[index] = entry.getValue();
//			index ++;
//			if (index == beehive_DHTleafsetsize)
//				break;
//		}
//	}
	
	private int pastryRouting(int node, int name, ArrayList<Integer> actives){ //return destination node
		int leafsetsize, lowleaf, highleaf;
		int tempnode, destnode, size;
		int shl, digitAtL, shl1;
		BigInteger diff, smallestDiff;
		String nameprefix, nodeprefix;
		ArrayList<Integer> setT;
		
		if(actives.contains(node)){
			GNS.getLogger().fine("node " + node + " is active replica of name " + name);
			System.exit(1);
		}
		
		leafsetsize = routingtable[node].leafset.size();
		if(leafsetsize > 0 ){  //use leaf set
			lowleaf = routingtable[node].leafset.get(0);
			highleaf = routingtable[node].leafset.get(leafsetsize-1);
		// Abhigyan: trying to find home node?
//			if( nameserverId[lowleaf].bigInteger.compareTo(nameId[name].bigInteger) <= 0 &&
//					nameId[name].bigInteger.compareTo(nameserverId[highleaf].bigInteger) <=0 ){
//				size = actives.size();
//				destnode = actives.get(random.nextInt(size));
//				//find Li such that |D-Li| is minimal
//				tempnode = routingtable[node].leafset.get(0);
//				smallestDiff = nameserverId[tempnode].bigInteger.subtract(nameId[name].bigInteger).abs();
//				for(int i:routingtable[node].leafset){
//					diff = nameserverId[i].bigInteger.subtract(nameId[name].bigInteger).abs();
//					if(diff.compareTo(smallestDiff) <= 0){
//						smallestDiff = diff;
//						destnode = i;
//					}
//				}				
//				return destnode;
//			}
		}
		
		//use routing table			
		shl = 0;
		nameprefix = nameId[name].hexformat.substring(0, shl+1);
		nodeprefix = nameserverId[node].hexformat.substring(0, shl+1);
		while( nameprefix.equals(nodeprefix) && shl + 1 < routingtable[node].rtable.length){
			shl += 1;
			nameprefix = nameId[name].hexformat.substring(0, shl+1);
			nodeprefix = nameserverId[node].hexformat.substring(0, shl+1);
		}
		digitAtL = Integer.parseInt( nameId[name].hexformat.substring(shl, shl+1), 16);
		
		// ABHIGYAN: CODE TO PRINT DEBUG OUTPUT.
//		try{
//			int x = routingtable[node].rtable[shl][digitAtL];
//		}catch (Exception e) {
////			GNRS.getLogger().fine("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n");
//			GNRS.getLogger().fine(" name  = " + name + " hex = " + nameId[name].hexformat);
//			GNRS.getLogger().fine(" node = " + node + " hex = " + nameserverId[node].hexformat);
//			GNRS.getLogger().fine(" shl = " + shl);
//			GNRS.getLogger().fine(" digitAtL = " + digitAtL);
//			GNRS.getLogger().fine(" routing table dimensions: " + routingtable[node].rtable.length  
//					+ " " + routingtable[node].rtable[0].length);
//			StringBuilder s = new StringBuilder();
//			for (StackTraceElement s1 : e.getStackTrace()) {
//				s.append(s1.toString() + "\n");
//			}
//			
//			GNRS.getLogger().fine("Pastry Routing Bug: " + s.toString());
//		}
			
		if( routingtable[node].rtable[shl][digitAtL] != -1){
			if( routingtable[node].rtable[shl][digitAtL] == node){
				GNS.getLogger().fine("ERROR in pastryRouting: route loop. returning random. Don't quit.");
				return actives.get(random.nextInt(actives.size()));
//				System.exit(1);
			}
			if(actives.contains( routingtable[node].rtable[shl][digitAtL] ))
				return routingtable[node].rtable[shl][digitAtL];
			else
				return pastryRouting(routingtable[node].rtable[shl][digitAtL], name, actives);
		}
		else{ //rare case: Abhigyan: ignore rare case: return random.
			//build set T = leafset+routingtable+neighborset
//			setT = new ArrayList<Integer>();
//			for(int i:routingtable[node].leafset)
//				setT.add(i);
//			for(int i = 0; i < (int) Math.ceil( Math.log(numns)/Math.log(beehive_DHTbase) ); i ++)
//				for(int j = 0; j < beehive_DHTbase; j ++)
//					if(routingtable[node].rtable[i][j] != -1)
//						setT.add(routingtable[node].rtable[i][j]);
//			for(int i = 0; i < Config.beehive_DHTneighborsetsize; i ++)
//				setT.add( routingtable[node].neighborset[i]);
//				
//			smallestDiff = nameserverId[node].bigInteger.subtract( nameId[name].bigInteger).abs();
//			for(int t:setT){
//				shl1 = 0;
//				nameprefix = nameId[name].hexformat.substring(0, shl1+1);
//				nodeprefix = nameserverId[t].hexformat.substring(0, shl1+1);
//				while( nameprefix.equals(nodeprefix)){
//					shl1 += 1;
//					nameprefix = nameId[name].hexformat.substring(0, shl1+1);
//					nodeprefix = nameserverId[t].hexformat.substring(0, shl1+1);
//				}
//				diff = nameserverId[t].bigInteger.subtract( nameId[name].bigInteger ).abs();
//				if(shl1 >= shl && diff.compareTo(smallestDiff) < 0){
//					if (Config.actives.get(name).contains(t))
//						return t;
//				}
//			}
		}
		
//		size = Config.actives.get(name).size();
		return actives.get(random.nextInt(actives.size()));
		
	}
	
//	private void writeNumActivesToFile(){
//		int name, numactives;
//		try{
//			FileWriter fw = new FileWriter(Config.numactivefile);
//			BufferedWriter bw = new BufferedWriter(fw);
//			
//			bw.write("#name\tnum_actives\n");
//			for (name = 0; name < numnames; name ++){
//				numactives = Config.actives.get(name).size();
//				bw.write(name + "\t" + numactives + "\n");
//			}
//			bw.close();
//			fw.close();			
//		}catch(Exception e){
//			GNRS.getLogger().fine("Error in writeNumActivesToFile " + e.getMessage());
//			System.exit(1);
//		}
//	}
	
}

class IDInformation{
	byte[] bytes;
	BigInteger bigInteger;
	String hexformat;
	
	IDInformation(byte[] bytes){
		this.bytes = bytes;
		bigInteger = new BigInteger(1, bytes);
		
		StringBuffer sb = new StringBuffer();
		for(int j = 0; j < bytes.length; j ++){
			String hex = Integer.toHexString( 0xff & bytes[j]);
			if (hex.length() == 1) sb.append('0');
			sb.append( hex );
		}
		hexformat = sb.toString();
	}
}

class RoutingTable{
	int[][] rtable;
	ArrayList<Integer> leafset;
//	int[] neighborset; // not implemeting neighbor set related optimizations now.
	
	RoutingTable(int numrow, int numcolumn, int leafsetsize){
		
		rtable = new int[numrow][numcolumn];
		
		for(int row = 0; row < numrow; row ++)
			for(int column = 0; column < numcolumn; column ++)
				rtable[row][column] = -1;
		
		leafset = new ArrayList<Integer>();
//		neighborset = new int[Config.beehive_DHTneighborsetsize];
	}
}
