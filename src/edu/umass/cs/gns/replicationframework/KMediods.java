package edu.umass.cs.gns.replicationframework;

import edu.umass.cs.gns.main.StartNameServer;
//import edu.umass.cs.gnrs.nameserver.NameRecord;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.nameserver.recordmap.ReplicaControllerRecord;
import edu.umass.cs.gns.util.ConfigFileInfo;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
/**************** FIXME Package deprecated by nsdesign/replicationFramework. this will soon be deleted. **/


/**
 * @deprecated
 */
public class KMediods  implements ReplicationFrameworkInterface {

	/**
	 * Returns set of active name servers using KMediods algorithm.
	 */
	@Override
	public Set<Integer> newActiveReplica(ReplicaControllerRecord record, int numReplica,
			int count) throws FieldNotFoundException{
        // TODO: implement record.getRequestCountsFromEachLocalNameServer for this scheme to work
		int k = numReplica;
		ArrayList<Integer> kmedoids = new ArrayList<Integer>();
		ArrayList<Integer> kmedoids_temp = new ArrayList<Integer>();
		
		//NS cluster, key: medoids, value:NS that is assigned to it
		HashMap<Integer, ArrayList<Integer>> NS_cluster = new HashMap<Integer, ArrayList <Integer>>();
		//LNS cluster, key:medoids, value: LNS that is assigned to it
		HashMap<Integer, ArrayList<Integer>> LNS_cluster = new HashMap<Integer, ArrayList<Integer>>();
		
		Iterator<Integer> iterator_NS, iterator_NS1, iterator_LNS;
		
		double latency, minlatency, latency_cluster, minlatency_cluster;
		int i, medoidNS, tempNS, medoidNS_new, lns;
		int local_nameservers = getNumberOfLocalNameServers();
		
//		int num_req[] = new int[local_nameservers];
		
		boolean kmedoidsChanged;
		
		//Initialization
		for(i = 0; i < ConfigFileInfo.getNumberOfNameServers(); i++)
			kmedoids.add(i);				
		Collections.shuffle(kmedoids);
		for(i = 0; i < ConfigFileInfo.getNumberOfNameServers() - k; i ++)
			kmedoids.remove(0);
		
//		Iterator<RequestStat> iterator_req;
//		for(i = 0; i < local_nameservers; i ++)
//			num_req[i] = 0;
//		iterator_req = NameStats.requestStats.get(name).iterator();
//		while(iterator_req.hasNext())
//			num_req[iterator_req.next().LNS] ++;

		// method not complete yet
        // TODO: implement record.getRequestCountsFromEachLocalNameServer for this scheme to work
		int [] num_req = {};//record.getRequestCountsFromEachLocalNameServer();
		
		while(true){
			iterator_NS = kmedoids.iterator();		
			//System.out.print("kmedoids : ");
			while(iterator_NS.hasNext()){
				medoidNS = iterator_NS.next();
				//System.out.print(medoidNS + " ");
				NS_cluster.put(medoidNS, new ArrayList<Integer>());
				LNS_cluster.put(medoidNS, new ArrayList<Integer>());
			}
			//Assign each NS to nearest medoid
			for( i = 0; i < ConfigFileInfo.getNumberOfNameServers(); i ++){
				if( kmedoids.contains(i) )
					continue;
				minlatency = Double.MAX_VALUE;
				medoidNS = -1;
				iterator_NS = kmedoids.iterator();
				while(iterator_NS.hasNext()){
					tempNS = iterator_NS.next();
					latency = getPingLatency_NS_NS( i, tempNS);
					if(latency < minlatency){
						minlatency = latency;
						medoidNS = tempNS;
					}
				}
				NS_cluster.get(medoidNS).add(i);	
			}
			//Assign each LNS to nearest medoid
			for( i = 0; i < local_nameservers; i ++){
				minlatency = Double.MAX_VALUE;
				medoidNS = -1;
				iterator_NS = kmedoids.iterator();
				while(iterator_NS.hasNext()){
					tempNS = iterator_NS.next();
					latency = getPingLatency_LNS_NS(i, tempNS) * num_req[i];
					if(latency < minlatency){
						minlatency = latency;
						medoidNS = tempNS;
					}
				}
				LNS_cluster.get(medoidNS).add(i);
			}
			latency = 0.0;
			iterator_NS = kmedoids.iterator();
			while(iterator_NS.hasNext()){
				medoidNS = iterator_NS.next();
				iterator_LNS = LNS_cluster.get(medoidNS).iterator();
				while(iterator_LNS.hasNext()){
					lns = iterator_LNS.next();
					latency += getPingLatency_LNS_NS( lns, medoidNS) * num_req[lns]; 
				}
			}
			//System.out.println("; total latency: " + latency);
			
			//UpdateTrace for each cluster
			iterator_NS = kmedoids.iterator();
			while(iterator_NS.hasNext()){
				medoidNS = iterator_NS.next();
				//System.out.print("For medoid " + medoidNS);
				medoidNS_new = medoidNS;
				minlatency_cluster = 0.0;
				iterator_LNS = LNS_cluster.get(medoidNS).iterator();
				while(iterator_LNS.hasNext()){
					lns = iterator_LNS.next();
					minlatency_cluster += getPingLatency_LNS_NS(lns, medoidNS) * num_req[lns];
				}
				//System.out.print(" clusterlatency " + minlatency_cluster + "; ");
				//Swap medoid
				iterator_NS1 = NS_cluster.get(medoidNS).iterator();
				while(iterator_NS1.hasNext()){
					tempNS = iterator_NS1.next();
					latency_cluster = 0.0;
					iterator_LNS = LNS_cluster.get(medoidNS).iterator();
					//System.out.print("other NS " + tempNS );
					while(iterator_LNS.hasNext()){
						lns = iterator_LNS.next();
						latency_cluster += getPingLatency_LNS_NS(lns, tempNS) * num_req[lns];
					}
					//System.out.print(" clusterlatency " + latency_cluster + "; ");
					if (latency_cluster < minlatency_cluster){
						medoidNS_new = tempNS;
						minlatency_cluster = latency_cluster;
					}
				}
				kmedoids_temp.add(medoidNS_new);
				//System.out.println();
			}
	
			iterator_NS = kmedoids.iterator();
			iterator_NS1 = kmedoids_temp.iterator();
			kmedoidsChanged = false;
			while(iterator_NS.hasNext()){
				if(iterator_NS.next() != iterator_NS1.next()){
					kmedoidsChanged = true;
					break;
				}	
			}
			if(kmedoidsChanged == true){
				kmedoids.clear();
				iterator_NS1 = kmedoids_temp.iterator();
				while(iterator_NS1.hasNext())
					kmedoids.add(iterator_NS1.next());
				
				kmedoids_temp.clear();
				NS_cluster.clear();
				LNS_cluster.clear();
			}
			else
				break;
		}
		iterator_NS = kmedoids.iterator();
		Set<Integer> activeNameServerSet = new HashSet<Integer>();
		//System.out.print("kmedoids: ");
		while(iterator_NS.hasNext()){
			medoidNS = iterator_NS.next();
			activeNameServerSet.add(medoidNS);
			// next line is simulators code xiaozheng's code
//			NameStats.actives.get(name).add(medoidNS);
			
			//System.out.print(medoidNS + " ");
		}
		//System.out.println();
		
		kmedoids_temp.clear();
		NS_cluster.clear();
		LNS_cluster.clear();
		return activeNameServerSet;
	}
	double [][] lnsnslatency = null;

	double [][] nsnslatency = null;

	/**

	* This method returns the ping latency between local name server lns and nameserver ns
	* @param lns local name server id
	* @param ns name server id
	* @return ping latency value
	*/

	private double getPingLatency_LNS_NS(int lns, int ns)
	{
        if (lnsnslatency == null)
        {
            lnsnslatency = readPingLatencyFile(StartNameServer.lnsnsPingFile, 
            StartNameServer.numberLNS, ConfigFileInfo.getNumberOfNameServers());
        }
        return lnsnslatency[lns][ns];
	}


	/**
	* This method returns the ping latency between  name server ns1 
	* and name server ns2
	* @param ns1 name server 1 id
	* @param ns2 name server 2 id
	* @return ping latency value
	*/

	private double getPingLatency_NS_NS(int ns1, int ns2)
	{
        if (nsnslatency == null)
        {
            nsnslatency = readPingLatencyFile(StartNameServer.nsnsPingFile, ConfigFileInfo.getNumberOfNameServers(), ConfigFileInfo.getNumberOfNameServers());
        }
        return nsnslatency[ns1][ns2];
	}

	private double[][] readPingLatencyFile(String fileName, int d1, int d2)
	{
        double[][] allLatencies = new double[d1][d2];
        try
        {
            BufferedReader br = new BufferedReader(new FileReader(fileName));
            int count = -1; 
            while(true)
            {
                count += 1;
                String line = br.readLine();
                if (line == null)
                    break;
                String[] tokens = line.split("\t");
                if (tokens.length < d2)
                {
                    continue;
                }
                for (int i = 0; i < tokens.length && i < d2 && count < d1; i++)
                {
                    allLatencies[count][i] = new Double(tokens[i]);
                }
            }
            br.close();
        } catch (NumberFormatException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return allLatencies;
	}
    
	/**
	 * Returns total number of local name servers in the experiment. 
	 * @return
	 */
	private int getNumberOfLocalNameServers()
	{
		// TODO write code here.
		return StartNameServer.numberLNS; // Xiaozheng
	}
	
	
}
