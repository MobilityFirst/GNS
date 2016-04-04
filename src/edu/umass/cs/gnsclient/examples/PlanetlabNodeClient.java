package edu.umass.cs.gnsclient.examples;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.umass.cs.gigapaxos.paxosutil.RateLimiter;
import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeGuidEntry;

/**
 * @author gaozy
 *
 */
public class PlanetlabNodeClient {
	private static GNSClient client;
	private static GuidEntry guidEntry;
	
	// constants
	private final static String pearAddress = "128.119.245.20";	
	private final static int BLOCK_SIZE = 10000;
	
	// values to be initialized at the beginning
	private static String PLANETLAB_NODE_NAME;
	private static String FOLDER;
	private static String POLICY;
	
    private static int QUERY_EVERY_FEW_REQUEST = 50;
        
    // trace to be loaded from a trace file
    private static ArrayList<Integer> trace;
    
    private static String cachedServer = "";
    private static int TOTAL = 0;
    
    private static Random rand = new Random();
    
    // Thread pool
 	private static ExecutorService executor;
    private final static int NUM_THREAD = 20;
    
    private static int req_id = 0;
    private synchronized static int getRequestID(){
    	return req_id++;
    }
    
    private static ArrayList<String> latency = new ArrayList<String>();
	private synchronized static void updateLatency(long lat, int id){
		latency.add(id+" "+lat);
	}
    
    private static void startSending(){		
		/*
		 * Send request in every second
		 */
		long t1 = System.currentTimeMillis();
		for(int rate:trace){
			long t = System.currentTimeMillis();
			sendAtRate(rate, rate);
			TOTAL = TOTAL + rate;
			long elapsed = System.currentTimeMillis() - t;
			if(elapsed < 1000){
				try{
					Thread.sleep(1000-elapsed);
				}catch(Exception e){
					e.printStackTrace();
				}
			}
		}
		long t2 = System.currentTimeMillis();
		long elapsed = t2 - t1;
		System.out.println("It takes "+elapsed+" ms to send these messages.");
		
		while(latency.size() != TOTAL){
    		
    		System.out.println("Received "+latency.size()+" messages");
    		try{
    			Thread.sleep(1000);
    		}catch(Exception e){
    			e.printStackTrace();
    		}
    		
    	}
		
		System.out.println("All messages have been received");
		try{
			sendFinish();
		}catch(IOException e){
			e.printStackTrace();
		}
	}
    
    private static void sendFinish() throws IOException{
		Socket sock = new Socket("none.cs.umass.edu", 60001);
		OutputStream oStream = sock.getOutputStream();
		PrintWriter out = new PrintWriter(oStream);

		out.print(PLANETLAB_NODE_NAME);
		out.flush();
		sock.close();
	}
    
	protected static void sendSingleRequest(){
		int id = getRequestID();
		if(id%QUERY_EVERY_FEW_REQUEST == 0){
			updateDNSRecord();
		}
		executor.execute(new httpRequestThread(id, cachedServer));	
	}
	
	private static void updateDNSRecord(){		
		try {
			cachedServer = client.fieldRead(guidEntry, PLANETLAB_NODE_NAME);
			// Server can not be an empty value
			assert(!cachedServer.equals(""));
			System.out.println("cachedServer is "+cachedServer);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void sendAtRate(int rate, int numRequest){
		System.out.println("Start sending " + rate + " requess ar the rate of "+rate+" reqs/sec...");
    	RateLimiter r = new RateLimiter(rate);
    	for (int i=0; i<numRequest; i++){
    			sendSingleRequest();
    			r.record();
    	}
	}
	
	protected static void dumpLatency(){
		try{
			File file = new File(FOLDER+"latency-"+POLICY);
			FileOutputStream fop = new FileOutputStream(file);
			
			if (!file.exists()) {
				file.createNewFile();
			}
			for(String lat:latency){
				byte[] contentInBytes = (lat+"\n").getBytes();
				fop.write(contentInBytes);
			}
			fop.flush();
			fop.close();
		}catch(IOException e){
			e.printStackTrace();
		}
		
		latency = new ArrayList<String>();
		TOTAL = 0;
	}
	
	private static ArrayList<Integer> loadTrace(){
		ArrayList<Integer> trace = new ArrayList<Integer>();
		try{
			File fin = new File(FOLDER+"trace");
			BufferedReader br = new BufferedReader(new FileReader(fin));
			String line = null;
			while ((line = br.readLine()) != null) {
				trace.add(Integer.parseInt(line));
			}
		 
			br.close();
		}catch(IOException e){
			e.printStackTrace();
		}
		
		return trace;
	}
	
	
	static class httpRequestThread implements Runnable{
		
		private static final String USER_AGENT = "Mozilla/5.0";
		private final int msg_id;
		private final String server;
		
		protected httpRequestThread(int msg_id, String cachedServer){
			this.msg_id = msg_id;
			this.server = cachedServer;
		}

		@Override
		public void run() {
			long t1 = System.currentTimeMillis();
			sendHttpRequest();
			long elapsed = System.currentTimeMillis() - t1;
			updateLatency(elapsed, msg_id);
		}
		
		private void sendHttpRequest(){
			try{
				int page = rand.nextInt();
				String url = "http://"+server+"/"+((page<0?-page:page)%2150)+".html";
				URL obj = new URL(url);
				
				
				HttpURLConnection con = (HttpURLConnection) obj.openConnection();
				con.setRequestMethod("GET");
				con.setRequestProperty("User-Agent", USER_AGENT);
				InputStream in = con.getInputStream();
				
				StringBuilder builder = new StringBuilder();
				byte[] b = new byte[BLOCK_SIZE];
				while (in.read(b, 0, BLOCK_SIZE) != -1) {
					builder.append(b);
				}
				System.out.println(builder.toString());
				in.close();
				
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}
	
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception{
		// edu.umass.cs.gnsclient.examples.PlanetlabNodeClient folder 
		PLANETLAB_NODE_NAME = args[0];
		
		FOLDER = args[1];
		if(! FOLDER.contains("/")){
			FOLDER = FOLDER+"/";
		}		
		POLICY = args[2];
		
		if(args.length > 4){
			QUERY_EVERY_FEW_REQUEST = Integer.parseInt(args[3]);
		}
		
		FileInputStream fis = new FileInputStream(FOLDER+SequentialRequestClient.getGuidFilename());
		@SuppressWarnings("resource")
		ObjectInputStream ois = new ObjectInputStream(fis);
		ActiveCodeGuidEntry activeEntry = (ActiveCodeGuidEntry) ois.readObject();
		guidEntry = new GuidEntry(activeEntry.entityName, activeEntry.guid, activeEntry.publicKey, activeEntry.privateKey);
		
		client = new GNSClient((InetSocketAddress) null, new InetSocketAddress(pearAddress, GNSClientConfig.LNS_PORT), true);
		
		executor = Executors.newFixedThreadPool(NUM_THREAD);
		
		// Load the trace to send
		trace = loadTrace();
		
		// Start sending all the requests
		startSending();
		
		// Dump latency result
		dumpLatency();
		
		System.exit(0);
	}
}
