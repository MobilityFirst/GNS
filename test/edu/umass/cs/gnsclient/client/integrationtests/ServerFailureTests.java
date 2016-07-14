package edu.umass.cs.gnsclient.client.integrationtests;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Random;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runners.MethodSorters;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnscommon.utils.ThreadUtils;
import edu.umass.cs.nio.JSONDelayEmulator;
import edu.umass.cs.utils.Util;

/**
 * @author Brendan
 * 
 * Runs a number of tests while shutting down a minority of GNSApp servers at random intervals.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ServerFailureTests {

	private static final String DEFAULT_ACCOUNT_ALIAS = "support@gns.name";

	private static String accountAlias = DEFAULT_ACCOUNT_ALIAS; // REPLACE																// ALIAS
	private static final String PASSWORD = "password";
	private static GNSClientCommands client = null;
	private static GuidEntry masterGuid;
	private static ServerFailureTests suite = new ServerFailureTests();
	private static double startClock;
	private static double endClock;
	private static int TEST_TIME = 10000; //Time in ms to run each phase of the testing.
	private static String gpPropDir;
	private static String execString;
	
	private ArrayList<String> serverNameArray = new ArrayList<String>();
	private ArrayList<String> serverDownNameArray = new ArrayList<String>();
	private int numRequestsSuccessful = 0;
	
	  private static final String HOME=System.getProperty("user.home");
	  private static final String GNS_DIR = "GNS";
	  private static final String GNS_HOME = HOME + "/" + GNS_DIR+"/";
	  
		private static final String SCRIPTS_SERVER="scripts/3nodeslocal/reset_and_restart.sh";
		private static final String SCRIPTS_OPTIONS=" 2>/tmp/log";
		
		private static final String GP_SERVER = "bin/gpServer.sh";
		private static final String GP_OPTIONS = getGigaPaxosOptions()
				+ " restart all";
		private static final boolean USE_GP_SCRIPTS = true;
		private static final String OPTIONS = USE_GP_SCRIPTS ? GP_OPTIONS : SCRIPTS_OPTIONS;
	
	private static final void setProperties() {
		for(DefaultProps prop : DefaultProps.values()) 
			if(System.getProperty(prop.key)==null)
				System.setProperty(prop.key, prop.isFile ? getPath(prop.value) : prop.value);
	}
	// this static block must be above GP_OPTIONS
	static {
		setProperties();
	}
	
	private static final String getPath(String filename) {
		if (new File(filename).exists())
			return filename;
		if (new File(GNS_HOME + filename).exists())
			return GNS_HOME + filename;
		else
			Util.suicide("Can not find server startup script: " + filename);
		return null;
	}
	
	
	private static enum DefaultProps {
		SERVER_COMMAND("server.command", USE_GP_SCRIPTS ? GP_SERVER : SCRIPTS_SERVER, true),
		GIGAPAXOS_CONFIG("gigapaxosConfig", "conf/gnsserver.3local.properties", true),
		KEYSTORE("javax.net.ssl.keyStore","conf/trustStore/node100.jks", true),
		KEYSTORE_PASSWORD("javax.net.ssl.keyStorePassword","qwerty"),
		TRUSTSTORE("javax.net.ssl.trustStore","conf/trustStore/node100.jks", true),
		TRUSTSTORE_PASSWORD("javax.net.ssl.trustStorePassword","qwerty"),
		START_SERVER("startServer", "true"),
		;

		final String key; 
		final String value;
		final boolean isFile;

		DefaultProps(String key, String value, boolean isFile) {
			this.key = key;
			this.value = value;
			this.isFile = isFile;
		}
		DefaultProps(String key, String value) {
			this(key, value, false);
		}
	}

	  private static final String getGigaPaxosOptions() {
		  String gpOptions="";
		  for(DefaultProps prop : DefaultProps.values()) 
			  gpOptions += " -D" + prop.key + "=" + System.getProperty(prop.key);
		  return gpOptions;
	  }
	
	public static void causeRandomServerFailure() throws IOException, InterruptedException{
		//Get the GNS directory
		//String topDir = Paths.get(".").toAbsolutePath().normalize().toString();
		//System.out.println("Current path: " + topDir);
		Random rand = new Random();
		
		//Get a random server name
		String serverName;
		synchronized(suite){
			int idx = rand.nextInt(suite.serverNameArray.size());
			serverName = suite.serverNameArray.remove(idx);
			suite.serverDownNameArray.add(serverName);
		}

		//Stop the server
		System.out.println("Stopping " +serverName);
		Process killServer = Runtime.getRuntime().exec(execString+ "stop " + serverName);
		/*RunServer.command(
				System.getProperty(DefaultProps.SERVER_COMMAND.key)
				+ " " + getGigaPaxosOptions()
				+ " stop " + serverName, ".");*/
		killServer.waitFor(); //Throws a possible interruptedException here
		System.out.println(serverName +" stopping.");
		
		/*
		//Wait 10 seconds
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			//If we get interrupted just bring the server up early and print out that the interruption occurred.
			e.printStackTrace();
		}
		
		//Restart the server to simulate recovery.
		System.out.println("Restarting " + serverName);
		Process restartServer = Runtime.getRuntime().exec(execString + "start " + serverName);
		restartServer.waitFor(); //Throws a possible interruptedException here
		System.out.println(serverName +" restarted.");
		synchronized(suite){
			suite.serverDownNameArray.remove(serverName);
			suite.serverNameArray.add(serverName);
		}
		*/
		
	}
	
	public static void recoverAllServers() throws IOException{
		//Restart the server to simulate recovery.
		synchronized(suite){
			ArrayList<String> toRestart = new ArrayList<String>();
			for (String serverName : suite.serverDownNameArray){
				System.out.println("Restarting " + serverName);
				Process restartServer = Runtime.getRuntime().exec(execString + "start " + serverName);
				try {
					restartServer.waitFor();
				} catch (InterruptedException e) {
					// There's not much to be done if the process gets interrupted here
					e.printStackTrace();
				}
				/*RunServer.command(
						System.getProperty(DefaultProps.SERVER_COMMAND.key)
						+ " " + getGigaPaxosOptions()
						+ " restart " + serverName, ".");*/
				
				System.out.println(serverName +" restarting.");
				toRestart.add(serverName);
			}
			synchronized(suite){
				suite.serverDownNameArray.removeAll(toRestart);
				suite.serverNameArray.addAll(toRestart);
			}
		}
	}
	
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		String topDir = Paths.get(".").toAbsolutePath().normalize().toString();

		//Parse gigapaxos.properties file for the active replica names
		String gpPropPath = System.getProperty("gigapaxosConfig");
		System.out.println("Using gigapaxos config: " + gpPropPath);
		gpPropDir = topDir + "/" + gpPropPath;
		File file = new File(gpPropDir);
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line = reader.readLine();
		while (line != null){
			if (line.startsWith("active.")){
				int idx = line.indexOf(".");
				int idx2 = line.indexOf("=",idx);
				String serverName = line.substring(idx+1, idx2);
				suite.serverNameArray.add(serverName);
			}
			line = reader.readLine();
		}
		reader.close();
		System.out.println("Active Replicas to be used: " + suite.serverNameArray.toString());
		
		//Kill any running servers
		Process startProcess  = Runtime.getRuntime().exec("kill -s TERM `ps -ef | grep GNS.jar | grep -v grep | grep -v ServerIntegrationTest  | grep -v \"context\" | awk '{print $2}'`");
		InputStream serverLauncherOutput = startProcess.getInputStream();
		BufferedReader output = new BufferedReader(new InputStreamReader(serverLauncherOutput));
		line = output.readLine();
		while (startProcess.isAlive() && (line!=null)){
			System.out.println(line);
			line = output.readLine();
		}
		startProcess.waitFor();
		
		//Kill any saved state
		execString = topDir + "/bin/gpServer.sh -DgigapaxosConfig="+gpPropDir+" ";
		System.out.println("Running " + execString + "forceclear all");
		startProcess = Runtime.getRuntime().exec(execString + "forceclear all");
		serverLauncherOutput = startProcess.getInputStream();
		output = new BufferedReader(new InputStreamReader(serverLauncherOutput));
		line = output.readLine();
		while (startProcess.isAlive() && (line!=null)){
			System.out.println(line);
			line = output.readLine();
		}
		startProcess.waitFor();
		


		//Start the servers
		System.out.println("Running " + execString + "restart all");
		startProcess = Runtime.getRuntime().exec(execString + "restart all");
		serverLauncherOutput = startProcess.getInputStream();
		output = new BufferedReader(new InputStreamReader(serverLauncherOutput));
		line = output.readLine();
		while (startProcess.isAlive() && (line!=null)){
			System.out.println(line);
			line = output.readLine();
		}
		startProcess.waitFor();
		System.out.println("Servers started.");
		
		/*RunServer
		.command(
				"kill -s TERM `ps -ef | grep GNS.jar | grep -v grep | grep -v ServerIntegrationTest  | grep -v \"context\" | awk '{print $2}'`",
				".");
		System.out.println(System.getProperty(DefaultProps.SERVER_COMMAND.key)
				+ " " + getGigaPaxosOptions()
				+ " forceclear all");

		RunServer.command(
				System.getProperty(DefaultProps.SERVER_COMMAND.key)
				+ " " + getGigaPaxosOptions()
				+ " forceclear all", ".");

		System.out.println(System.getProperty(DefaultProps.SERVER_COMMAND.key) + " " + OPTIONS);
		ArrayList<String> output = RunServer.command(
				System.getProperty(DefaultProps.SERVER_COMMAND.key) + " " + OPTIONS, ".");
		if (output != null) {
			for (String nextLine : output) {
				System.out.println(nextLine);
			}
		} else {
			fail("Server command failure: ; aborting all tests.");
		}
		*/
		Thread.sleep(5000);
		 System.out.println("Starting client");
		    
		    client = new GNSClientCommands();
		    // Make all the reads be coordinated
		    client.setForceCoordinatedReads(true);
		    // arun: connectivity check embedded in GNSClient constructor
		    boolean connected = client instanceof GNSClient;
		    if (connected) {
		      System.out.println("Client created and connected to server.");
		    }
		    //
		    int tries = 5;
		    boolean accountCreated = false;

		    do {
		      try {
		        System.out.println("Creating account guid: " + (tries - 1)
		                + " attempt remaining.");
		        masterGuid = GuidUtils.lookupOrCreateAccountGuid(client,
		                accountAlias, PASSWORD, true);
		        accountCreated = true;
		      } catch (Exception e) {
		        ThreadUtils.sleep((5 - tries) * 5000);
		      }
		    } while (!accountCreated && --tries > 0);
		    if (accountCreated == false) {
		      fail("Failure setting up account guid; aborting all tests.");
		    }
		
		//Begin emulating transport delays
		//JSONDelayEmulator.emulateDelays();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		/*
		ArrayList<String> output = RunServer.command(
				new File(System
						.getProperty(DefaultProps.SERVER_COMMAND.key))
				.getParent()
				+ "/shutdown.sh", ".");
		if (output != null) {
			for (String line : output) {
				System.out.println(line);
			}
		} else {
			System.out.println("SHUTDOWN SERVER COMMAND FAILED!");
		}*/
		
		RunServer.command(
				System.getProperty(DefaultProps.SERVER_COMMAND.key)
				+ " " + getGigaPaxosOptions()
				+ " stop all", ".");
		if (client != null) {
			client.close();
		}
	}
	@Rule
	public TestName testName = new TestName();

	@Rule
	public TestWatcher ruleExample = new TestWatcher() {
		@Override
		protected void failed(Throwable e, Description description) {
			System.out.println(" FAILED!!!!!!!!!!!!! " + e);
			e.printStackTrace();
			System.exit(1);
		}

		@Override
		protected void succeeded(Description description) {
			System.out.println(" succeeded");
		}
	};

	@Before
	public void beforeMethod() {
		System.out.print(testName.getMethodName() + " ");
	}

	@After
	public void afterMethod() {
	}

	@Test
	public void test_010_Throughput() throws Exception {
		String threadString = System.getProperty("integrationTest.threads");
		int numThreads = 20;
		if (threadString != null){
			numThreads=Integer.parseInt(threadString);
		}
		
		System.out.println("Creating guid for the test...");
		String testGuidName = "testGUID" + RandomString.randomString(6);
	    final GuidEntry testGuid;
	    try {
	      testGuid = client.guidCreate(masterGuid, testGuidName);
	      System.out.println("Guid created.");
	    } catch (Exception e) {
	      System.out.println("Exception while creating testGuid: " + e);
	      throw e;
	    }
	    startClock = System.currentTimeMillis();
	    numRequestsSuccessful = 0;
	    Thread threads[] = new Thread[numThreads];
	    System.out.println("Spawning client threads...");
	    for (int i = 0; i < numThreads; i++){
			threads[i] = new Thread(){ 
				public void run(){
					try {
						//Reads the guidEntry continuously until the thread is interrupted.
						while(!Thread.interrupted()){
						    try {
							      assertEquals(masterGuid.getGuid(), client.lookupPrimaryGuid(testGuid.getGuid()));
							      //We spawn a new thread that will sequentially increment the number of requests that have been successful so we don't have to block this thread.
							      Thread increment = new Thread(){
							    	  public void run(){
							    		  synchronized(suite){
							    			  suite.numRequestsSuccessful++;
							    		  }
							    	  }
							      };
							      increment.run();
							    } catch (IOException | ClientException e) {
							      //Ignore failures to read, they just won't increment the successful request count.
							    }
						}
					} catch (Exception e) {
						//Since this is threaded we need to handle any exceptions.  In this case by failing the test.
						StringWriter printException = new StringWriter();
						e.printStackTrace(new PrintWriter(printException));
						fail("A testing thread threw an exception during the test:\n" + printException.toString());
					}
				}
			};
			threads[i].start();
		}
	    System.out.println("Client threads all spawned.");
	    //Wait TEST_TIME ms before ending the test phase.
	    int requestCount = 0;
	    int NUM_FAILURES=2;
	    endClock=System.currentTimeMillis();
	    for (int i = 0; i <= NUM_FAILURES; i++){
	    	//Bring all servers back up
	    	recoverAllServers();
	    	//Start with 0 failures, then 1, then 2, etc.
    		System.out.println("Causing " + i + " server failures.");
	    	for (int j = 0; j < i; j++){
	    		causeRandomServerFailure();
	    	}
	    	synchronized(suite){
		    	startClock = System.currentTimeMillis();
	    		suite.numRequestsSuccessful = 0;
	    	}
	    	while (endClock - startClock < TEST_TIME){
	    		Thread.sleep(1000);
	    		endClock = System.currentTimeMillis();
	    		requestCount = suite.numRequestsSuccessful;
	    		System.out.println("Average read throughput: " + Double.toString(requestCount / (endClock - startClock)));
	    	}

	    }
	    //Stop all the threads.
	    for (int i = 0; i < numThreads; i++){
	    	threads[i].interrupt();
	    }
	    
		/*for (int i = 0; i < numRuns; i++){
			if (i % 1000 == 0){
				System.out.println("Running test for the " + Integer.toString(i) + "th time.");
				endClock = System.currentTimeMillis();
				double deltaSeconds = (endClock - startClock) / 1000;
				System.out.println("Throughput: " + Double.toString(numRequestsSuccessful / deltaSeconds) + " requests/second.");
				startClock = System.currentTimeMillis();
				numRequestsSuccessful = 0;
			}
		    try {
			      assertEquals(masterGuid.getGuid(), client.lookupPrimaryGuid(testGuid.getGuid()));
			      numRequestsSuccessful++;
			    } catch (IOException | ClientException e) {
			      //System.out.println("Exception while looking up primary guid on " + Integer.toString(i)+ "th run : " + e);
			      //throw e;
			    	//Ignore failures to read, they just won't increment the successful request count.
			    }*/
	}





}
