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

	public static void causeRandomServerFailure() throws IOException, InterruptedException{
		//Get the GNS directory
		String topDir = Paths.get(".").toAbsolutePath().normalize().toString();
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
		killServer.waitFor(); //Throws a possible interruptedException here
		System.out.println(serverName +" stopped.");
		
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
		
		//Start all the servers
		execString = topDir + "/bin/gpServer.sh -DgigapaxosConfig="+gpPropDir+" ";
		System.out.println("Running " + execString + "start all");
		Process startProcess = Runtime.getRuntime().exec(execString + "start all");
		InputStream serverLauncherOutput = startProcess.getInputStream();
		BufferedReader output = new BufferedReader(new InputStreamReader(serverLauncherOutput));
		line = output.readLine();
		while (startProcess.isAlive() && (line!=null)){
			System.out.println(line);
			line = output.readLine();
		}
		startProcess.waitFor();
		System.out.println("Servers started.");
		
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
		JSONDelayEmulator.emulateDelays();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		if (client != null) {
			client.close();
		}
		String topDir = Paths.get(".").toAbsolutePath().normalize().toString();
		System.out.println("Stopping all servers...");
		Process stopProcess = Runtime.getRuntime().exec(execString+"stop all");
		stopProcess.waitFor();
		System.out.println("Servers stopped.");
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
	public void test_010_CreateEntity() throws Exception {
		String threadString = System.getProperty("integrationTest.threads");
		int numThreads = 20;
		if (threadString != null){
			numThreads=Integer.parseInt(threadString);
		}
		
		String testGuidName = "testGUID" + RandomString.randomString(6);
	    final GuidEntry testGuid;
	    try {
	      testGuid = client.guidCreate(masterGuid, testGuidName);
	    } catch (Exception e) {
	      System.out.println("Exception while creating testGuid: " + e);
	      throw e;
	    }
	    startClock = System.currentTimeMillis();
	    numRequestsSuccessful = 0;
	    Thread threads[] = new Thread[numThreads];
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
			threads[i].run();
		}
	    //Wait TEST_TIME ms before ending the test phase.
	    int requestCount = 0;
	    while (endClock - startClock < TEST_TIME){
	    	Thread.sleep(1000);
	    	endClock = System.currentTimeMillis();
	    	requestCount = suite.numRequestsSuccessful;
	    	System.out.println("Average read throughput: " + Double.toString(requestCount / (endClock - startClock)));
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
