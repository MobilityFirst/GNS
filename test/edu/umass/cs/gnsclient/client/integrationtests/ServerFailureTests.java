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
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnscommon.utils.ThreadUtils;
import edu.umass.cs.nio.JSONDelayEmulator;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Brendan
 *
 * Runs a number of tests while shutting down a minority of GNSApp servers at random intervals.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ServerFailureTests {

  private static final String DEFAULT_ACCOUNT_ALIAS = "support@gns.name";
  private static final boolean EMULATING_DELAYS = false;

  private static String accountAlias = DEFAULT_ACCOUNT_ALIAS; // REPLACE																// ALIAS
  private static final String PASSWORD = "password";
  private static GNSClientCommands client = null;
  private static GuidEntry masterGuid;
  private static ServerFailureTests suite = new ServerFailureTests();
  private static double startClock;
  private static double endClock;
  private static int TEST_TIME; //Time in ms to run each phase of the testing.
  private static String gpPropDir;
  private static String execString;
  private static int numServers;

  private ArrayList<String> serverNameArray = new ArrayList<String>();
  private ArrayList<String> serverDownNameArray = new ArrayList<String>();
  private int numRequestsSuccessful = 0;
  private int roundNumber = 0;

  private final int EXPECTED_MIN_THROUGHPUT = 1000; //If throughput is below this when a majority of replicas are up, the test fails.

  /**
   *
   * @throws IOException
   * @throws InterruptedException
   */
  public static void causeRandomServerFailure() throws IOException, InterruptedException {
    //Get the GNS directory
    //String topDir = Paths.get(".").toAbsolutePath().normalize().toString();
    //System.out.println("Current path: " + topDir);
    Random rand = new Random();

    //Get a random server name
    String serverName;
    synchronized (suite) {
      int idx = rand.nextInt(suite.serverNameArray.size());
      serverName = suite.serverNameArray.remove(idx);
      suite.serverDownNameArray.add(serverName);
    }

    //Stop the server
    System.out.println("Stopping " + serverName);
    Process killServer = Runtime.getRuntime().exec(execString + "stop " + serverName);
    /*RunServer.command(
				System.getProperty(DefaultProps.SERVER_COMMAND.key)
				+ " " + getGigaPaxosOptions()
				+ " stop " + serverName, ".");*/
    killServer.waitFor(); //Throws a possible interruptedException here
    System.out.println(serverName + " stopping.");

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

  /**
   *
   * @throws IOException
   */
  public static void recoverAllServers() throws IOException {
    //Restart the server to simulate recovery.
    synchronized (suite) {
      ArrayList<String> toRestart = new ArrayList<String>();
      for (String serverName : suite.serverDownNameArray) {
        System.out.println("Restarting " + serverName);
        Process restartServer = Runtime.getRuntime().exec(execString + "start " + serverName);
        try {
          restartServer.waitFor();
        } catch (InterruptedException e) {
          // There's not much to be done if the process gets interrupted here
          e.printStackTrace();
        }

        System.out.println(serverName + " restarting.");
        toRestart.add(serverName);
      }
      synchronized (suite) {
        suite.serverDownNameArray.removeAll(toRestart);
        suite.serverNameArray.addAll(toRestart);
      }
    }
  }

  /**
   *
   * @throws Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    String topDir = Paths.get(".").toAbsolutePath().normalize().toString();

    //Set the running time (in seconds) for each round of throughput testing.
    String inputRoundTime = System.getProperty("failureTest.roundTime");
    if (inputRoundTime != null) {
      TEST_TIME = Integer.parseInt(inputRoundTime) * 1000;
    } else {
      TEST_TIME = 10000; //Default to 10,000 ms
    }

    //Parse gigapaxos.properties file for the active replica names
    String gpPropPath = System.getProperty("gigapaxosConfig");
    System.out.println("Using gigapaxos config: " + gpPropPath);
    gpPropDir = topDir + "/" + gpPropPath;
    File file = new File(gpPropDir);
    BufferedReader reader = new BufferedReader(new FileReader(file));
    String line = reader.readLine();
    while (line != null) {
      if (line.startsWith("active.")) {
        int idx = line.indexOf(".");
        int idx2 = line.indexOf("=", idx);
        String serverName = line.substring(idx + 1, idx2);
        suite.serverNameArray.add(serverName);
      }
      line = reader.readLine();
    }
    reader.close();
    numServers = suite.serverNameArray.size();
    System.out.println("Active Replicas to be used: " + suite.serverNameArray.toString());

    //Kill any running servers
    Process startProcess = Runtime.getRuntime().exec("kill -s TERM `ps -ef | grep GNS.jar | grep -v grep | grep -v ServerIntegrationTest  | grep -v \"context\" | awk '{print $2}'`");
    InputStream serverLauncherOutput = startProcess.getInputStream();
    BufferedReader output = new BufferedReader(new InputStreamReader(serverLauncherOutput));
    line = output.readLine();
    while (startProcess.isAlive() && (line != null)) {
      System.out.println(line);
      line = output.readLine();
    }
    startProcess.waitFor();

    //Kill any saved state
    execString = topDir + "/bin/gpServer.sh -DgigapaxosConfig=" + gpPropDir + " ";
    System.out.println("Running " + execString + "forceclear all");
    startProcess = Runtime.getRuntime().exec(execString + "forceclear all");
    serverLauncherOutput = startProcess.getInputStream();
    output = new BufferedReader(new InputStreamReader(serverLauncherOutput));
    line = output.readLine();
    while (startProcess.isAlive() && (line != null)) {
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
    while (startProcess.isAlive() && (line != null)) {
      System.out.println(line);
      line = output.readLine();
    }
    startProcess.waitFor();
    System.out.println("Servers started.");

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

    //Begin emulating transport delays -- This was causing timeouts on everything so it's unused out for now.
    if (EMULATING_DELAYS) {
      JSONDelayEmulator.emulateDelays();
    }
  }

  /**
   *
   * @throws Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    System.out.println("Stopping and clearing all replicas.");
    Process stop = Runtime.getRuntime().exec(execString + "stop all");
    stop.waitFor();
    Runtime.getRuntime().exec(execString + "forceclear all");
    if (client != null) {
      client.close();
    }
  }

  /**
   *
   */
  @Rule
  public TestName testName = new TestName();

  /**
   *
   */
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

  /**
   * Do this before.
   */
  @Before
  public void beforeMethod() {
    System.out.print(testName.getMethodName() + " ");
  }

  /**
   * Do this after.
   */
  @After
  public void afterMethod() {
  }

  /**
   *
   * @throws Exception
   */
  @Test
  public void test_010_Throughput() throws Exception {
    String threadString = System.getProperty("failureTest.threads");
    int numThreads = 20;
    if (threadString != null) {
      numThreads = Integer.parseInt(threadString);
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
    for (int i = 0; i < numThreads; i++) {
      threads[i] = new Thread() {
        public void run() {
          try {
            //Reads the guidEntry continuously until the thread is interrupted.
            while (true) {
              if (Thread.interrupted()) {
                return;
              }
              try {
                final int roundNumber = suite.roundNumber;
                assertEquals(masterGuid.getGuid(), client.lookupPrimaryGuid(testGuid.getGuid()));
                //We spawn a new thread that will sequentially increment the number of requests that have been successful so we don't have to block this thread.
                Thread increment = new Thread() {
                  public void run() {
                    synchronized (suite) {
                      if (suite.roundNumber == roundNumber) { //If the testing has moved on to a new failure phase, then don't count this request since it was started before.
                        suite.numRequestsSuccessful++;
                      }
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
    int NUM_FAILURES = (numServers / 2) + 1; //Cause a majority of servers to fail.  So if there are 5 servers it will fail 0, 1, 2, 3 servers and expect throughput to hit 0 on the 4th round of testing.
    endClock = System.currentTimeMillis();
    double throughput = 0;
    for (int i = 0; i <= NUM_FAILURES; i++) {
      synchronized (suite) {
        suite.roundNumber = i; //This is used so we don't end up counting throughput from requests of the previous phase.
      }
      //Bring all servers back up
      recoverAllServers();
      //Start with 0 failures, then 1, then 2, etc.
      System.out.println("Causing " + i + " server failures.");
      for (int j = 0; j < i; j++) {
        causeRandomServerFailure();
      }
      synchronized (suite) {
        startClock = System.currentTimeMillis();
        suite.numRequestsSuccessful = 0;
      }
      while (endClock - startClock < TEST_TIME) {
        Thread.sleep(1000);
        endClock = System.currentTimeMillis();
        requestCount = suite.numRequestsSuccessful;
        throughput = 1000 * requestCount / (endClock - startClock);
        System.out.println("Average read throughput: " + Double.toString(throughput));
      }
      if (i < NUM_FAILURES) {
        if (throughput < EXPECTED_MIN_THROUGHPUT) {
          fail("The measured throughput of " + Double.toString(throughput) + " requests/second was below the expected minimum throughput of " + Integer.toString(EXPECTED_MIN_THROUGHPUT) + " requests/second.");
        }
      } else //This checks that the throughput was 0 on the last round, which is when a majority of replicas were down.
      if (throughput > 0) {
        fail("When a majority of " + Integer.toString(NUM_FAILURES) + " out of " + Integer.toString(numServers) + " active replicas were down some requests were still handled!");
      }
    }

    //Stop all the threads.
    for (int i = 0; i < numThreads; i++) {
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
