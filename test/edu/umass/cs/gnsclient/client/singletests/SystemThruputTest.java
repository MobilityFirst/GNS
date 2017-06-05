package edu.umass.cs.gnsclient.client.singletests;

import java.io.IOException;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.json.JSONException;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Util;
import edu.umass.cs.utils.Utils;

/**
 * This test is used to ensure that the system performance 
 * (in terms of throughput, i.e., reqs/sec) of unsigned read 
 * and signed write won't be degraded in the future commits 
 * to the master branch.
 * If the performance of any of these two operations decreases
 * to lower than 80% of the threshold defined in this test,
 * this test will fail.
 * 
 * @author gaozy
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SystemThruputTest extends DefaultGNSTest {
	
	private static GNSClient[] clients = null;
	private static GuidEntry accountGuid;
	
	private static String testField;
	private static String testValue;
	
	private static ThreadPoolExecutor executor;
	
	private static final int numWrites = 10000;
	private static final int numReads = 100000;
	
	// TODO: not sure 10 is enough to saturate the servers on Travis CI, as there are 3 servers
	private final static int numClients = 10;
	
	private static int numFinishedOps = 0;
	private static long lastOpFinishedTime = System.currentTimeMillis();
	
	synchronized static void incrFinishedOps() {
		numFinishedOps++;
		lastOpFinishedTime = System.currentTimeMillis();
	}
	
	private void reset() {
		numFinishedOps = 0;
		lastOpFinishedTime = System.currentTimeMillis();
	}

	
	/**
	 * 
	 */
	public SystemThruputTest() {
		clients = new GNSClient[numClients];
		// initialize numClients clients
		for (int i=0; i<numClients; i++){
			try {
				clients[i] = new GNSClient();
			} catch (IOException e) {
				Utils.failWithStackTrace("Exception creating clients: ", e);
			}
		}
		
		try {
			accountGuid = GuidUtils.lookupOrCreateAccountGuid(clients[0], "ACCOUNT_GUID", "password");
		} catch (Exception e) {
			Utils.failWithStackTrace("Exception creating client: ", e);
		}
		
		// initialize thread pool executor
		executor = (ThreadPoolExecutor) Executors
				.newFixedThreadPool(50*numClients);
				
		SecureRandom random = new SecureRandom();
		// initialize random field and value: 10-byte string
		testField = new BigInteger(80, random).toString();
		testValue = new BigInteger(80, random).toString();
		
	}
	
	/**
	 * Update the {testField:testValue} to user json
	 */
	@Test
	public void test_01_CreateFieldAndValue() {
		try {
			clients[0].execute(GNSCommand.fieldUpdate(accountGuid, testField, testValue));
			assert(client.execute(GNSCommand.fieldRead(accountGuid, testField)).getResultJSONObject().getString(testField)
					.equals(testValue));
		} catch (ClientException | IOException | JSONException e) {
			Utils.failWithStackTrace("Exception updating a field: ", e);
		}
	}
	
	/**
	 * This warm-up round is used to warm up not just the server JVMs but also the client.
	 * Without this round, the performance may not stablize. 
	 */
	public void test_10_Warmup() {
		
	}
	
	
	/**
	 * TODO: threshold
	 */
	@Test
	public void test_11_ParallelWriteCapacity() {
		long t = System.currentTimeMillis();
		for (int i=0; i<numWrites; i++){
			blockingSignedWrite(i % numClients, accountGuid);
		}
		System.out.print("[total_writes=" + numWrites+": ");
		int lastCount = 0;
		while (numFinishedOps < numWrites) {
			if(numFinishedOps>lastCount)  {
				lastCount = numFinishedOps;
				System.out.print(numFinishedOps + "@" + Util.df(numFinishedOps * 1.0 / (lastOpFinishedTime - t))+"K/s ");
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				//swallow exception
			}
		}
		System.out.print("] ");
		System.out.print("parallel_write_rate="
				+ Util.df(numWrites * 1.0 / (lastOpFinishedTime - t))
				+ "K/s");
	}
	
	/**
	 * TODO: threshold
	 */
	@Test
	public void test_12_ParallelReadCapacity() {
		reset();
		long t = System.currentTimeMillis();
		for (int i = 0; i < numReads; i++) {
			blockingUnsignedRead(i % numClients, accountGuid);
		}
		int j = 1;
		System.out.print("[total_unsigned_reads=" + numReads+": ");
		while (numFinishedOps < numReads) {
			if (numFinishedOps >= j) {
				j *= 2;
				System.out.print(numFinishedOps + "@" + Util.df(numFinishedOps * 1.0 / (lastOpFinishedTime - t))+"K/s ");
			}
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// swallow exception
			}
		}
		System.out.print("] ");
		System.out.print("parallel_unsigned_read_rate="
				+ Util.df(numReads * 1.0 / (lastOpFinishedTime - t))
				+ "K/s");
	}
	
	
	private void blockingUnsignedRead(int clientIndex, GuidEntry guid) {
		executor.submit(new Runnable() {
			public void run() {
				try {					
					assert(clients[clientIndex].execute(GNSCommand.fieldRead(guid.getGuid(), 
								testField, null)).getResultJSONObject().getString(testField).equals(testValue));					
				} catch (IOException e) {
					Utils.failWithStackTrace("Exception reading a field: ", e);
				} catch (JSONException | ClientException e){
					// swallow the exception as it is very possible a timeout exception
				}
				incrFinishedOps();
			}
		});
	}
	
	private void blockingSignedWrite(int clientIndex, GuidEntry guid) {
		executor.submit(new Runnable() {
			@Override
			public void run() {
				try {
					clients[clientIndex].execute(GNSCommand.fieldUpdate(guid, testField, testValue));					
				} catch (IOException e) {
					Utils.failWithStackTrace("Exception updating a field: ", e);
				} catch (ClientException e) {
					// swallow the timeout exception
				}
				incrFinishedOps();
			}			
		});
	}
	
}
