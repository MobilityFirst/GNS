/**
 * 
 */
package edu.umass.cs.gnsclient.client.integrationtests;


import static org.junit.Assert.fail;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Brendan
 * 
 * Runs each test in ServerIntegrationTest a number of times sequentially, then in parallel.
 *
 */
public class ServerIntegrationTestThreaded {
	
	
	
	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}
	


	/**
	 * Runs each of the ServerIntegrationTest tests numThreads times in parallel for numRuns time sequentially.
	 * Only uses one ServerIntegrationTest object and client. amongst all threads, and excludes tests that cannot be run in parallel.
   * @throws java.lang.Exception
	 */
	@Test
	public void test_01_ParallelServerIntegrationTest() throws Exception{
		boolean failedAnyTest = false;
		String runsString = System.getProperty("integrationTest.runs");
		//This style of definition is used to make numRuns effectively final so it can be used in threads.
		int numRuns;
		 if (runsString != null){
			 numRuns=Integer.parseInt(runsString);
		 }
		 else{
			 numRuns = 10;
		 }
		 String threadString = System.getProperty("integrationTest.threads");
			int numThreads = 10;
			 if (threadString != null){
				 numThreads=Integer.parseInt(threadString);
			 }
			 
		String exitOnFailureString = System.getProperty("integrationTest.exitOnFailure");
				final boolean exitOnFailure;
				if (exitOnFailureString != null){
					 exitOnFailure = Boolean.parseBoolean(exitOnFailureString);
				}
				else{
					exitOnFailure = true;
				}
		
		
		ServerIntegrationTest.setUpBeforeClass();
		ServerIntegrationTest siTest = new ServerIntegrationTest();
		Class<?> siClass = siTest.getClass();
		Method[] allMethods = siClass.getDeclaredMethods();
		ExecutorService pool = Executors.newFixedThreadPool(numThreads);
		for (Method method : allMethods){
			boolean failedThisTest = false;
			Test testAnnotation = method.getAnnotation(Test.class);
			//Exclude non test methods by skipping over them.
			if (testAnnotation == null){
				continue;
			}

			
			//Run the tests in parallel and record any exceptions thrown.
			List<Future<Exception>> failures = new ArrayList<Future<Exception>>();
			System.out.println("Running test: " + method.getName() + " in " + numThreads + " threads with " + numRuns + " runs per thread.");
			for (int i = 0; i < numThreads; i++){
				Callable<Exception >call = new Callable<Exception>(){ 
					public Exception call(){
						try {
							//Runs the current test numRuns times in this thread.
							for (int j = 0; j < numRuns; j++){
								method.invoke(siTest);
							}
							//Return no exception since none was thrown.
							return null;
						} catch (Exception e) {
							//Return the thrown exception.
							return e;
						}
					}
				};
				//Add this test to our working queue.
				Future<Exception> future = pool.submit(call);
				failures.add(future);
			}
			//Wait for all threads to finish before moving on to the next test.
			for (int i = 0; i < failures.size(); i++){
				Exception e = failures.get(i).get();
				if (e != null){
					failedThisTest = true;
					StringWriter printException = new StringWriter();
					e.printStackTrace(new PrintWriter(printException));
					System.err.println(method.getName() + " FAILED: Testing thread " + Integer.toString(i) 
														+ " threw an exception:\n" + printException.toString());
				}
			}
			if (!failedThisTest){
				System.out.println("Test " + method.getName() + " passed.");
			}
			else{
				failedAnyTest = true;
				if (exitOnFailure){
					fail("Aborting further testing due to failure! Set -DintegrationTest.exitOnFailure=false to disable.");
				}
			}
		}
		ServerIntegrationTest.tearDownAfterClass();
		if (failedAnyTest){
			System.exit(1);
		}
	}
	

}
