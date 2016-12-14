/**
 * 
 */
package edu.umass.cs.gnsclient.client.integrationtests;


import static org.junit.Assert.fail;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.TreeMap;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * @author Brendan
 * 
 * Runs each test in ServerIntegrationTest a number of times sequentially, then in parallel.
 *
 */
//@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ServerIntegrationTestThreaded {
	
	
	/**
	 * This variable and the two methods to set it failTest() and resetTestFailure()
	 * are used to allow threads to signal a test failure without having to stop running
	 * JUnit tests.
	 */
	private  boolean failedTest = false;
	private void failTest(){
		synchronized(this){
			failedTest = true;
		}
	}
	
	private void resetTestFailure(){
		synchronized(this){
			failedTest = false;
		}
	}
	
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
		
		
		ServerIntegrationTest.setUpBeforeClass();
		ServerIntegrationTest siTest = new ServerIntegrationTest();
		Class<?> siClass = siTest.getClass();
		Method[] allMethods = siClass.getDeclaredMethods();
		TreeMap<String, Method> methodTree = new TreeMap<String, Method>();
		TreeMap<String, Method> nonparallelMethodTree = new TreeMap<String, Method>();
		TreeMap<String, Method> dontRepeatMethodTree = new TreeMap<String, Method>();
		for (Method method : allMethods){
			String methodName = method.getName();
			Test testAnnotation = method.getAnnotation(Test.class);
			//Exclude non test methods by not putting them in our list.
			if (testAnnotation == null){
				continue;
			}
			methodTree.put(methodName,method);
			
		}
		for (Method method : methodTree.values()){	
			//Create numThreads threads.
			Thread threads[] = new Thread[numThreads];

			
			//These tests will be run in parallel.
			resetTestFailure();
			System.out.println("Running test: " + method.getName() + " in " + numThreads + " threads with " + numRuns + " runs per thread.");
			for (int i = 0; i < numThreads; i++){
				final int threadNumber = i;
				threads[i] = new Thread(){ 
					public void run(){
						try {
							//Runs the current test numRuns times in this thread.
							for (int j = 0; j < numRuns; j++){
								method.invoke(siTest);
							}
							
						} catch (Exception e) {
							//Since this is threaded we need to handle any exceptions.  In this case by failing the test.
							StringWriter printException = new StringWriter();
							e.printStackTrace(new PrintWriter(printException));
							System.err.println(method.getName() + " FAILED: A testing thread threw an exception:\n" + printException.toString());
							failTest();
						}
					}
				};
				threads[i].start();
			}
			//Wait for all threads to finish before moving on to the next test.
			for (Thread thread : threads){
				thread.join();
			}
			if (!failedTest){
				System.out.println("Test " + method.getName() + " passed.");
			}
		}
		ServerIntegrationTest.tearDownAfterClass();
	}
	

}
