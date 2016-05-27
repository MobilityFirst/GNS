/**
 * 
 */
package edu.umass.cs.gnsclient.client;

import static org.junit.Assert.*;

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
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ServerIntegrationTestRunner {

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
	 * Runs each of the ServerIntegrationTest tests numRuns times sequentially.
	 */
	@Test
	public void test_01_SequentialServerIntegrationTest() throws Exception{
		String runsString = System.getProperty("integrationTest.runs");
		int numRuns = 10;
		 if (runsString != null){
			 numRuns=Integer.parseInt(runsString);
		 }
		
		ServerIntegrationTest.setUpBeforeClass();
		ServerIntegrationTest siTest = new ServerIntegrationTest();
		Class<?> siClass = siTest.getClass();
		Method[] allMethods = siClass.getDeclaredMethods();
		TreeMap<String, Method> methodTree = new TreeMap<String, Method>();
		TreeMap<String, Method> dontRepeatMethodTree = new TreeMap<String, Method>();
		for (Method method : allMethods){
			String methodName = method.getName();
			if (!methodName.startsWith("test_")){
				//Ignore non test methods
				continue;
			}
			//Exclude tests that don't work sequentially here.
			if (	methodName.equals("test_180_DBUpserts") ||
					methodName.contains("Remove") ||
					methodName.equals("test_232_AliasCheck") ||
					methodName.equals("test_410_JSONUpdate") ||
					methodName.equals("test_420_NewRead") ||
					methodName.equals("test_430_NewUpdate") ||
					methodName.equals("test_512_CheckBatch")){
					dontRepeatMethodTree.put(methodName,method);
					//continue;
			}

			
			methodTree.put(methodName,method);
		}
		for (Method method : methodTree.values()){
			
			//Run these tests only once.
			if (dontRepeatMethodTree.containsKey(method.getName())){
				System.out.println("Running test: " + method.getName() + " once.");
				method.invoke(siTest);
				continue;
			}
			//Run all other tests repeatedly.
			System.out.println("Running test: " + method.getName() + " " + numRuns + " times sequentially.");
			for (int i = 0; i < numRuns; i++){
				
				method.invoke(siTest);
			}
		}
	}
	
	/**
	 * Runs each of the ServerIntegrationTest tests numThreads times in parallel for numRuns time sequentially.
	 * Only uses one ServerIntegrationTest object and client. amongst all threads, and excludes tests that cannot be run in parallel.
	 */
	@Test
	public void test_02_ParallelServerIntegrationTest() throws Exception{
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
			if (!methodName.startsWith("test_")){
				//Ignore non test methods
				continue;
			}
			//Exclude tests that don't work sequentially here.
			if (	methodName.equals("test_180_DBUpserts") ||
					methodName.contains("Remove") ||
					methodName.equals("test_232_AliasCheck") ||
					methodName.equals("test_410_JSONUpdate") ||
					methodName.equals("test_420_NewRead") ||
					methodName.equals("test_430_NewUpdate") ||
					methodName.equals("test_512_CheckBatch")){
					dontRepeatMethodTree.put(methodName,method);
					//continue;
			}
			//Exclude tests that don't work in parallel here, and instead run them invididually.
			//test_220_GroupAndACLCreateGuids()
			if (	methodName.equals("test_220_GroupAndACLCreateGuids()")){
				//Add this test to the list of things to run single threaded.
					nonparallelMethodTree.put(methodName, method);
					//continue;
			}
			
			methodTree.put(methodName,method);
		}
		//TODO: numThreads and numRuns
		for (Method method : methodTree.values()){
			//String methodName = method.getName();		
			//Create numThreads threads.
			Thread threads[] = new Thread[numThreads];
			//ServerIntegrationTest threadSITest = siTest;
			if (dontRepeatMethodTree.containsKey(method.getName())){
				//Run these tests only once then move on.
				System.out.println("Running test: " + method.getName() + " once.");
				method.invoke(siTest);
				continue;
			}
			else if (nonparallelMethodTree.containsKey(method.getName())){
				//Run these test single threaded.
				System.out.println("Running test: " + method.getName() + " " + numRuns + " times sequentially.");
				for (int j = 0; j < numRuns; j++){
					method.invoke(siTest);
					continue;
				}
			}
			//These tests are threadable.
			System.out.println("Running test: " + method.getName() + " in " + numThreads + " threads with " + numRuns + " runs per thread.");
			for (int i = 0; i < numThreads; i++){
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
							fail("A testing thread threw an exception during test "+method.getName()+":\n" + printException.toString());
						}
					}
				};
				threads[i].run();
			}
			//Wait for all threads to finish before moving on to the next test.
			for (int i = 0; i < numThreads; i++){
				threads[i].join();
			}
			
		}
		ServerIntegrationTest.tearDownAfterClass();
	}

}
