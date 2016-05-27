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
		for (Method method : allMethods){
			String methodName = method.getName();
			if (!methodName.startsWith("test_")){
				//Ignore non test methods
				continue;
			}
			if (	methodName.equals("test_180_DBUpserts") ||
					methodName.contains("Remove") ||
					methodName.equals("test_232_AliasCheck") ||
					methodName.equals("test_410_JSONUpdate") ||
					methodName.equals("test_420_NewRead") ||
					methodName.equals("test_430_NewUpdate") ||
					methodName.equals("test_512_CheckBatch")){
					continue;
			}

			
			methodTree.put(methodName,method);
		}
		for (Method method : methodTree.values()){
		System.out.println("Running test: " + method.getName() + " " + numRuns + " times sequentially.");
			//Run the current method numRuns times.
			for (int i = 0; i < numRuns; i++){
				method.invoke(siTest);
			}
		}
	}
	
	private String someAlias = "support@GNS.NAME";
	/**
	 * Runs each of the ServerIntegrationTest tests numRuns in parallel.
	 */
	@Test
	public void test_02_ParallelServerIntegrationTest() throws Exception{
		String runsString = System.getProperty("integrationTest.runs");
		int numRuns = 10;
		 if (runsString != null){
			 numRuns=Integer.parseInt(runsString);
		 }
		
		
		//System.out.println("*** Beginning parallel tests. ***");
		ServerIntegrationTest siTest = new ServerIntegrationTest();
		Class<?> siClass = siTest.getClass();
		Method[] allMethods = siClass.getDeclaredMethods();
		TreeMap<String, Method> methodTree = new TreeMap<String, Method>();
		for (Method method : allMethods){
			String methodName = method.getName();
			if (!methodName.startsWith("test_")){
				//Ignore non test methods
				continue;
			}
			if (	methodName.equals("test_180_DBUpserts") ||
					methodName.contains("Remove") ||
					methodName.equals("test_232_AliasCheck") ||
					methodName.equals("test_410_JSONUpdate") ||
					methodName.equals("test_420_NewRead") ||
					methodName.equals("test_430_NewUpdate") ||
					methodName.equals("test_512_CheckBatch")){
					continue;
			}

			
			methodTree.put(methodName,method);
		}
		ServerIntegrationTest.setUpBeforeClass();
		for (Method method : methodTree.values()){
			//String methodName = method.getName();
			
			System.out.println("Running test: " + method.getName() + " " + numRuns+ " times in parallel.");
			//Run the current method numRuns times.
			Thread threads[] = new Thread[numRuns];
			//ServerIntegrationTest threadSITest = siTest;
			for (int i = 0; i < numRuns; i++){
				//Each thread should have a unique account alias for its test.
				ServerIntegrationTest.setAccountAlias(someAlias+Integer.toString(i));
				ServerIntegrationTest.setUpBeforeClass();
				final ServerIntegrationTest threadSITest = new ServerIntegrationTest();
				threads[i] = new Thread(){ 
					public void run(){
						try {
							method.invoke(threadSITest);
						} catch (Exception e) {
							StringWriter printException = new StringWriter();
							e.printStackTrace(new PrintWriter(printException));
							fail("Thread threw an exception: \n" + printException.toString());
						}
					}
				};
				threads[i].run();
			}
			//Wait for all threads to finish before moving on to the next test.
			for (int i = 0; i < numRuns; i++){
				threads[i].join();
			}
			ServerIntegrationTest.tearDownAfterClass();
		}
	}

}
