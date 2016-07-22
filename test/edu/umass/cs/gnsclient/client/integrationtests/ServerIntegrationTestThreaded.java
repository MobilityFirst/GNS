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
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
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
	
	//Any test method in ServerIntegrationTest matching one of these strings will only be run once instead of multiple times, and only be run in one thread.
	private static final String[] runOnlyOnce = {
			"test_180_DBUpserts",
			"test_212_GroupRemoveGuid", 				// Double remove would cause an error.
			"test_223_GroupAndACLTestRemoveGuid",		// Same as above
			"test_230_AliasAdd",						//Double add causes duplicate exception
			"test_231_AliasRemove",						// Double remove would cause an error.
			"test_320_GeoSpatialSelect",                //Double add could cause duplicate exception if random strings collide.
			"test_410_JSONUpdate",
			"test_420_NewRead",
			"test_430_NewUpdate",
			"test_511_CreateBatch"		
	};
	
	//Any test method in ServerIntegrationTest matching one of these strings will only be run in one thread, but will be run repeatedly sequentially.
	private static final String[] runOnlySingleThreaded = {
			"test_020_RemoveGuid", 						// Uses random names and checks state, so collisions would cause failures.
			"test_030_RemoveGuidSansAccountInfo", 		// Same as above
			"test_130_ACLALLFields",					// Same as above
			"test_210_GroupCreate", 					// Same as above
			"test_211_GroupAdd",						// Depends on test_210_GroupCreate for guidToDelete
			"test_220_GroupAndACLCreateGuids", 			// Uses random names and checks state, so collisions would cause failures.
			"test_270_RemoveField", 					// Race condition in test could cause failure
			"test_280_ListOrderAndSetElement", 			// Collisions in random strings could cause failures since it checks state.
			"test_400_SetFieldNull", 					// Race condition in test could cause failure
			"test_410_JSONUpdate",						// Random string collisions would create race conditions that could cause test failure.
			"test_420_NewRead", 						// Race condition in test could cause failure
			"test_430_NewUpdate", 						// Same as above
			"test_440_CreateBytesField"					// Race condition in this test could cause test_441_ReadBytesField() to fail if remembered test value and last written test value differ.
	};

	/**
	 * Runs each of the ServerIntegrationTest tests numThreads times in parallel for numRuns time sequentially.
	 * Only uses one ServerIntegrationTest object and client. amongst all threads, and excludes tests that cannot be run in parallel.
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
			//Exclude non test methods by not putting them in our list.
			if (!methodName.startsWith("test_")){
				continue;
			}
			methodTree.put(methodName,method);
				
			//Create a list of test methods that will only be run once each.
			for (String exclusion : runOnlyOnce){
				if (methodName.equals(exclusion)){
						dontRepeatMethodTree.put(methodName,method);
				}
			}
			//Create a list of test methods to run single threaded only.
			for (String exclusion : runOnlySingleThreaded){
				if (methodName.equals(exclusion)){
					nonparallelMethodTree.put(methodName, method);
				}
			}
		}
		for (Method method : methodTree.values()){	
			//Create numThreads threads.
			Thread threads[] = new Thread[numThreads];
			
			//Run these tests only once then move on.
			if (dontRepeatMethodTree.containsKey(method.getName())){
				System.out.println("Running test: " + method.getName() + " once.");
				method.invoke(siTest);
				continue;
			}
			
			//Run these tests single threaded.
			else if (nonparallelMethodTree.containsKey(method.getName())){
				System.out.println("Running test: " + method.getName() + " " + numRuns + " times sequentially.");
				for (int j = 0; j < numRuns; j++){
					method.invoke(siTest);
				}
				continue;
			}
			
			//These tests will be run in parallel.
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
			for (Thread thread : threads){
				thread.join();
			}
		}
		ServerIntegrationTest.tearDownAfterClass();
	}
	

}
