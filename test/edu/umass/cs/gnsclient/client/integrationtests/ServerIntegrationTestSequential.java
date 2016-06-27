/**
 * 
 */
package edu.umass.cs.gnsclient.client.integrationtests;


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
public class ServerIntegrationTestSequential {

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
	
	//Any test method in ServerIntegrationTest matching one of these strings will only be run once instead of multiple times.
	private static final String[] runOnlyOnce = {
			"test_180_DBUpserts",
			"test_212_GroupRemoveGuid", 				// Double remove would cause an error.
			"test_223_GroupAndACLTestRemoveGuid",		// Same as above
			"test_231_AliasRemove",						// Same as above
			"test_410_JSONUpdate",
			"test_420_NewRead",
			"test_430_NewUpdate",
			"test_511_CreateBatch"		
	};
	/**
	 * Runs each of the ServerIntegrationTest tests numRuns times sequentially.
	 */
	@Test
	public void test_01_SequentialServerIntegrationTest() throws Exception{
		String runsString = System.getProperty("integrationTest.runs");
		int numRuns = 50;
		 if (runsString != null){
			 numRuns=Integer.parseInt(runsString);
		 }
		
		ServerIntegrationTest.setUpBeforeClass();
		ServerIntegrationTest siTest = new ServerIntegrationTest();
		Class<?> siClass = siTest.getClass();
		Method[] allMethods = siClass.getDeclaredMethods();
		TreeMap<String, Method> methodTree = new TreeMap<String, Method>();
		TreeMap<String, Method> dontRepeatMethodTree = new TreeMap<String, Method>();
		
		//Create a list of all the tests we're going to run.
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
		}
		//For each test, run it the appropriate number of times sequentially.
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
	
	

}
