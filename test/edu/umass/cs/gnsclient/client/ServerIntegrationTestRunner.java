/**
 * 
 */
package edu.umass.cs.gnsclient.client;

import static org.junit.Assert.*;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Brendan
 *
 */
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
		//TODO: Eventually take this as a commandline option...
		int numRuns = 100;
		ServerIntegrationTest siTest = new ServerIntegrationTest();
		Class<?> siClass = siTest.getClass();
		Method[] allMethods = siClass.getDeclaredMethods();
		for (Method method : allMethods){
			String methodName = method.getName();
			if (!methodName.startsWith("test_")){
				//Ignore non test methods
				continue;
			}
			//TODO: Check for some tests that we should not run here since they are not repeatable
			
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
		//TODO: Eventually take this as a commandline option...
		int numRuns = 100;
		ServerIntegrationTest siTest = new ServerIntegrationTest();
		Class<?> siClass = siTest.getClass();
		Method[] allMethods = siClass.getDeclaredMethods();
		for (Method method : allMethods){
			String methodName = method.getName();
			if (!methodName.startsWith("test_")){
				//Ignore non test methods
				continue;
			}
			//TODO: Check for some tests that we should not run here since they are not repeatable
			
			//Run the current method numRuns times.
			Thread threads[] = new Thread[numRuns];
			for (int i = 0; i < numRuns; i++){
				siTest.setAccountAlias(someAlias+Integer.toString(i));
				threads[i] = new Thread(){ 
					public void run(){
						try {
							method.invoke(siTest);
						} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
							// TODO Auto-generated catch block
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
		}
	}

}
