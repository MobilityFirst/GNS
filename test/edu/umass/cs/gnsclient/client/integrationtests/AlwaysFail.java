/**
 * 
 */
package edu.umass.cs.gnsclient.client;

import static org.junit.Assert.*;


import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;


/**
 * @author Brendan
 * 
 * A test that always fails so I can test how Travis CI handles failed builds.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AlwaysFail {
	
	/**
	 * Always fails.
	 */
	@Test
	public void test_01_always_fails(){
		fail("This test always fails and is used for debugging things like Travis CI.");
	}
	

}
