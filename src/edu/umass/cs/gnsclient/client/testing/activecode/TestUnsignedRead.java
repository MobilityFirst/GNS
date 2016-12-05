package edu.umass.cs.gnsclient.client.testing.activecode;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;

/**
 * A simple test for unsigned read
 * @author gaozy
 *
 */
public class TestUnsignedRead {
	
	private final static String ACCOUNT_GUID = "GUID";
	private final static String PASSWORD = "";
	private final static String someField = "someField";
	private final static String someValue = "someValue";
	
	/**
	 * Update a field, them read the field without a reader
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception{
		
		final GNSClientCommands client = new GNSClientCommands();
		GuidEntry entry = GuidUtils.lookupOrCreateAccountGuid(
				client, ACCOUNT_GUID, PASSWORD);
		
		// Update someField with someValue 
		client.fieldUpdate(entry, someField, someValue);
		
		// Test unsigned read
		String response = client.fieldRead(entry.getGuid(), someField, null);
		assert(response.equals(someValue));
		
		System.out.println("Test succeeds!");
	}
}
