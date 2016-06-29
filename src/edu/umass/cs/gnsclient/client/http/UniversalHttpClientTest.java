package edu.umass.cs.gnsclient.client.http;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.GNSClientInterface;
import edu.umass.cs.gnsclient.client.GuidEntry;

/**
 * @author arun
 *
 *         This class exists for backwards compatible test methods that we
 *         don't want to expose in {@link UniversalHttpClient}.
 */
public class UniversalHttpClientTest {
	/**
	 * @param client
	 * @param entry 
	 * @param tagName
	 * @return GuidEntry created
	 * @throws Exception
	 */
	public static GuidEntry addTag(GNSClientInterface client, GuidEntry entry,
			String tagName) throws Exception {
		((UniversalHttpClient) client).addTag(entry, tagName);
		return entry;
	}
}
