package edu.umass.cs.gnsclient.client;

import edu.umass.cs.gnsclient.client.http.UniversalHttpClient;

/**
 * @author arun
 *
 *         This class exists for backwards compatible test methods that we don't
 *         want to expose in {@link GNSClientCommands}.
 */
public class GNSClientCommandsTest {
	/**
	 * @param client
	 * @param entry
	 * @param tagName
	 * @return GuidEntry created
	 * @throws Exception
	 */
	public static GuidEntry addTag(GNSClientInterface client, GuidEntry entry,
			String tagName) throws Exception {
		((GNSClientCommands) client).addTag(entry, tagName);
		return entry;
	}
}
