/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.gigapaxos.testing;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import edu.umass.cs.gigapaxos.paxospackets.PValuePacket;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.gigapaxos.paxosutil.RequestInstrumenter;

/*
 * @author arun
 * 
 * This class is for printing testing/debug information when a paxos test run is
 * killed ungracefully (using Ctrl-C).
 */
@SuppressWarnings("javadoc")
public class TESTPaxosShutdownThread extends Thread {

	public static final ConcurrentHashMap<Integer, PValuePacket> decisions101 = new ConcurrentHashMap<Integer, PValuePacket>();
	public static final LinkedBlockingQueue<Integer> decisions102 = new LinkedBlockingQueue<Integer>();

	public static final ConcurrentHashMap<Integer, PValuePacket> accepts101 = new ConcurrentHashMap<Integer, PValuePacket>();
	// public static final ConcurrentHashMap<Integer, PValuePacket> accepts102 =
	// new ConcurrentHashMap<Integer, PValuePacket>();
	public static final LinkedBlockingQueue<Integer> accepts102 = new LinkedBlockingQueue<Integer>();

	static {
		Runtime.getRuntime().addShutdownHook(
				new Thread(new TESTPaxosShutdownThread()));
	}
	private static Set<TESTPaxosClient> testClients = new HashSet<TESTPaxosClient>();
	private static Set<TESTPaxosApp> apps = new HashSet<TESTPaxosApp>();

	protected static void register(TESTPaxosClient[] clients) {
		for (TESTPaxosClient client : clients) {
			testClients.add(client);
		}
	}

	protected static void register(TESTPaxosApp app) {
		apps.add(app);
	}

	public void run() {
		System.out.println("![TESTPaxosShutdownThread invoked]!");
		Set<RequestPacket> missing = TESTPaxosClient
				.getMissingRequests(testClients.toArray(new TESTPaxosClient[0]));
		if (RequestInstrumenter.DEBUG) {
			String sep = "-------\n";
			for (RequestPacket req : missing) {
				// logging doesn't seem to work here
				System.err.println(sep
						+ (RequestInstrumenter.getLog(req.requestID)));
			}
			System.out.println("\n#missing=" + missing.size());
		} else if (!missing.isEmpty())
			System.out
					.println("Request instrumentation (RequestInstrumenter.DEBUG) not turned on.");
	}

	static class Main {
		public static void main(String[] args) {
			try {
				Thread.sleep(100000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
