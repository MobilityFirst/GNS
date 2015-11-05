package edu.umass.cs.gigapaxos.examples.noop;

import java.io.IOException;

import org.json.JSONException;

import edu.umass.cs.gigapaxos.PaxosClientAsync;
import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;

/**
 * @author arun
 * 
 *         A simple client for NoopApp.
 */
public class NoopPaxosClient extends PaxosClientAsync {

	/**
	 * @throws IOException
	 */
	public NoopPaxosClient() throws IOException {
		super();
	}

	/**
	 * A simple example of asynchronously sending a few requests with a callback
	 * method that is invoked when the request has been executed or is known to
	 * have failed.
	 * 
	 * @param args
	 * @throws IOException
	 * @throws JSONException
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, JSONException, InterruptedException {
		NoopPaxosClient noopClient = new NoopPaxosClient();
		for (int i = 0; i < 100; i++) {
			final String requestValue = "hello world" + i;
			noopClient.sendRequest(PaxosConfig.application.getSimpleName()+"0",
					requestValue, new RequestCallback() {
				long createTime = System.currentTimeMillis();
				@Override
				public void handleResponse(Request response) {
					System.out
							.println("Response for request ["
									+ requestValue
									+ "] = "
									+ (response instanceof ClientRequest ? ((ClientRequest) response)
											.getResponse() : null)
									+ " received in "
									+ (System.currentTimeMillis() - createTime)
									+ "ms");
				}
					});
			Thread.sleep(100);
		}
	}
}
