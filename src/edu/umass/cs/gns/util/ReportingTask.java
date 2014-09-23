package edu.umass.cs.gns.util;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.GenericMessagingTask;
import edu.umass.cs.gns.util.Reportable;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/* A utility class for gathering stats and reporting to specified
 * recipients from any object that implements Reportable.
 */

public class ReportingTask<NodeIDType> implements Runnable {

	private static final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(5);
	public static List<Runnable> stop() {return executor.shutdownNow();}
	
	private final Reportable<NodeIDType> reportee;
	private final double msgRate; // total message rate per second for this task
	
	public ReportingTask(Reportable<NodeIDType> r, double rate) {
		this.reportee = r;
		this.msgRate = rate;
	}
	
	public ScheduledFuture<?> start() {
		long delay = (long)((1/msgRate)*reportee.getRecipients().size()*1000);
		return executor.scheduleWithFixedDelay(this, 0, delay, TimeUnit.MILLISECONDS);
	}

	@Override
	public void run() {
		JSONObject report = this.reportee.getStats();
		GenericMessagingTask<NodeIDType,?> mtask = new GenericMessagingTask<NodeIDType, Object>(reportee.getRecipients().toArray(), report);
		try {
			this.reportee.getJSONMessenger().send(mtask);
		} catch(IOException ioe) {
			ioe.printStackTrace();
		} catch(JSONException je) {
			je.printStackTrace();
		}

	}
}
