package edu.umass.cs.gns.gigapaxos.paxosutil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.json.JSONException;

import edu.umass.cs.gns.gigapaxos.PaxosManager;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.RequestPacket;

public class RequestBatcher extends ConsumerTask<RequestPacket> {

	private final HashMap<String, ArrayList<RequestPacket>> batched;
	private final PaxosManager<?> paxosManager;

	public RequestBatcher(HashMap<String, ArrayList<RequestPacket>> lock, PaxosManager<?> paxosManager) {
		super(lock);
		this.batched = lock;
		this.paxosManager = paxosManager;
	}

	@Override
	public void enqueueImpl(RequestPacket task) {
		ArrayList<RequestPacket> taskList = this.batched.get(task.getPaxosID());
		if (taskList == null)
			taskList = new ArrayList<RequestPacket>();
		taskList.add(task);
		this.batched.put(task.getPaxosID(), taskList); // unnecessary
	}

	@Override
	public RequestPacket dequeueImpl() {
		if (this.batched.isEmpty())
			return null;
		// extract first batch (grouped by paxosID)
		Iterator<ArrayList<RequestPacket>> iter = this.batched.values()
				.iterator();
		ArrayList<RequestPacket> taskList = iter.next();
		iter.remove();
		// make batched request out of the extracted list
		RequestPacket first = taskList.get(0);
		RequestPacket[] rest = new RequestPacket[taskList.size() - 1];
		for (int i = 1; i < taskList.size(); i++) {
			rest[i - 1] = taskList.get(i);
		}
		first.latchToBatch(rest);
		return first;
	}

	@Override
	public void process(RequestPacket task) {
		try {
			this.paxosManager.handleIncomingPacketInternal(task.toJSONObject());
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}
}
