package edu.umass.cs.gigapaxos.paxosutil;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.SQLPaxosLogger;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket.PaxosPacketType;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.Keyable;
import edu.umass.cs.utils.Pausable;

/**
 * @author arun
 *
 */
public class LogIndex implements Keyable<String>, Serializable, Pausable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 37128037120321945L;
	/**
	 * 
	 */
	public final String paxosID;
	/**
	 * 
	 */
	public final int version;

	private Integer gcSlot = -1;
	private String minLogfile = null;
	private long lastActive = System.currentTimeMillis();

	private ArrayList<LogIndexEntry> log = null;

	/**
	 *
	 */
	public static class LogIndexEntry implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 32189003128411932L;

		/**
		 * 
		 */
		public final int slot;
		final int ballotNum;
		final int ballotCoord;
		final int type;

		String logfile;
		long offset;
		int length;

		/**
		 * @param slot
		 * @param ballotNum
		 * @param ballotCoord
		 * @param type
		 * @param logfile
		 * @param offset
		 * @param length
		 */
		public LogIndexEntry(int slot, int ballotNum, int ballotCoord,
				int type, String logfile, long offset, int length) {
			this.slot = slot;
			this.ballotNum = ballotNum;
			this.ballotCoord = ballotCoord;
			this.type = type;
			this.logfile = logfile;
			this.offset = offset;
			this.length = length;
		}

		/**
		 * @return Logfile
		 */
		public String getLogfile() {
			return logfile;
		}

		/**
		 * @return Offset
		 */
		public long getOffset() {
			return this.offset;
		}

		/**
		 * @return Length
		 */
		public int getLength() {
			return this.length;
		}
	}

	/**
	 * @param paxosID
	 * @param version
	 */
	public LogIndex(String paxosID, int version) {
		this.paxosID = paxosID;
		this.version = version;
	}

	@Override
	public String getKey() {
		return paxosID;
	}

	/**
	 * @param gcSlot
	 * @return {@code this}
	 */
	public LogIndex setGCSlot(int gcSlot) {
		this.gcSlot = gcSlot;
		this.GC();
		return this;
	}

	/**
	 * 
	 */
	public void GC() {
		assert (this.gcSlot != null);
		if (this.log == null)
			return;
		for (Iterator<LogIndexEntry> entryIter = this.log.iterator(); entryIter
				.hasNext();) {
			LogIndexEntry entry = entryIter.next();
			if (entry.slot - this.gcSlot <= 0)
				entryIter.remove();
		}
		// the only time after initialization when minLogfile is updated
		if (!this.log.isEmpty())
			this.minLogfile = this.log.get(0).logfile;
	}

	/**
	 * @param s
	 * @param bnum
	 * @param bcoord
	 * @param type
	 * @param file
	 * @param offset
	 * @param length
	 * @return True if added
	 */
	public boolean add(int s, int bnum, int bcoord, int type, String file,
			long offset, int length) {
		return this.add(new LogIndexEntry(s, bnum, bcoord, type, file, offset,
				length));
	}

	/**
	 * @param cur
	 * @return True if modified.
	 */
	public boolean modify(LogIndexEntry cur) {
		for (LogIndexEntry entry : this.log) {
			if (entry.slot == cur.slot && entry.ballotNum == cur.ballotNum
					&& entry.ballotCoord == cur.ballotCoord
					&& entry.type == cur.type) {
				entry.logfile = cur.logfile;
				entry.offset = cur.offset;
				entry.length = cur.length;
				return true;
			}
		}
		return false;
	}

	/**
	 * @param entry
	 * @return True if added
	 */
	public boolean add(LogIndexEntry entry) {
		if (gcSlot == null || entry.slot - gcSlot <= 0)
			return false;
		if (this.minLogfile == null)
			this.minLogfile = entry.logfile;
		if (this.log == null)
			this.log = new ArrayList<LogIndexEntry>();
		this.lastActive = System.currentTimeMillis();

		synchronized (this.log) {
			return this.log.add(entry);
		}
	}

	/**
	 * @return Last active time.
	 */
	public long getLastActive() {
		return this.lastActive;
	}

	private ArrayList<LogIndexEntry> getLoggedMessages(int minSlot,
			Integer maxSlot, int type) {
		ArrayList<LogIndexEntry> messages = new ArrayList<LogIndexEntry>();
		if (this.log != null)
			synchronized (this.log) {
				for (LogIndexEntry entry : this.log)
					if ((type == -1 || entry.type == type)
							&& entry.slot - minSlot >= 0
							&& ((maxSlot == null) || (entry.slot - maxSlot <= 0)))
						messages.add(entry);
			}
		return messages;
	}

	/**
	 * @param minSlot
	 * @param maxSlot
	 * @return List of accepts in the range {@code [minSlot, maxSlot]}, both
	 *         inclusive.
	 */
	public ArrayList<LogIndexEntry> getLoggedAccepts(int minSlot,
			Integer maxSlot) {
		return this.getLoggedMessages(minSlot, maxSlot,
				PaxosPacketType.ACCEPT.getInt());
	}

	/**
	 * @param minSlot
	 * @param maxSlot
	 * @return List of decisions in the range {@code [minSlot, maxSlot]}, both
	 *         inclusive.
	 */
	public ArrayList<LogIndexEntry> getLoggedDecisions(int minSlot, int maxSlot) {
		return this.getLoggedMessages(minSlot, maxSlot,
				PaxosPacketType.DECISION.getInt());
	}

	/**
	 * @return Set of all logfiles from which messages are indexed.
	 */
	public Set<String> getLogfiles() {
		Set<String> logfiles = new HashSet<String>();
		if (this.log != null)
			for (LogIndexEntry entry : this.log)
				logfiles.add(entry.logfile);
		return logfiles;
	}

	/**
	 * The ordering of fields below is important for correctness as we are not
	 * using keys for fields here.
	 * 
	 * @return Serialized JSONArray string.
	 */
	public String toString() {
		JSONArray jArray = new JSONArray();
		jArray.put(this.paxosID); // 0
		jArray.put(this.version); // 1
		jArray.put(this.gcSlot); // 2
		jArray.put(this.minLogfile); // 3
		jArray.put(this.lastActive); // 4
		JSONArray logArray = new JSONArray();
		if (this.log != null)
			for (LogIndexEntry lindex : this.log) {
				JSONArray logEntryArray = new JSONArray();
				logEntryArray.put(lindex.slot);
				logEntryArray.put(lindex.ballotNum);
				logEntryArray.put(lindex.ballotCoord);
				logEntryArray.put(lindex.type);
				logEntryArray.put(lindex.logfile);
				logEntryArray.put(lindex.offset);
				logEntryArray.put(lindex.length);

				logArray.put(logEntryArray);
			}
		jArray.put(logArray); // 5
		return jArray.toString();
	}

	/**
	 * @param jArray
	 * @throws JSONException
	 */
	public LogIndex(JSONArray jArray) throws JSONException {
		this.paxosID = jArray.getString(0);
		this.version = (Integer) jArray.get(1);
		this.gcSlot = jArray.getInt(2);
		this.minLogfile = jArray.getString(3);
		this.lastActive = jArray.getLong(4);
		JSONArray logArray = jArray.getJSONArray(5);
		for (int i = 0; i < logArray.length(); i++) {
			if (this.log == null)
				this.log = new ArrayList<LogIndexEntry>();
			JSONArray jEntry = logArray.getJSONArray(i);
			synchronized (this.log) {
				this.log.add(new LogIndexEntry(jEntry.getInt(0), jEntry
						.getInt(1), jEntry.getInt(2), jEntry.getInt(3), jEntry
						.getString(4), jEntry.getLong(5), jEntry.getInt(6)));
			}
		}
	}

	/**
	 * @return Oldest log file containing an entry for this paxosID after the
	 *         last commit to disk.
	 */
	public String getMinLogfile() {
		return this.minLogfile;
	}

	/**
	 * @param slot
	 * @param ballotNum
	 * @param ballotCoord
	 * @param type
	 * @return True if log message can not be garbage collected yet.
	 */
	public boolean isLogMsgNeeded(int slot, int ballotNum, int ballotCoord,
			int type) {
		if (slot - this.gcSlot <= 0) {
			return false;
		} else if (type == PaxosPacketType.PREPARE.getInt()) {
			// we only need the highest prepare
			for (LogIndexEntry entry : this.log) {
				if (entry.ballotNum - ballotNum > 0
						|| (entry.ballotNum == ballotNum && entry.ballotCoord
								- ballotCoord > 0))
					return false;
			}
		}
		return true;
	}

	/**
	 * @param makeObject
	 * @return Summary string.
	 */
	public Object getSummary(boolean makeObject) {
		if (!makeObject)
			return null;
		return new Object() {
			public String toString() {
				String s = "";
				s += LogIndex.this.paxosID + ":" + LogIndex.this.version + ":"
						+ LogIndex.this.minLogfile + ":" + LogIndex.this.gcSlot;
				if (LogIndex.this.log != null && !LogIndex.this.log.isEmpty()) {
					s += ":[";
					for (LogIndexEntry entry : LogIndex.this.log) {
						s += (PaxosPacketType.getPaxosPacketType(entry.type)
								.toString().substring(0, 1) + entry.slot);
					}
					s += "]";
				}
				return s;
			}
		};
	}

	/**
	 * @param args
	 * @throws JSONException
	 * @throws IOException
	 */
	public static void main(String[] args) throws JSONException, IOException {
		LogIndex logIndex = new LogIndex("paxos0", 3);
		// logIndex.add(1, 2, 102, 3, "file1", 345, 1100);
		// logIndex.add(2, 2, 102, 3, "file1", 1445, 1200);
		System.out.println(logIndex.toString());
		LogIndex restored = new LogIndex(new JSONArray(logIndex.toString()));
		System.out.println(restored);
		assert (logIndex.toString().equals(restored.toString()));
		System.out.println(new String(SQLPaxosLogger.inflate(SQLPaxosLogger
				.deflate(logIndex.toString().getBytes("ISO-8859-1"))),
				"ISO-8859-1"));
	}
	
	static {PaxosConfig.load();}

	private static final long DEACTIVATION_PERIOD = Config
			.getGlobalLong(PC.DEACTIVATION_PERIOD);

	@Override
	public boolean isPausable() {
		return System.currentTimeMillis() - this.lastActive > DEACTIVATION_PERIOD;
	}
}
