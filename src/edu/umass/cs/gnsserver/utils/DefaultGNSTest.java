package edu.umass.cs.gnsserver.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.paxosutil.RequestInstrumenter;
import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.integrationtests.RunServer;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.utils.ThreadUtils;
import edu.umass.cs.gnsserver.database.MongoRecords;
import edu.umass.cs.reconfiguration.ReconfigurableNode;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.DefaultNodeConfig;
import edu.umass.cs.utils.DefaultTest;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 * 
 *
 */
public class DefaultGNSTest extends DefaultTest {
	/* Do not change stuff or hardcode any thing in this class randomly. Ask if
	 * you are unsure. */

	private static final String HOME = System.getProperty("user.home");
	private static final String GNS_DIR = "GNS";
	private static final String GNS_HOME = HOME + "/" + GNS_DIR + "/";

	protected static final String DEFAULT_ACCOUNT_ALIAS = "support@gns.name";
	protected static final String DEFAULT_PASSWORD = "password";

	protected static String accountAlias = DEFAULT_ACCOUNT_ALIAS;
	@Deprecated
	protected static GNSClientCommands clientCommands = null;
	protected static GNSClient client = null;
	protected static GuidEntry masterGuid = null;

	protected static boolean serversStarted = false;

	private static final String getPath(String filename) {
		if (new File(filename).exists()) {
			return filename;
		}
		if (new File(GNS_HOME + filename).exists()) {
			return GNS_HOME + filename;
		} else {
			Util.suicide("Can not find server startup script: " + filename);
		}
		return null;
	}

	private static enum DefaultProps {
		SERVER_COMMAND("server.command", GP_SERVER, true),

		GIGAPAXOS_CONFIG("gigapaxosConfig",
				"conf/gnsserver.3local.unittest.properties", true),

		KEYSTORE("javax.net.ssl.keyStore", "conf/keyStore.jks", true), KEYSTORE_PASSWORD(
				"javax.net.ssl.keyStorePassword", "qwerty"), TRUSTSTORE(
				"javax.net.ssl.trustStore", "conf/trustStore.jks", true), TRUSTSTORE_PASSWORD(
				"javax.net.ssl.trustStorePassword", "qwerty"),

		LOGGING_PROPERTIES("java.util.logging.config.file",
				"conf/logging.gns.unittest.properties", true),

		START_SERVER("startServer", "true"),

		SINGLE_JVM("singleJVM", "true"),

		FORCECLEAR("forceclear", "true");

		final String key;
		final String value;
		final boolean isFile;

		DefaultProps(String key, String value, boolean isFile) {
			this.key = key;
			this.value = value;
			this.isFile = isFile;
		}

		DefaultProps(String key, String value) {
			this(key, value, false);
		}
	}

	private static void setProperties() {
		for (DefaultProps prop : DefaultProps.values()) {
			if (System.getProperty(prop.key) == null) {
				System.setProperty(prop.key, prop.isFile ? getPath(prop.value)
						: prop.value);
			}
		}
	}

	// this static block must be above GP_OPTIONS
	static {
		setProperties();
	}

	private static final String GP_SERVER = "bin/gpServer.sh";
	private static final String GP_OPTIONS = getGigaPaxosOptions();
	private static String options = GP_OPTIONS;

	private static String getGigaPaxosOptions() {
		String gpOptions = "";
		for (DefaultProps prop : DefaultProps.values()) {
			gpOptions += " -D" + prop.key + "=" + System.getProperty(prop.key);
		}
		return gpOptions + " -ea";
	}

	private static boolean singleJVM() {
		return (System.getProperty("singleJVM") != null && System
				.getProperty("singleJVM").trim().toLowerCase().equals("true"));
	}

	/**
	 * @throws IOException
	 * @throws FileNotFoundException
	 * @throws InterruptedException
	 *
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws FileNotFoundException,
			IOException, InterruptedException {
		deleteLogFiles();
		startServers();
		waitTillServersReady();
		startClients();

		createMasterAccountGUID();

	}

	private static void createMasterAccountGUID() throws InterruptedException {
		int tries = 5;
		boolean accountCreated = false;

		do {
			try {
				System.out.println("Creating account guid: " + (tries - 1)
						+ " attempt remaining.");
				String createdGUID = client.execute(
						GNSCommand.createAccount(accountAlias))
						.getResultString();
				Assert.assertEquals(createdGUID,
						GuidUtils.getGUIDKeys(accountAlias).guid);

				// older code; okay to leave it hanging or to remove
				masterGuid = GuidUtils.lookupOrCreateAccountGuid(
						clientCommands, accountAlias, DEFAULT_PASSWORD, true);
				accountCreated = true;
			} catch (Exception e) {
				e.printStackTrace();
				ThreadUtils.sleep((5 - tries) * 4000);
			}
		} while (!accountCreated && --tries > 0);
		if (accountCreated == false) {
			Util.suicide("Failure setting up account guid; aborting all tests.");
		}
	}

	private static void startClients() throws IOException {
		System.out.println("Starting client");
		int numRetries = 2;
		boolean forceCoordinated = true;
		clientCommands = (GNSClientCommands) new GNSClientCommands()
				.setNumRetriesUponTimeout(numRetries).setForceCoordinatedReads(
						forceCoordinated);
		client = new GNSClient().setNumRetriesUponTimeout(numRetries)
				.setForceCoordinatedReads(forceCoordinated)
				.setForcedTimeout(8000);
		System.out.println("Client(s) created and connected to server.");
	}

	private static void waitTillServersReady() throws InterruptedException,
			FileNotFoundException, IOException {

		// no need to wait if singleJVM
		if (singleJVM())
			return;

		// explicit sleep not needed in local tests
		if (explicitlySleepTillServersReady())
			return;

		// logs check meaningful only for local tests
		if (!allServersLocal())
			return;

		// check logs
		int numServers = PaxosConfig.getActives().size()
				+ ReconfigurationConfig.getReconfigurators().size();

		int numServersUp = 0;
		// a little sleep ensures that there is time for at least one log file
		// to get created
		Thread.sleep(500);
		do {
			File[] files = getMatchingFiles(getLogFileDir(), getLogFile());
			numServersUp = 0;
			for (File f : files)
				numServersUp += Util.readFileAsString(f.getAbsolutePath())
						.contains("server ready") ? 1 : 0;

			System.out.println((numServersUp) + " out of "
					+ Integer.toString(numServers) + " servers are ready.");
			Thread.sleep(1000);
		} while (numServersUp < numServers);
	}

	private static File[] getMatchingFiles(String dir, String startsWith) {
		return new File(dir).listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return (dir + "/" + name).startsWith(startsWith);
			}
		});
	}

	// TODO: implement by checking for localhost
	private static boolean allServersLocal() {
		return true;
	}

	protected static final boolean serversAlreadyStarted() {
		return serversStarted;
	}

	// synchronized to prevent multiple tests calling this at the same time.
	private synchronized static void startServers() throws IOException {
		// start server
		if (System.getProperty("startServer") != null
				&& System.getProperty("startServer").equals("true")) {

			// forceclear
			String forceClearCmd = System
					.getProperty(DefaultProps.SERVER_COMMAND.key)
					+ " "
					+ getGigaPaxosOptions() + " forceclear all";
			if (System.getProperty(DefaultProps.FORCECLEAR.key).equals("true")) {
				System.out.println(forceClearCmd);
				RunServer.command(forceClearCmd, ".");
			}

			/* We need to do this to limit the number of files used by mongo.
			 * Otherwise failed runs quickly lead to more failed runs because
			 * index files created in previous runs are not removed. */
			dropAllDatabases();

			options = getGigaPaxosOptions() + " restart all";

			String startServerCmd = System
					.getProperty(DefaultProps.SERVER_COMMAND.key)
					+ " "
					+ options;
			System.out.println(startServerCmd);

			// servers are being started here
			if (singleJVM())
				startServersSingleJVM();
			else {
				ArrayList<String> output = RunServer.command(startServerCmd,
						".");
				if (output != null)
					for (String line : output)
						System.out.println(line);
				else
					Util.suicide("Server command failure: ; aborting all tests.");
			}
		}

		serversStarted = true;
	}

	private static boolean explicitlySleepTillServersReady()
			throws InterruptedException {
		/* The waitTillAllServersReady parameter is not needed for
		 * single-machine tests as we check the logs explicitly below. It may
		 * still be useful for distributed tests as there is no explicit support
		 * in gigapaxos' async client to detect if all servrs are up. */
		String waitServersProperty = "waitTillAllServersReady";
		long sleepTime = 0;
		if (System.getProperty(waitServersProperty) != null)
			sleepTime = Long.parseLong(System.getProperty(waitServersProperty));
		Thread.sleep(sleepTime);
		return sleepTime > 0;
	}

	/**
	 *
	 * @throws Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		removeCreatedState();
		closeClients();
		closeServers();
		dropAllDatabases();
		printReverseEngineeredType();
	}

	private static void removeCreatedState() {
		/* arun: need a more efficient, parallel implementation of removal of
		 * sub-guids, otherwise this times out. */

		// client.accountGuidRemove(masterGuid);
	}

	private static void closeServers() {
		System.out.println("--" + RequestInstrumenter.getLog() + "--");

		if (System.getProperty("startServer") != null
				&& System.getProperty("startServer").equals("true")) {
			if (singleJVM()) {
				for (String server : PaxosConfig.getActives().keySet())
					ReconfigurableNode.forceClear(server);
				for (String server : ReconfigurationConfig
						.getReconfiguratorIDs())
					ReconfigurableNode.forceClear(server);
			} else {

				boolean forceclear = System.getProperty(
						DefaultProps.FORCECLEAR.key).equals("true");
				String stopCmd = System
						.getProperty(DefaultProps.SERVER_COMMAND.key)
						+ " "
						+ getGigaPaxosOptions()
						+ (forceclear ? " forceclear all" : " stop all");
				System.out.print((forceclear ? "Force-clearing" : "Stopping")
						+ " all servers in "
						+ System.getProperty(DefaultProps.GIGAPAXOS_CONFIG.key)
						+ " with " + stopCmd);

				try {
					RunServer.command(stopCmd, ".");
				} catch (Exception e) {
					System.out.println(" failed to stop all servers with ["
							+ stopCmd + "]");
					e.printStackTrace();
					throw e;
				}
				System.out.println(" stopped all servers.");
			}
		}
	}

	private static void closeClients() {
		if (clientCommands != null)
			clientCommands.close();
		if (client != null)
			client.close();
	}

	private static void printReverseEngineeredType() {
		System.out.println("\nPrinting reverse-engineered return types:");
		for (CommandType type : GNSClientCommands.REVERSE_ENGINEER.keySet()) {
			System.out.println(type
					+ " returns "
					+ GNSClientCommands.REVERSE_ENGINEER.get(type)
					+ "; e.g., "
					+ Util.truncate(
							GNSClientCommands.RETURN_VALUE_EXAMPLE.get(type),
							64, 64));

		}
	}

	private static void dropAllDatabases() {
		for (String server : new DefaultNodeConfig<>(PaxosConfig.getActives(),
				ReconfigurationConfig.getReconfigurators()).getNodeIDs()) {
			MongoRecords.dropNodeDatabase(server);
		}
	}

	private static final void startServersSingleJVM() throws IOException {
		// all JVM properties should be already set above
		for (String server : ReconfigurationConfig.getReconfiguratorIDs())
			ReconfigurableNode
					.main(new String[] { server,
							ReconfigurationConfig.CommandArgs.start.toString(),
							server });
		for (String server : PaxosConfig.getActives().keySet())
			ReconfigurableNode
					.main(new String[] { server,
							ReconfigurationConfig.CommandArgs.start.toString(),
							server });
	}

	private static final String getLogFile() throws FileNotFoundException,
			IOException {
		Properties logProps = new Properties();
		logProps.load(new FileInputStream(System
				.getProperty(DefaultProps.LOGGING_PROPERTIES.key)));
		String logFiles = logProps
				.getProperty("java.util.logging.FileHandler.pattern");
		if (logFiles != null)
			logFiles = logFiles.replaceAll("%.*", "").trim();
		return logFiles;
	}

	private static final String getLogFileDir() throws FileNotFoundException,
			IOException {
		return getLogFile().replaceFirst("/[^/]*$", "");
	}

	private static final void deleteLogFiles() throws FileNotFoundException,
			IOException {
		String dir = getLogFileDir();
		String logFile = getLogFile();
		// make logs directory if it doesn't exist
		new File(dir).mkdirs();

		System.out.print("Deleting log files " + logFile + "*");
		for (File f : getMatchingFiles(dir, logFile))
			f.delete();
		System.out.println(" ...done");
	}
}
