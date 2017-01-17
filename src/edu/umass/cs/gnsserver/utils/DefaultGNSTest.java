package edu.umass.cs.gnsserver.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Properties;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.paxosutil.RequestInstrumenter;
import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.integrationtests.RunServer;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnscommon.utils.ThreadUtils;
import edu.umass.cs.gnsserver.database.MongoRecords;
import edu.umass.cs.reconfiguration.ReconfigurableNode;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.DefaultNodeConfig;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DefaultTest;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 * 
 *
 */
public class DefaultGNSTest extends DefaultTest {
	/* arun: Do not change stuff or hardcode any thing in this class without
	 * consulting me. */

	private static final String HOME = System.getProperty("user.home");
	private static final String GNS_DIR = "GNS";
	private static final String GNS_HOME = HOME + "/" + GNS_DIR + "/";

	protected static final String RANDOM_PASSWORD = "password"
			+ RandomString.randomString(12);
	protected static final String RANDOM_ACCOUNT_ALIAS_PREFIX = "accountGUID";
	protected static final String globalAccountName = RANDOM_ACCOUNT_ALIAS_PREFIX
			+ RandomString.randomString(12);

	// static but not final
	protected static GNSClient client = null;
	protected static boolean serversStarted = false;

	// non-static
	private GuidEntry myAccountGUID = null;

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
	
	/**
	 * Overriding parent watcher with {code tearDownAfterClass(boolean)} invocation.
	 */
	@Rule
	public TestWatcher teardown = new TestWatcher() {
		@Override
		protected void failed(Throwable e, Description description) {
			System.out.println(" FAILED!!!!!!!!!!!!! " + e);
			e.printStackTrace();
			if (Config.getGlobalBoolean(PC.DEBUG))
				System.out.println(RequestInstrumenter.getLog());
			try {
				tearDownAfterClass(true);
			} catch (ClientException | IOException e1) {
				e1.printStackTrace();
			}
			System.exit(1);
		}
	};

	protected static enum DefaultProps {
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

		SINGLE_JVM("singleJVM", "false"),

		STOP_SERVER("stopServer", "true"),

		/**
		 * If {@link #STOP_SERVER}, whether to forceclear or just stop.
		 */
		FORCECLEAR("forceclear", "false"),

		;

		public final String key;
		public final String value;
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
		return ("true".equals(System.getProperty(DefaultProps.SINGLE_JVM.key)));
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
		if (!"true".equals(System.getProperty(DefaultProps.START_SERVER.key))
				|| serversAlreadyStarted())
			return;
		deleteLogFiles();
		startServers();
		waitTillServersReady();
		startClients();

		createMasterAccountGUID();
	}

	private static final int MAX_TRIES = 5;

	private static void createMasterAccountGUID() throws InterruptedException {
		int tries = MAX_TRIES;
		boolean accountCreated = false;

		do {
			try {
				System.out.print("Creating account GUID "
						+ (globalAccountName)
						+ (tries == MAX_TRIES ? "" : ": " + (tries - 1)
								+ " attempt remaining."));
				String createdGUID = client.execute(
						GNSCommand.createAccount(globalAccountName))
						.getResultString();
				Assert.assertEquals(createdGUID,
						(GuidUtils.getGUIDKeys(globalAccountName)).guid);

				accountCreated = true;
			} catch (Exception e) {
				e.printStackTrace();
				ThreadUtils.sleep((5 - tries) * 4000);
			}
		} while (!accountCreated && --tries > 0);
		if (accountCreated == false) {
			Util.suicide("Failure setting up account guid; aborting all tests.");
		}
		System.out.println(" ..created "
				+ GuidUtils.getGUIDKeys(globalAccountName));
	}

	protected static final long TIMEOUT = 8000;

	private static void startClients() throws IOException {
		System.out.print("Starting client ");
		int numRetries = 2;
		boolean forceCoordinated = true;
		client = new GNSClient().setNumRetriesUponTimeout(numRetries)
				.setForceCoordinatedReads(forceCoordinated)
				.setForcedTimeout(TIMEOUT);
		System.out.println("..client(s) created and connected to server.");
	}

	private static void waitTillServersReady() throws InterruptedException,
			FileNotFoundException, IOException {

		// no need to wait if singleJVM or not starting servers
		if (singleJVM()
				|| "false".equals(System
						.getProperty(DefaultProps.START_SERVER.key)))
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

	protected synchronized static final boolean serversAlreadyStarted() {
		return serversStarted;
	}

	// synchronized to prevent multiple tests calling this at the same time.
	private synchronized static void startServers() throws IOException {
		// start server
		if ("true".equals(System.getProperty(DefaultProps.START_SERVER.key))) {

			if (System.getProperty(DefaultProps.FORCECLEAR.key).equals("true"))
				closeServers(DefaultProps.FORCECLEAR.key);

			options = getGigaPaxosOptions() + " restart all";

			String startServerCmd = System
					.getProperty(DefaultProps.SERVER_COMMAND.key)
					+ " "
					+ options;
			System.out.println(startServerCmd);

			// if we started servers, add a graceful shutdown hook
			Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

				@Override
				public void run() {
					try {
						tearDownAfterClass(true);
					} catch (ClientException | IOException e) {
						// can't do much at this point
						e.printStackTrace();
					}
				}
			}));

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
	 * @throws IOException
	 * @throws ClientException
	 *
	 */
	@AfterClass
	public static void tearDownAfterClass() throws ClientException, IOException {
		tearDownAfterClass(false);
	}

	private static void tearDownAfterClass(boolean force)
			throws ClientException, IOException {
		if (!"true".equals(System.getProperty(DefaultProps.STOP_SERVER.key))
				&& !force)
			return;
		removeCreatedState();
		closeServers(DefaultProps.FORCECLEAR.key);
		closeClients();
		printReverseEngineeredType();
	}

	private static void removeCreatedState() throws ClientException,
			IOException {
		// need to remove globalAccountName here
	}

	private static void closeServers(String stopOrForceclear) {
		System.out.println("--" + RequestInstrumenter.getLog() + "--");

		// if ("true".equals(System.getProperty(DefaultProps.STOP_SERVER.key)))
		{
			if (singleJVM()) {
				for (String server : PaxosConfig.getActives().keySet())
					ReconfigurableNode.forceClear(server);
				for (String server : ReconfigurationConfig
						.getReconfiguratorIDs())
					ReconfigurableNode.forceClear(server);
			} else { // separate JVMs
				String stopCmd = System
						.getProperty(DefaultProps.SERVER_COMMAND.key)
						+ " "
						+ getGigaPaxosOptions()
						+ (" " + stopOrForceclear + " all");
				System.out.print(stopOrForceclear + "ing" + " all servers in "
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
			dropAllDatabases();
		}
	}

	private static void closeClients() {
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
			try {
				ReconfigurableNode.main(new String[] { server,
						ReconfigurationConfig.CommandArgs.start.toString(),
						server });
			} catch (IOException e) {
				e.printStackTrace();
			}
		for (String server : PaxosConfig.getActives().keySet())
			try {
				ReconfigurableNode.main(new String[] { server,
						ReconfigurationConfig.CommandArgs.start.toString(),
						server });
			} catch (IOException e) {
				e.printStackTrace();
			}

	}

	private static final String getLogFile() throws FileNotFoundException,
			IOException {
		String logPropsFile = System
				.getProperty(DefaultProps.LOGGING_PROPERTIES.key);
		if(logPropsFile==null) return null;
		Properties logProps = new Properties();
		logProps.load(new FileInputStream(logPropsFile));
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

	// synchronized for one-time acount GUID creation
	private synchronized GuidEntry createOnceAccountGUID()
			throws ClientException, NoSuchAlgorithmException, IOException {
		if (myAccountGUID != null)
			return myAccountGUID;
		String accountHRN = RANDOM_ACCOUNT_ALIAS_PREFIX
				+ RandomString.randomString(12);
		String createdGUID = client.execute(
				GNSCommand.createAccount(accountHRN)).getResultString();
		Assert.assertEquals(createdGUID,
				(myAccountGUID = GuidUtils.getGUIDKeys(accountHRN)).guid);
		return myAccountGUID;
	}

	protected GuidEntry myAccountGUID() throws ClientException,
			NoSuchAlgorithmException, IOException {
		return this.createOnceAccountGUID();
	}

	protected void removeMyAccountGUID() throws ClientException,
			NoSuchAlgorithmException, IOException {
		if (this.myAccountGUID == null)
			return;
		System.out.println("Removing my account GUID " + this.myAccountGUID);
		client.execute(GNSCommand.accountGuidRemove(myAccountGUID));
	}

	private int numExecutedTests = 0;

	/**
	 * Method to detect if all test methods have been completed so that we can
	 * do any necessary cleanup.
	 * 
	 * @throws ClientException
	 * @throws NoSuchAlgorithmException
	 * @throws IOException
	 */
	@After
	public void after() throws ClientException, NoSuchAlgorithmException,
			IOException {
		numExecutedTests++;
		int numTotalTests = 0;
		Method[] methods = this.getClass().getMethods();
		for (Method method : methods) {
			Annotation[] annotations = method.getAnnotations();
			for (Annotation annotation : annotations)
				if (annotation.annotationType().equals(org.junit.Test.class))
					numTotalTests++;
		}
		if (numTotalTests == numExecutedTests)
			cleanup();
	}

	private void cleanup() throws ClientException, NoSuchAlgorithmException,
			IOException {
		removeMyAccountGUID();
	}

	/**
	 * A stop-only version of {@link DefaultGNSTest}.
	 */
	public static class ZDefaultGNSTest extends DefaultGNSTest {
		/**
		 *
		 */
		public ZDefaultGNSTest() {
		}

		@AfterClass
		public static void tearDownAfterClass() throws ClientException,
				IOException {
			DefaultGNSTest.tearDownAfterClass(true);
		}
	}
}
