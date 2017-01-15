/* Copyright (C) 2015 University of Massachusetts All Rights Reserved
 * 
 * Initial developer(s): Westy. */
package edu.umass.cs.gnsclient.client.integrationtests;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/**
 * For integration testing.
 * 
 * @author westy, arun
 */
public class RunServer {

	private static final Timer timer = new Timer(true);
	private static final int MAX_LINES = 1024;
	private static final long PROCESS_WAIT_TIMEOUT = 30;

	/**
	 * @param cmdline
	 * @param directory
	 * @return Output
	 */
	public static ArrayList<String> command(final String cmdline,
			final String directory) {
		return command(cmdline, directory, "true".equals(System.getProperty("inheritIO")));
	}

	/**
	 * Returns null if it failed for some reason.
	 *
	 * @param cmdline
	 * @param directory
	 * @param inheritIO 
	 * @return Output
	 */
	public static ArrayList<String> command(final String cmdline,
			final String directory, boolean inheritIO) {
		try {
			ProcessBuilder processBuilder = new ProcessBuilder(new String[] {
					"bash", "-c", cmdline });
			if (inheritIO)
				processBuilder.inheritIO()
				;

			Process process = processBuilder.redirectErrorStream(true)
					.directory(new File(directory)).start();

			if (!inheritIO)
				return gatherOutput(process);

			// There should really be a timeout here.
			if (process.waitFor(PROCESS_WAIT_TIMEOUT, TimeUnit.SECONDS)) 
				return new ArrayList<String>(); 
			else
				throw new RuntimeException("Process initiated by [" + cmdline
						+ "] timed out");

		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
			return null;
		}
	}

	private static ArrayList<String> gatherOutput(Process process)
			throws IOException {
		ArrayList<String> output = new ArrayList<>();
		BufferedReader br = new BufferedReader(new InputStreamReader(
				process.getInputStream()));
		String line = null;
		while ((line = br.readLine()) != null && output.size() < MAX_LINES) {
			output.add(line);
		}
		return output;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		test("which bash");
		test("pwd");

		// test("find . -type f -printf '%T@\\\\t%p\\\\n' "
		// + "| sort -n | cut -f 2- | "
		// + "sed -e 's/ /\\\\\\\\ /g' | xargs ls -halt");

	}

	static void test(String cmdline) {
		ArrayList<String> output = command(cmdline, ".");
		if (null == output) {
			System.out.println("\n\n\t\tCOMMAND FAILED: " + cmdline);
		} else {
			for (String line : output) {
				System.out.println(line);
			}
		}

	}
}
