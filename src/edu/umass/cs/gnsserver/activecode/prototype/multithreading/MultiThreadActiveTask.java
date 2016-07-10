package edu.umass.cs.gnsserver.activecode.prototype.multithreading;

import java.io.UnsupportedEncodingException;

import javax.script.ScriptException;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveRunner;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage.Type;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.ActiveChannel;
import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * @author gaozy
 *
 */
public class MultiThreadActiveTask implements Runnable {

	ActiveRunner runner;
	ActiveMessage am;
	//MultiThreadActiveWorker worker;
	ActiveChannel channel;
	
	protected MultiThreadActiveTask(ActiveRunner runner, ActiveMessage am, ActiveChannel channel) {
		this.runner = runner;
		this.am = am;
		//this.worker = worker;
		this.channel = channel;
	}
	
	@Override
	public void run() {
		byte[] buf = null;
		ActiveMessage response = null;
		try {
			ValuesMap value = runner.runCode(am.getGuid(), am.getField(), am.getCode(), am.getValue(), am.getTtl());
			response = new ActiveMessage(value, null);
			buf = response.toBytes();
			channel.write(buf, 0, buf.length);
		} catch (NoSuchMethodException | ScriptException | UnsupportedEncodingException e) {
			e.printStackTrace();	
		}
		
	}

}
