package edu.umass.cs.gnsserver.activecode.prototype.multithreading;

import java.io.IOException;

import javax.script.ScriptException;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveRunner;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Channel;
import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * @author gaozy
 *
 */
public class MultiThreadActiveTask implements Runnable {

	ActiveRunner runner;
	ActiveMessage am;
	Channel channel;
	
	protected MultiThreadActiveTask(ActiveRunner runner, ActiveMessage am, Channel channel) {
		this.runner = runner;
		this.am = am;
		this.channel = channel;
	}
	
	@Override
	public void run() {
		ActiveMessage response = null;
		try {
			ValuesMap value = runner.runCode(am.getGuid(), am.getField(), am.getCode(), am.getValue(), am.getTtl(), am.getId());
			response = new ActiveMessage(am.getId(), value, null);
			channel.sendMessage(response);
		} catch (Exception e) {
			e.printStackTrace();	
		}
		
	}

}
