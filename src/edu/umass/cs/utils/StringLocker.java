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
package edu.umass.cs.utils;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author V. Arun
 * 
 *         This class is a hack in order to have the syntactic luxury of saying
 *         synchronized(key), for an arbitrary String key, and get
 *         synchronization with respect to anyone else competing also for a
 *         synchronized(key) code block. Normally, we don't get this property,
 *         i.e., synchronization on key1 and key2 are unrelated even if
 *         key1.equals(key2). With this class, the caller can simply say
 *         synchronized(stringLocker.get(key)) { ... } and ensure
 *         synchronization for the following code block with respect to anyone
 *         else also using the same stringLocker object.
 * 
 *         We need to be careful to rely on this only when we know that the
 *         total number of Strings used for synchronization will be small. It is
 *         best to use this class only for constant strings.
 */
public class StringLocker {
	private final ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();

	/**
	 * @param key
	 * @return The canonical String that equals {@code key}.
	 */
	public String get(String key) {
		String prev = map.putIfAbsent(key, key);
		return prev!=null ? prev : key;
	}

	/**
	 * Removes key when no one is using it for synchronization. We know that no
	 * one is using it because we are using it ourselves through the
	 * {@code synchronized(get(key)} block below.
	 * 
	 * @param key
	 */
	public void remove(String key) {
		synchronized (get(key)) {
			this.map.remove(key);
		}
	}
}
