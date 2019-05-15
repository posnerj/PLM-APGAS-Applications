/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package utils;

import com.hazelcast.core.ReadOnly;
import com.hazelcast.map.EntryProcessor;

public interface ReadOnlyEntryProcessor extends EntryProcessor, ReadOnly {};
