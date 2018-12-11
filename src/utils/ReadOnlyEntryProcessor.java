package utils;

import com.hazelcast.core.ReadOnly;
import com.hazelcast.map.EntryProcessor;

public interface ReadOnlyEntryProcessor extends EntryProcessor, ReadOnly {};
