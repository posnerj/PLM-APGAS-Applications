/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package FTGLB;

import apgas.util.IncrementalEntryValue;
import java.io.Serializable;
import java.util.Arrays;

public class QueueWrapper<Queue extends FTTaskQueue<Queue, T>, T extends Serializable>
    extends IncrementalEntryValue implements Serializable {

  private static final long serialVersionUID = 1L;

  public final Queue queue;
  public final long[] receivedLids;
  private long myLid;
  private boolean done;

  public QueueWrapper(Queue q, int numPlaces) {
    this.queue = q;
    receivedLids = new long[numPlaces];
    Arrays.fill(this.receivedLids, Long.MIN_VALUE);
    this.setMyLid(Long.MIN_VALUE);
    done = false;
  }

  public boolean getDone() {
    return done;
  }

  public void setDone(boolean done) {
    this.done = done;
  }

  public long getReceivedLid(int id) {
    return receivedLids[id];
  }

  public void setReceivedLid(int id, long lid) {
    this.receivedLids[id] = lid;
  }

  public long getMyLid() {
    return myLid;
  }

  public void setMyLid(long myLid) {
    this.myLid = myLid;
  }
}
