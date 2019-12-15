/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package LocalFTTimedGLB_NoVar;

import static apgas.Constructs.here;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class LocalFTTaskBag implements Serializable {
  private static final AtomicInteger LAST_ID = new AtomicInteger(1);

  public long bagId = ((long) (here().id) << 32) + LAST_ID.incrementAndGet();
  public int parentPlace = here().id;
  public LocalFTGLBResult ownerResult;
  public boolean recovered = false;
  public long backupId = 0L;

  public abstract int size();
}
