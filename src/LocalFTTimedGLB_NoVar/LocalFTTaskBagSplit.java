/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package LocalFTTimedGLB_NoVar;

import java.io.Serializable;

public class LocalFTTaskBagSplit implements Serializable {
  public long step;
  public long split;
  public long backupId; // corresponding backup
}
