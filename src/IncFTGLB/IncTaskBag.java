/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package IncFTGLB;

import java.io.Serializable;

public interface IncTaskBag extends Serializable {

  int size();

  void mergeAtTop(IncTaskBag bag);
}
