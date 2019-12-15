/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package LocalFTTimedGLB_NoVar.examples.UTSOneQueue;

import GLBCoop.examples.UTSOneQueue.TreeNode;
import LocalFTTimedGLB_NoVar.LocalFTTaskBag;
import java.io.Serializable;

class Bag extends LocalFTTaskBag implements Serializable {
  public TreeNode[] hash;

  @Override
  public int size() {
    return hash.length;
  }
}
