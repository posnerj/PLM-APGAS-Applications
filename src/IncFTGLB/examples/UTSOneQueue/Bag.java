/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package IncFTGLB.examples.UTSOneQueue;

import GLBCoop.examples.UTSOneQueue.TreeNode;
import IncFTGLB.IncTaskBag;
import java.io.Serializable;

class Bag implements IncTaskBag, Serializable {

  public TreeNode[] hash;

  @Override
  public int size() {
    return hash.length;
  }

  @Override
  public void mergeAtTop(IncTaskBag bag) {
    mergeAtTop((Bag) bag);
  }

  public void mergeAtTop(Bag bag) {
    TreeNode[] newHash = new TreeNode[size() + bag.size()];
    System.arraycopy(this.hash, 0, newHash, 0, size());
    System.arraycopy(bag.hash, 0, newHash, size(), bag.size());
    this.hash = newHash;
  }
}
