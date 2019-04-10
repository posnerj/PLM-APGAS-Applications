package FTGLB.examples.UTSOneQueue;

import GLBCoop.TaskBag;
import GLBCoop.examples.UTSOneQueue.TreeNode;
import java.io.Serializable;

class Bag implements TaskBag, Serializable {

  public TreeNode[] hash;

  @Override
  public int size() {
    return hash.length;
  }
}
