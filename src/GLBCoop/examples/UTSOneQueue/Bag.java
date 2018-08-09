package GLBCoop.examples.UTSOneQueue;

import GLBCoop.TaskBag;
import java.io.Serializable;

public class Bag implements TaskBag, Serializable {

  public TreeNode[] hash;

  @Override
  public int size() {
    return hash.length;
  }

  public Bag(int size) {
    this.hash = new TreeNode[size];
  }

  public void merge(TaskBag taskBag) {
    this.merge((Bag) taskBag);
  }

  public void merge(Bag bag) {
    TreeNode[] newHash = new TreeNode[size() + bag.size()];
    System.arraycopy(this.hash, 0, newHash, 0, size());
    System.arraycopy(bag.hash, 0, newHash, size(), bag.size());
    this.hash = newHash;
  }
}
