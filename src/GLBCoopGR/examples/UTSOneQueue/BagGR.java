package GLBCoopGR.examples.UTSOneQueue;

import GLBCoopGR.TaskBagGR;
import java.io.Serializable;

public class BagGR implements TaskBagGR, Serializable {

  public TreeNode[] hash;

  @Override
  public int size() {
    return hash.length;
  }

  public BagGR(int size) {
    this.hash = new TreeNode[size];
  }

  public void merge(TaskBagGR taskBagGR) {
    this.merge((BagGR) taskBagGR);
  }

  public void merge(BagGR bag) {
    TreeNode[] newHash = new TreeNode[size() + bag.size()];
    System.arraycopy(this.hash, 0, newHash, 0, size());
    System.arraycopy(bag.hash, 0, newHash, size(), bag.size());
    this.hash = newHash;
  }
}
