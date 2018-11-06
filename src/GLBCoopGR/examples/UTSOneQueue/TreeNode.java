package GLBCoopGR.examples.UTSOneQueue;

import java.io.Serializable;
import utils.SHA1Rand;

public class TreeNode implements Serializable {

  private static final long serialVersionUID = 1L;

  protected SHA1Rand parent;
  protected int id;

  public TreeNode(SHA1Rand parent, int id) {
    this.id = id;
    this.parent = parent;
  }

  public SHA1Rand getParent() {
    return parent;
  }

  public void setParent(SHA1Rand parent) {
    this.parent = parent;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }
}
