package GLBCoopGR.examples.UTS;

import GLBCoopGR.TaskBagGR;
import utils.SHA1Rand;

/** Created by jposner on 08.11.16. */
public class BagGR implements TaskBagGR {

  public SHA1Rand[] hash;
  public int[] lower;
  public int[] upper;

  public BagGR(int size) {
    this.hash = new SHA1Rand[size];
    this.lower = new int[size];
    this.upper = new int[size];
  }

  @Override
  public int size() {
    return hash.length;
  }
}
