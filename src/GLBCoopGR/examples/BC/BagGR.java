package GLBCoopGR.examples.BC;

import GLBCoopGR.TaskBagGR;

/** Created by jposner on 08.11.16. */
public class BagGR implements TaskBagGR {

  public int[] lower;
  public int[] upper;

  public BagGR(int size) {
    this.lower = new int[size];
    this.upper = new int[size];
  }

  public void merge(TaskBagGR other) {
    this.merge((BagGR) other);
  }

  public void merge(BagGR other) {
    if (other == null) {
      return;
    }
    int thisSize = this.size();

    if (thisSize == 0) {
      this.lower = other.lower;
      this.upper = other.upper;
      return;
    }

    int otherSize = other.size();
    if (otherSize == 0) {
      return;
    }

    int newSize = thisSize + otherSize;
    int[] newLower = new int[newSize];
    int[] newUpper = new int[newSize];

    System.arraycopy(this.lower, 0, newLower, 0, thisSize);
    System.arraycopy(other.lower, 0, newLower, thisSize, otherSize);

    System.arraycopy(this.upper, 0, newUpper, 0, thisSize);
    System.arraycopy(other.upper, 0, newUpper, thisSize, otherSize);

    this.lower = newLower;
    this.upper = newUpper;
  }

  @Override
  public int size() {
    return this.lower.length;
  }
}
