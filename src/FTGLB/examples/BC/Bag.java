package FTGLB.examples.BC;

import FTGLB.TaskBag;

public class Bag implements TaskBag {

  public int[] lower;
  public int[] upper;

  public Bag(int size) {
    this.lower = new int[size];
    this.upper = new int[size];
  }

  public void merge(TaskBag other) {
    this.merge((Bag) other);
  }

  public void merge(Bag other) {
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
