package GLBCoord.examples.BC;

import GLBCoop.TaskBag;

public class Bag implements TaskBag {

  public Integer[] data;

  public Bag(Integer[] data) {
    this.data = data;
  }

  public Bag(int size) {
    this.data = new Integer[size];
  }

  public void merge(TaskBag that) {
    merge((Bag) that);
  }

  public void merge(Bag that) {
    int thisSize = this.size();
    int thatSize = that.size();
    int newSize = thisSize + thatSize;
    Integer[] newData = new Integer[newSize];

    System.arraycopy(this.data, 0, newData, 0, thisSize);
    System.arraycopy(that.data, 0, newData, thisSize, thatSize);

    this.data = newData;
  }

  public int size() {
    return this.data.length;
  }
}
