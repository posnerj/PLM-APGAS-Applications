package FTGLB.examples.UTS;

import FTGLB.TaskBag;
import utils.SHA1Rand;

public class Bag implements TaskBag {

  public SHA1Rand[] hash;
  public int[] lower;
  public int[] upper;

  public Bag(int size) {
    this.hash = new SHA1Rand[size];
    this.lower = new int[size];
    this.upper = new int[size];
  }

  @Override
  public int size() {
    return hash.length;
  }
}
