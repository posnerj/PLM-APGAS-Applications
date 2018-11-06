package GLBCoopGR.examples.NQueens;

import GLBCoopGR.TaskBagGR;
import java.util.Arrays;

/** Created by jposner on 05.07.17. */
public class BagGR implements TaskBagGR {

  public int[][] a;
  public int[] depth;

  public BagGR(int size) {
    this.a = new int[size][];
    this.depth = new int[size];
  }

  @Override
  public int size() {
    return this.depth.length;
  }

  @Override
  public String toString() {
    return "BagGR{" + "a=" + aToString() + ", depth=" + Arrays.toString(depth) + '}';
  }

  public String aToString() {
    String result = new String();
    for (int i = 0; i < this.depth.length; i++) {
      if (a[i] != null) {
        if (a[i].length > 0) {
          result += a[i][0] + " ";
        } else {
          result += "null ";
        }
      } else {
        result += "null ";
      }
    }
    return result;
  }
}
