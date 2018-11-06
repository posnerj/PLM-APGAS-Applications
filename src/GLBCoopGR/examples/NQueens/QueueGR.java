package GLBCoopGR.examples.NQueens;

import static apgas.Constructs.here;

import GLBCoopGR.GLBResultGR;
import GLBCoopGR.TaskBagGR;
import GLBCoopGR.TaskQueueGR;
import java.io.Serializable;
import java.util.Arrays;

/** Created by jposner on 05.07.17. */
public final class QueueGR extends NQueens implements TaskQueueGR<QueueGR, Long>, Serializable {

  public QueueGR() {
    super();
  }

  public QueueGR(int size, int threshold) {
    super(size, threshold);
  }

  @Override
  public boolean process(int n) {
    int i = 0;
    for (; ((i < n) && (this.size() > 0)); ++i) {
      this.nqueensKernelPar();
    }
    return (this.size() > 0);
  }

  @Override
  public TaskBagGR split() {

    if (2 > this.size()) {
      return null;
    }

    //        int otherHalf = this.size / 2;
    int otherHalf = this.size * 1 / 6;
    if (otherHalf == 0) {
      otherHalf = 1;
    }
    //        int otherHalf = this.size - (this.size -1);

    int myHalf = this.size - otherHalf;

    BagGR loot = new BagGR(otherHalf);

    int[] lootD = new int[otherHalf];
    int[][] lootA = new int[otherHalf][];

    //        System.out.println("otherHalf " + otherHalf + ", myHalf " + myHalf + ", size " +
    // this.size());
    // von oben
    //        System.arraycopy(this.depth, this.size-otherHalf, lootD, 0, otherHalf);
    //        for (int i = this.size-otherHalf; i < this.size; i++) {
    //            int ii = i - (this.size-otherHalf);
    ////            System.out.println("ii " + ii + ", i " + i);
    //            lootA[ii] = new int[a[i].length];
    //            System.arraycopy(this.a[i], 0, lootA[ii], 0, a[i].length);
    //        }

    //        //von unten
    System.arraycopy(this.depth, 0, lootD, 0, otherHalf);
    System.arraycopy(this.depth, otherHalf, this.depth, 0, myHalf);

    for (int i = 0; i < otherHalf; i++) {
      lootA[i] = new int[a[i].length];
      System.arraycopy(this.a[i], 0, lootA[i], 0, a[i].length);
    }

    int j = 0;
    for (int i = otherHalf; i < this.size(); i++) {
      this.a[j] = new int[a[i].length];
      System.arraycopy(this.a[i], 0, this.a[j++], 0, a[i].length);
    }

    this.size = myHalf;

    loot.a = lootA;
    loot.depth = lootD;

    return loot;
  }

  @Override
  public void merge(TaskBagGR taskBagGR) {
    BagGR bag = (BagGR) taskBagGR;
    if ((null == bag) || (0 == bag.size())) {
      System.err.println(here() + " merge: bag was empty!!!");
      return;
    }
    int bagSize = bag.size();
    int newSize = this.size + bagSize;
    int thisSize = this.size();
    while (newSize >= this.depth.length) {
      this.grow();
    }

    System.arraycopy(bag.depth, 0, this.depth, thisSize, bagSize);

    for (int i = 0; i < bag.depth.length; i++) {
      a[i + thisSize] = new int[bag.a[i].length];
      System.arraycopy(bag.a[i], 0, a[i + thisSize], 0, bag.a[i].length);
    }

    this.size = newSize;
  }

  @Override
  public long count() {
    return count;
  }

  @Override
  public GLBResultGR<Long> getResult() {
    NQueensResultGR result = new NQueensResultGR();
    return result;
  }

  @Override
  public void printLog() {
    return;
  }

  @Override
  public void mergeResult(TaskQueueGR<QueueGR, Long> that) {
    if (that == null) {
      System.err.println(here() + "(in mergeResult): other is null");
    }
    count += that.count();
  }

  @Override
  public int size() {
    return this.getSize();
  }

  public void init() {
    push(new int[0], 0);
  }

  @Override
  public String toString() {
    return "QueueGR{"
        + "a="
        + aToString()
        + ", depth="
        + Arrays.toString(depth)
        + ", size="
        + size
        + '}';
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

  public class NQueensResultGR extends GLBResultGR<Long> {

    Long[] result;

    public NQueensResultGR() {
      this.result = new Long[1];
    }

    @Override
    public Long[] getResult() {
      this.result[0] = count();
      return this.result;
    }

    @Override
    public void display(Long[] param) {}
  }
}
