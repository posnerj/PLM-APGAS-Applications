/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package LocalFTTimedGLB_NoVar.examples.NQueens;

import static apgas.Constructs.here;

import GLBCoop.examples.NQueens.NQueens;
import LocalFTTimedGLB_NoVar.LocalFTGLBResult;
import LocalFTTimedGLB_NoVar.LocalFTTaskBag;
import LocalFTTimedGLB_NoVar.LocalFTTaskQueue;
import java.io.Serializable;
import java.util.Arrays;

/** Created by jposner on 05.07.17. */
public final class Queue extends NQueens implements LocalFTTaskQueue<Queue, Long>, Serializable {
  public Queue() {
    super();
  }

  public Queue(int size, int threshold) {
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
  public LocalFTTaskBag split() {

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

    Bag loot = new Bag(otherHalf);

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
  public LocalFTTaskBag getAllTasks() {
    final int s = this.size();
    Bag bag = new Bag(s);
    System.arraycopy(this.depth, 0, bag.depth, 0, s);
    for (int i = 0; i < s; i++) {
      bag.a[i] = new int[this.a[i].length];
      System.arraycopy(this.a[i], 0, bag.a[i], 0, this.a[i].length);
    }
    return bag;
  }

  @Override
  public void merge(LocalFTTaskBag taskBag) {
    Bag bag = (Bag) taskBag;

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
  public LocalFTGLBResult<Long> getResult() {
    NQueensResult result = new NQueensResult();
    return result;
  }

  @Override
  public void printLog() {
    return;
  }

  @Override
  public int size() {
    return this.getSize();
  }

  public void init() {
    push(new int[0], 0);
  }

  public class NQueensResult extends LocalFTGLBResult<Long> {
    Long[] result;

    public NQueensResult() {
      this.result = new Long[] {count};
    }

    @Override
    public Long[] getResult() {
      return this.result;
    }

    @Override
    public void display(Long[] param) {}

    @Override
    public void mergeResult(LocalFTGLBResult<Long> other) {
      NQueensResult otherResult = (NQueensResult) other;
      result[0] += otherResult.getResult()[0];
    }
  }

  @Override
  public String toString() {
    return "Queue{"
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

  @Override
  public void clearResult() {
    this.count = 0;
  }
}
