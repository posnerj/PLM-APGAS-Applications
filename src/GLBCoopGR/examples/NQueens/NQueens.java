/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package GLBCoopGR.examples.NQueens;

import java.io.Serializable;

/** Created by jposner on 05.07.17. */
public class NQueens implements Serializable {

  static final transient long[] SOLUTIONS = {
    1,
    0,
    0,
    2,
    10, /* 5 */
    4,
    40,
    92,
    352,
    724, /* 10 */
    2680,
    14200,
    73712,
    365596,
    2279184, /* 15 */
    14772512,
    95815104,
    666090624,
    4968057848L,
    39029188884L, /* 20 */
  };
  public static int SIZE = 15;
  public static int THRESHOLD = 7;
  public static int INIT_SIZE = 4096;
  public long count = 0;
  public int[][] a;
  public int[] depth;
  public int size;

  public NQueens() {
    this.a = new int[INIT_SIZE][];
    this.depth = new int[INIT_SIZE];
    this.size = 0;
  }

  public NQueens(int size, int threshold) {
    this();
    SIZE = size;
    THRESHOLD = threshold;
  }

  public static int[] extendRight(final int[] src, final int newValue) {
    final int[] res = new int[src.length + 1];

    System.arraycopy(src, 0, res, 0, src.length);
    res[src.length] = newValue;

    return res;
  }

  /*
   * <a> contains array of <n> queen positions.  Returns 1
   * if none of the queens conflict, and returns 0 otherwise.
   */
  public static boolean boardValid(final int n, final int[] a) {
    int i, j;
    int p, q;

    for (i = 0; i < n; i++) {
      p = a[i];

      for (j = (i + 1); j < n; j++) {
        q = a[j];
        if (q == p || q == p - (j - i) || q == p + (j - i)) {
          return false;
        }
      }
    }
    return true;
  }

  public static void main(String[] args) {
    NQueens queens = new NQueens();
    queens.push(new int[0], 0);
    while (queens.getSize() > 0) {
      queens.nqueensKernelPar();
    }

    System.out.println(queens.count);
  }

  public void push(int[] b, int d) {
    while (this.size >= this.depth.length) {
      this.grow();
    }
    this.a[this.size] = new int[b.length];
    System.arraycopy(b, 0, this.a[this.size], 0, b.length);
    this.depth[this.size++] = d;
  }

  public void grow() {
    int capacity = this.depth.length * 2;
    int[][] b = new int[capacity][];

    for (int i = 0; i < this.size; i++) {
      b[i] = new int[this.a[i].length];
      System.arraycopy(this.a[i], 0, b[i], 0, this.a[i].length);
    }

    this.a = b;
    int[] d = new int[capacity];
    System.arraycopy(this.depth, 0, d, 0, this.size);
    this.depth = d;
  }

  public int getSize() {
    return this.size;
  }

  public void nqueensKernelPar() {
    int top = --this.size;
    int currentA[] = a[top];
    int currentD = depth[top];

    for (int i = 0; i < SIZE; i++) {
      final int ii = i;
      final int[] b = extendRight(currentA, ii);

      if (boardValid((currentD + 1), b)) {
        if (currentD < THRESHOLD) {
          push(b, currentD + 1);
        } else {
          final int[] b2 = new int[SIZE];
          try {
            System.arraycopy(b, 0, b2, 0, b.length);

          } catch (Throwable t) {
            t.printStackTrace();
          }
          nqueensKernelSeq(b2, depth[top] + 1);
        }
      }
    }
  }

  public void nqueensKernelSeq(final int[] a, final int depth) {
    if (SIZE == depth) {
      this.count++;
      return;
    }

    for (int i = 0; i < SIZE; i++) {
      a[depth] = i;
      if (boardValid((depth + 1), a)) {
        nqueensKernelSeq(a, depth + 1);
      }
    }
  }
}
