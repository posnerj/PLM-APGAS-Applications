/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package examples.nqueens;

import static apgas.Constructs.asyncAny;
import static apgas.Constructs.getThreadLocalResult;
import static apgas.Constructs.setThreadLocalResult;

import java.util.stream.LongStream;

/**
 * Created by jonas on 02.04.17.
 *
 * <p>Source
 * https://github.com/shamsimam/priorityworkstealing/tree/master/src/main/java/edu/rice/habanero/benchmarks/nqueens
 */
public class NQueens {

  private static final long[] SOLUTIONS = {
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

  private long count = 0;

  private static int size = 10;
  private static int threshold = 5;

  public static void initNQueens(int size, int threshold) {
    NQueens.size = size;
    NQueens.threshold = threshold;
  }

  public static boolean isSolutionCorrect(long value) {
    return LongStream.of(NQueens.SOLUTIONS).anyMatch(x -> x == value);
  }

  private static int[] extendRight(final int[] src, final int newValue) {
    final int[] res = new int[src.length + 1];

    System.arraycopy(src, 0, res, 0, src.length);
    res[src.length] = newValue;

    return res;
  }

  /*
   * <a> contains array of <n> queen positions.  Returns 1
   * if none of the queens conflict, and returns 0 otherwise.
   */
  private static boolean boardValid(final int n, final int[] a) {
    int i;
    int j;
    int p;
    int q;

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

  private void nqueensKernelPar(final int[] a, final int depth) {
    for (int i = 0; i < size; i++) {
      final int[] b = extendRight(a, i);

      if (boardValid((depth + 1), b)) {
        if (depth < threshold) {
          asyncAny(
              () -> {
                new NQueens().compute(b, depth + 1);
              });
        } else {
          final int[] b2 = new int[size];
          System.arraycopy(b, 0, b2, 0, b.length);
          nqueensKernelSeq(b2, depth + 1);
        }
      }
    }
  }

  private void nqueensKernelSeq(final int[] a, final int depth) {
    if (size == depth) {
      this.count++;
      return;
    }

    for (int i = 0; i < size; i++) {
      a[depth] = i;
      if (boardValid((depth + 1), a)) {
        nqueensKernelSeq(a, depth + 1);
      }
    }
  }

  public void compute(final int[] a, final int depth) {
    nqueensKernelPar(a, depth);
    addResult();
  }

  private void addResult() {
    NQueensResult result = getThreadLocalResult();
    if (result == null) {
      result = new NQueensResult();
      setThreadLocalResult(result);
    }
    result.mergeResult(this.count);
  }
}
