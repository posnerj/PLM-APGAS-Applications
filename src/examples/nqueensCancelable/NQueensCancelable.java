/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package examples.nqueensCancelable;

import static apgas.Constructs.cancelAllCancelableAsyncAny;
import static apgas.Constructs.cancelableAsyncAny;
import static apgas.Constructs.getThreadLocalResult;
import static apgas.Constructs.here;
import static apgas.Constructs.immediateAsyncAt;
import static apgas.Constructs.places;
import static apgas.Constructs.reduceAsyncAny;
import static apgas.Constructs.setThreadLocalResult;

import apgas.Place;
import apgas.impl.ResultAsyncAny;
import apgas.impl.Worker;
import examples.nqueens.NQueensResult;
import java.util.stream.LongStream;

/**
 * Created by jonas on 02.04.17.
 *
 * <p>Source
 * https://github.com/shamsimam/priorityworkstealing/tree/master/src/main/java/edu/rice/habanero/benchmarks/nqueens
 */
public class NQueensCancelable {

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

  private long lastCancelCheck = 0;
  private static int size = 14;
  private static int threshold = 6;
  private static long solutionsLimit = Long.MAX_VALUE;
  private static boolean cancel = false;

  protected static void initNQueens(int size, int threshold, long limit) {
    NQueensCancelable.size = size;
    NQueensCancelable.threshold = threshold;
    NQueensCancelable.solutionsLimit = limit;
  }

  protected static boolean isSolutionCorrect(long value) {
    return LongStream.of(NQueensCancelable.SOLUTIONS).anyMatch(x -> x == value);
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

    if (true == checkCancel()) {
      return;
    }

    for (int i = 0; i < size; i++) {
      final int[] b = extendRight(a, i);

      if (boardValid((depth + 1), b)) {
        if (depth < threshold) {
          cancelableAsyncAny(
              () -> {
                new NQueensCancelable().compute(b, depth + 1);
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

    if (true == checkCancel()) {
      return;
    }

    for (int i = 0; i < size; i++) {
      a[depth] = i;
      if (boardValid((depth + 1), a)) {
        nqueensKernelSeq(a, depth + 1);
      }
    }
  }

  private boolean checkCancel() {
    if (true == cancel) {
      return true;
    }

    if (here().id != 0 || ((Worker) Thread.currentThread()).getMyID() != 0) {
      return false;
    }

    if (0 == lastCancelCheck) {
      lastCancelCheck = System.nanoTime();
      return false;
    }

    if (((System.nanoTime() - lastCancelCheck) / 1E9D) < 1) {
      return false;
    }

    ResultAsyncAny tmp = reduceAsyncAny();
    if (null == tmp) {
      lastCancelCheck = System.nanoTime();
      return false;
    }

    NQueensResult asyncAnyResult = (NQueensResult) tmp;

    if (count >= solutionsLimit || asyncAnyResult.getResult() >= solutionsLimit) {
      System.out.println("found solution limit");
      cancel = true;
      for (Place p : places()) {
        if (p.id == here().id) {
          continue;
        }
        immediateAsyncAt(
            p,
            () -> {
              NQueensCancelable.cancel = true;
            });
      }
      cancelAllCancelableAsyncAny();
      lastCancelCheck = System.nanoTime();
      return true;
    }

    lastCancelCheck = System.nanoTime();
    return false;
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
