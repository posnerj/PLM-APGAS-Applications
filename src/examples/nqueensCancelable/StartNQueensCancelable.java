package examples.nqueensCancelable;

import static apgas.Constructs.at;
import static apgas.Constructs.cancelableAsyncAny;
import static apgas.Constructs.finishAsyncAny;
import static apgas.Constructs.places;
import static apgas.Constructs.reduceAsyncAny;

import apgas.Configuration;
import apgas.Place;
import apgas.impl.Config;
import apgas.impl.GlobalRuntimeImpl;
import examples.nqueens.NQueensResult;

/** Created by jonas on 02.04.17. */
public class StartNQueensCancelable {

  public static void main(String[] args) {

    if (System.getProperty(Configuration.APGAS_PLACES) == null) {
      System.setProperty(Configuration.APGAS_PLACES, "2");
    }
    if (System.getProperty(Configuration.APGAS_THREADS) == null) {
      System.setProperty(Configuration.APGAS_THREADS, "2");
    }
    System.setProperty(Config.APGAS_SERIALIZATION, "java");

    int numThreads = Integer.parseInt(System.getProperty(Configuration.APGAS_THREADS));

    int depth = 20;
    if (args.length >= 1) {
      depth = Integer.parseInt(args[0]);
    }
    int threshold = 2;
    if (args.length >= 2) {
      threshold = Integer.parseInt(args[1]);
    }
    long solutionLimit = Long.MAX_VALUE;
    if (args.length >= 3) {
      long l = Long.parseLong(args[2]);
      if (l > 0) {
        solutionLimit = Long.parseLong(args[2]);
      }
    }

    solutionLimit= 10;

    final int _depth = depth;
    final int _threshold = threshold;
    final long _solutionLimit = solutionLimit;
    for (Place p : places()) {
      at(
          p,
          () -> {
            NQueensCancelable.initNQueens(_depth, _threshold, _solutionLimit);
          });
    }

    if (_threshold >= _depth) {
      System.err.println("Attention threshold >= depth!!!");
    }

    System.out.println(
        "Running " + StartNQueensCancelable.class.getName() + " with "
            + places().size()
            + " places and "
            + numThreads
            + " "
            + "Threads, d="
            + _depth
            + ", threshold="
            + _threshold
            + ", "
            + "solutionsLimit="
            + _solutionLimit);

    long start = System.nanoTime();
    finishAsyncAny(
        () -> {
          cancelableAsyncAny(
              () -> {
                final int[] a = new int[0];
                NQueensCancelable queens = new NQueensCancelable();
                queens.compute(a, 0);
              });
        });
    long end = System.nanoTime();
    System.out.println("Process time: " + ((end - start) / 1E9D));

    start = System.nanoTime();
    NQueensResult result = (NQueensResult) reduceAsyncAny();
    end = System.nanoTime();
    System.out.println("Reduce time: " + ((end - start) / 1E9D));
    result.display();
    System.out.println(
        "The result is "
            + (NQueensCancelable.isSolutionCorrect(result.getResult())
                ? "correct"
                : " " + "wrong!!!!!!!!!!!!!"));
    for (Place p : places()) {
      at(
          p,
          () -> {
            System.out.println(GlobalRuntimeImpl.getRuntime().localCancelableTasks.entrySet());
          });
    }
  }
}
