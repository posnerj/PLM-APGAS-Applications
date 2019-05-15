/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package examples.nqueens;

import static apgas.Constructs.asyncAny;
import static apgas.Constructs.at;
import static apgas.Constructs.finishAsyncAny;
import static apgas.Constructs.places;
import static apgas.Constructs.reduceAsyncAny;

import apgas.Configuration;
import apgas.Place;
import apgas.impl.Config;
import java.util.Calendar;

/** Created by jonas on 02.04.17. */
public class StartNQueens {

  public static void main(String[] args) {
    System.out.println("Start date: " + Calendar.getInstance().getTime());

    if (System.getProperty(Configuration.APGAS_PLACES) == null) {
      System.setProperty(Configuration.APGAS_PLACES, "2");
    }
    if (System.getProperty(Configuration.APGAS_THREADS) == null) {
      System.setProperty(Configuration.APGAS_THREADS, "2");
    }
    System.setProperty(Config.APGAS_SERIALIZATION, "java");

    int numThreads = Integer.parseInt(System.getProperty(Configuration.APGAS_THREADS));

    int depth = 13;
    if (args.length >= 1) {
      depth = Integer.parseInt(args[0]);
    }
    int threshold = 2;
    if (args.length >= 2) {
      threshold = Integer.parseInt(args[1]);
    }
    final int _depth = depth;
    final int _threshold = threshold;
    for (Place p : places()) {
      at(
          p,
          () -> {
            NQueens.initNQueens(_depth, _threshold);
          });
    }

    if (_threshold >= _depth) {
      System.err.println("Attention threshold >= depth!!!");
    }

    System.out.println(
        "Running "
            + StartNQueens.class.getName()
            + " with "
            + places().size()
            + " places and "
            + numThreads
            + " "
            + "Threads, d="
            + _depth);

    long start = System.nanoTime();
    finishAsyncAny(
        () -> {
          asyncAny(
              () -> {
                final int[] a = new int[0];
                NQueens queens = new NQueens();
                queens.compute(a, 0);
              });
        },
        1000);
    long end = System.nanoTime();
    System.out.println("Process time: " + ((end - start) / 1E9D));

    start = System.nanoTime();
    NQueensResult result = (NQueensResult) reduceAsyncAny();
    end = System.nanoTime();
    System.out.println("Reduce time: " + ((end - start) / 1E9D));
    result.display();
    System.out.println(
        "The result is "
            + (NQueens.isSolutionCorrect(result.getResult())
                ? "correct"
                : " " + "wrong!!!!!!!!!!!!!"));
    System.out.println("End date: " + Calendar.getInstance().getTime());
  }
}
