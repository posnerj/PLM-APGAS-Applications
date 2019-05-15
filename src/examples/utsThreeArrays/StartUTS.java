/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package examples.utsThreeArrays;

import static apgas.Constructs.asyncAny;
import static apgas.Constructs.finishAsyncAny;
import static apgas.Constructs.reduceAsyncAny;
import static apgas.Constructs.staticInit;

import apgas.Configuration;
import java.util.Calendar;

/** Created by jposner on 15.03.17. */
public class StartUTS {

  public static void main(String[] args) {
    System.out.println("Start date: " + Calendar.getInstance().getTime());

    if (System.getProperty(Configuration.APGAS_PLACES) == null) {
      System.setProperty(Configuration.APGAS_PLACES, "2");
    }
    if (System.getProperty(Configuration.APGAS_THREADS) == null) {
      System.setProperty(Configuration.APGAS_THREADS, "2");
    }

    final int numPlaces = Integer.parseInt(System.getProperty(Configuration.APGAS_PLACES));
    final int numThreads = Integer.parseInt(System.getProperty(Configuration.APGAS_THREADS));

    staticInit(
        () -> {
          UTS.initMdList();
        });

    final UTS uts = new UTS(64);

    int d = 13;
    if (args.length == 1) {
      d = Integer.parseInt(args[0]);
    }
    final int _d = d;

    System.out.println(
        "Running "
            + StartUTS.class.getName()
            + " with "
            + numPlaces
            + " places and "
            + numThreads
            + " Threads, d="
            + _d);

    long start = 0;
    long end = 0;
    start = System.nanoTime();
    finishAsyncAny(
        () -> {
          uts.seed(19, _d);

          asyncAny(
              () -> {
                uts.run();
              });
        });
    end = System.nanoTime();
    System.out.println("Process time: " + ((end - start) / 1E9D));

    start = System.nanoTime();
    reduceAsyncAny().display();
    end = System.nanoTime();
    System.out.println("Reduce time: " + ((end - start) / 1E9D));
    System.out.println("End date: " + Calendar.getInstance().getTime());
  }
}
