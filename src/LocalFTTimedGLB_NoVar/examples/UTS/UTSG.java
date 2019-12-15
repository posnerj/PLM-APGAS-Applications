/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package LocalFTTimedGLB_NoVar.examples.UTS;

import static LocalFTTimedGLB_NoVar.LocalFTGLBParameters.computeL;
import static LocalFTTimedGLB_NoVar.LocalFTGLBParameters.computeZ;
import static apgas.Constructs.places;

import LocalFTTimedGLB_NoVar.LocalFTGLB;
import LocalFTTimedGLB_NoVar.LocalFTGLBParameters;
import apgas.Configuration;
import apgas.SerializableCallable;
import java.util.Calendar;
import org.apache.commons.cli.*;

public class UTSG {

  static final int COUNT_PLACES = 4;
  static final int COUNT_THREADS = 16;

  public static Long[] compute(String[] args) throws ParseException {
    Options options = new Options();

    options.addOption("b", true, "Branching factor");
    options.addOption("r", true, "Seed (0 <= r < 2^31)");
    options.addOption("d", true, "Tree depth");
    options.addOption("n", true, "Number of nodes to process before probing. Default 511.");
    options.addOption("w", true, "Number of thieves to send out. Default 1.");
    options.addOption("l", true, "Base of the lifeline");
    options.addOption("m", true, "Max potential victims");
    options.addOption("v", true, "Verbose. Default 0 (no).");
    options.addOption("s", true, "backup-cycles");
    options.addOption(
        "timestamps",
        true,
        "count of timestamps for logging, 0=disabled, 500 = recommanded, default ist 0");
    options.addOption("backupCount", true, "amount of backups, default=1");
    options.addOption("crashNumber", true, "crash number");

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    int b = Integer.parseInt(cmd.getOptionValue("b", "4"));
    int r = Integer.parseInt(cmd.getOptionValue("r", "19"));
    int d = Integer.parseInt(cmd.getOptionValue("d", "13"));
    int n = Integer.parseInt(cmd.getOptionValue("n", "511"));
    int m = Integer.parseInt(cmd.getOptionValue("m", "1024"));
    int s = Integer.parseInt(cmd.getOptionValue("s", "1"));
    int timestamps = Integer.parseInt(cmd.getOptionValue("timestamps", "0"));
    int backupCount = Integer.parseInt(cmd.getOptionValue("backupCount", "1"));
    int crashNumber = Integer.parseInt(cmd.getOptionValue("crashNumber", "0"));

    if (System.getProperty(Configuration.APGAS_PLACES) == null) {
      System.setProperty(Configuration.APGAS_PLACES, String.valueOf(COUNT_PLACES));
    }

    String sysThreads = System.getProperty(Configuration.APGAS_THREADS);
    if (sysThreads == null || Integer.parseInt(sysThreads) < COUNT_THREADS) {
      System.setProperty(Configuration.APGAS_THREADS, String.valueOf(COUNT_THREADS));
    }
    sysThreads = System.getProperty(Configuration.APGAS_THREADS);

    int verbose =
        Integer.parseInt(
            cmd.getOptionValue("v", String.valueOf(LocalFTGLBParameters.SHOW_RESULT_FLAG)));
    int numPlaces = places().size();

    int l = Integer.parseInt(cmd.getOptionValue("l", String.valueOf(computeL(numPlaces))));
    int z = computeZ(l, numPlaces);
    int w = Integer.parseInt(cmd.getOptionValue("w", String.valueOf(z)));

    System.out.println(
        "places = "
            + numPlaces
            + "   b = "
            + b
            + "   r = "
            + r
            + "   d = "
            + d
            + "   w = "
            + w
            + "   n = "
            + n
            + "   l = "
            + l
            + "   m = "
            + m
            + "   z = "
            + z
            //                + "   g = " + g
            + "   v = "
            + verbose
            + "   s = "
            + s
            + "   timestamps = "
            + timestamps
            + "   sysThreads = "
            + sysThreads
            + "   backupCount = "
            + backupCount
            + "   crashNumber = "
            + crashNumber);

    SerializableCallable<Queue> init = () -> new Queue(b, numPlaces);
    LocalFTGLBParameters glbPara =
        new LocalFTGLBParameters(
            n, w, l, z, m, verbose, timestamps, crashNumber, backupCount, numPlaces, s);
    LocalFTGLB<Queue, Long> glb = new LocalFTGLB<Queue, Long>(init, glbPara, true);

    Runnable start = () -> glb.getTaskQueue().init(r, d);

    Long[] result = glb.run(start);
    return result;
  }

  public static void main(String[] args) {
    int n = Integer.getInteger(utils.Constants.benchmarkIterations, 1);
    System.out.println("benchmarkIterations: " + n);
    for (int i = 0; i < n; i++) {
      System.out.println("Iteration: " + i + ", start date: " + Calendar.getInstance().getTime());
      System.out.println(UTSG.class.getName() + " starts");
      Long[] result = new Long[0];
      try {
        result = compute(args);
      } catch (ParseException e) {
        e.printStackTrace();
      }

      System.out.println("Result of run " + i + " is: " + result[0]);
      System.out.println("Iteration: " + i + ", end date: " + Calendar.getInstance().getTime());
      System.out.println("\n\n\n---------------------------------------------------------\n\n\n");
    }
  }
}
