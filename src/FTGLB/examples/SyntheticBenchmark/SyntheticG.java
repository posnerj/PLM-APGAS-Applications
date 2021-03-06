/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package FTGLB.examples.SyntheticBenchmark;

import static FTGLB.FTGLBParameters.computeL;
import static FTGLB.FTGLBParameters.computeZ;
import static apgas.Constructs.places;

import FTGLB.FTGLB;
import FTGLB.FTGLBParameters;
import apgas.Configuration;
import apgas.SerializableCallable;
import apgas.impl.Config;
import java.util.Calendar;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class SyntheticG {

  static final int COUNT_PLACES = 4;
  static final int COUNT_THREADS = 16;

  public static Long[] compute(String[] args) throws ParseException {
    Options options = new Options();

    options.addOption("static", false, "Static initialization (Default dynamic)");
    options.addOption("dynamic", false, "Dynamic initialization (Default)");
    options.addOption("b", true, "Task ballast in bytes (Default 2 KiB)");
    options.addOption("t", true, "Task count (only used in static version) (Default 16384)");
    options.addOption("g", true, "Task granularity (Default 100)");
    options.addOption("d", true, "Tree depth");
    options.addOption("c", true, "Maximum amount of direct children of a task (Default 0)");
    options.addOption("n", true, "Number of nodes to process before probing. Default 511.");
    options.addOption("w", true, "Number of thieves to send out. Default 1.");
    options.addOption("l", true, "Base of the lifeline");
    options.addOption("m", true, "Max potential victims");
    options.addOption("v", true, "Verbose. Default 0 (no).");
    options.addOption(
        "k", true, " Write cyclic backups every k * n computation-elements. Default: 2048");
    options.addOption(
        "timestamps",
        true,
        "count of timestamps for logging, 0=disabled, 500 = recommanded, default ist 0");
    options.addOption("crashNumber", true, "postion of crashing, default ist 0");
    options.addOption("backupCount", true, "count of backups, default ist 1");

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    boolean staticMode = cmd.hasOption("static");
    int b = Integer.parseInt(cmd.getOptionValue("b", String.valueOf(1)));
    int t = Integer.parseInt(cmd.getOptionValue("t", String.valueOf(16 * 1024)));
    int c = Integer.parseInt(cmd.getOptionValue("c", "22"));
    int g = Integer.parseInt(cmd.getOptionValue("g", "1"));
    int d = Integer.parseInt(cmd.getOptionValue("d", "7"));
    int n = Integer.parseInt(cmd.getOptionValue("n", "511"));
    int m = Integer.parseInt(cmd.getOptionValue("m", "1024"));
    long k = Long.parseLong(cmd.getOptionValue("k", "12"));
    int timestamps = Integer.parseInt(cmd.getOptionValue("timestamps", "0"));
    int crashNumber = Integer.parseInt(cmd.getOptionValue("crashNumber", "0"));
    int backupCount = Integer.parseInt(cmd.getOptionValue("backupCount", "1"));

    if (System.getProperty(Configuration.APGAS_PLACES) == null) {
      System.setProperty(Configuration.APGAS_PLACES, String.valueOf(COUNT_PLACES));
    }

    String sysThreads = System.getProperty(Configuration.APGAS_THREADS);
    if (sysThreads == null || Integer.parseInt(sysThreads) < COUNT_THREADS) {
      System.setProperty(Configuration.APGAS_THREADS, String.valueOf(COUNT_THREADS));
    }
    sysThreads = System.getProperty(Configuration.APGAS_THREADS);

    System.setProperty(Config.APGAS_SERIALIZATION, "java");

    int verbose =
        Integer.parseInt(cmd.getOptionValue("v", String.valueOf(FTGLBParameters.SHOW_RESULT_FLAG)));
    int numPlaces = places().size();

    int l = Integer.parseInt(cmd.getOptionValue("l", String.valueOf(computeL(numPlaces))));
    int z = computeZ(l, numPlaces);
    int w = Integer.parseInt(cmd.getOptionValue("w", String.valueOf(z)));

    StringBuilder output = new StringBuilder();
    output.append("places = " + numPlaces);
    if (staticMode) {
      output.append("   static");
      output.append("   t = " + t);
    } else {
      output.append("   dynamic");
      output.append("   c = " + c);
      output.append("   d = " + d);
    }
    output.append("   b = " + b);
    output.append("   g = " + g);
    output.append("   w = " + w);
    output.append("   n = " + n);
    output.append("   l = " + l);
    output.append("   m = " + m);
    output.append("   z = " + z);
    output.append("   k = " + k);
    output.append("   v = " + verbose);
    output.append("   timestamps = " + timestamps);
    output.append("   sysThreads = " + sysThreads);
    output.append("   crashNumber = " + crashNumber);
    output.append("   backupCount = " + backupCount);
    System.out.println(output);

    if (backupCount > 6) {
      System.out.println("backupCount  can't be larger than than 6");
      return null;
    }

    Boolean propertyApgasRes = Boolean.valueOf(System.getProperty(Configuration.APGAS_RESILIENT));
    if (propertyApgasRes == null || propertyApgasRes.equals(false)) {
      System.out.println("Warning: APGAS_RESILIENT is disabled!!!!");
    }

    Long[] result;
    if (staticMode) {
      SerializableCallable<Queue> init = () -> new Queue(b, t, g);
      FTGLBParameters glbPara =
          new FTGLBParameters(
              n, w, l, z, m, verbose, timestamps, k, crashNumber, backupCount, numPlaces);
      FTGLB<Queue, Long> glb = new FTGLB<Queue, Long>(init, glbPara, false);
      result = glb.runParallel();
    } else {
      SerializableCallable<Queue> init = () -> new Queue(c);
      FTGLBParameters glbPara =
          new FTGLBParameters(
              n, w, l, z, m, verbose, timestamps, k, crashNumber, backupCount, numPlaces);
      FTGLB<Queue, Long> glb = new FTGLB<Queue, Long>(init, glbPara, true);
      result = glb.run(() -> glb.getTaskQueue().init(b, d, g));
    }
    return result;
  }

  public static void main(String[] args) {
    int n = Integer.getInteger(utils.Constants.benchmarkIterations, 1);
    System.out.println("benchmarkIterations: " + n);
    for (int i = 0; i < n; i++) {
      System.out.println("Iteration: " + i + ", start date: " + Calendar.getInstance().getTime());
      System.out.println(SyntheticG.class.getName() + " starts");
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
