package GLBCoopGR.examples.NQueens;

import static apgas.Constructs.places;

import GLBCoopGR.GLBCoopGR;
import GLBCoopGR.GLBParametersGR;
import apgas.Configuration;
import apgas.SerializableCallable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class NQueensG {

  static final int COUNT_PLACES = 4;
  static final int COUNT_THREADS = 16;

  public static Long[] compute(String[] args) throws ParseException {
    Options options = new Options();

    options.addOption("s", true, "Size of NQueens");
    options.addOption("t", true, "Threshold");
    options.addOption("n", true, "Number of nodes to process before probing. Default 511.");
    options.addOption("w", true, "Number of thieves to send out. Default 1.");
    options.addOption("l", true, "Base of the lifeline");
    options.addOption("m", true, "Max potential victims");
    options.addOption("v", true, "Verbose. Default 0 (no).");
    options.addOption(
        "timestamps",
        true,
        "count of timestamps for logging, 0=disabled, 500 = recommanded, default ist 0");

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    int s = Integer.parseInt(cmd.getOptionValue("s", "13"));
    int t = Integer.parseInt(cmd.getOptionValue("t", "10"));

    int n = Integer.parseInt(cmd.getOptionValue("n", "511"));
    int l = Integer.parseInt(cmd.getOptionValue("l", "32"));
    int m = Integer.parseInt(cmd.getOptionValue("m", "1024"));
    int timestamps = Integer.parseInt(cmd.getOptionValue("timestamps", "0"));

    if (System.getProperty(Configuration.APGAS_PLACES) == null) {
      System.setProperty(Configuration.APGAS_PLACES, String.valueOf(COUNT_PLACES));
    }

    String sysThreads = System.getProperty(Configuration.APGAS_THREADS);
    if (sysThreads == null || Integer.parseInt(sysThreads) < COUNT_THREADS) {
      System.setProperty(Configuration.APGAS_THREADS, String.valueOf(COUNT_THREADS));
    }
    sysThreads = System.getProperty(Configuration.APGAS_THREADS);

    int verbose =
        Integer.parseInt(cmd.getOptionValue("v", String.valueOf(GLBParametersGR.SHOW_RESULT_FLAG)));
    int numPlaces = places().size();

    int z0 = 1;
    int zz = l;
    while (zz < numPlaces) {
      z0++;
      zz *= l;
      System.out.println("calculating zz...");
    }

    int z = z0;
    int w = Integer.parseInt(cmd.getOptionValue("w", String.valueOf(z)));

    System.out.println(
        "places = "
            + numPlaces
            + "   s = "
            + s
            + "   t = "
            + t
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
            + "   v = "
            + verbose
            + "   timestamps = "
            + timestamps
            + "   sysThreads = "
            + sysThreads);


    SerializableCallable<QueueGR> init = () -> new QueueGR(s, t);

    GLBParametersGR glbPara = new GLBParametersGR(n, w, l, z, m, verbose, timestamps, numPlaces);
    GLBCoopGR<QueueGR, Long> glb = new GLBCoopGR<QueueGR, Long>(init, glbPara, true);

    Runnable start = () -> glb.getTaskQueue().init();

    Long[] result = glb.run(start);
    return result;
  }

  public static void main(String[] args) {
    System.out.println(NQueensG.class.getName() + " starts");
    Long[] result = new Long[0];
    try {
      result = compute(args);
    } catch (ParseException e) {
      e.printStackTrace();
    }

    System.out.println("Result of run is: " + result[0]);
  }
}
