package GLBCoord.examples.BC;

import static apgas.Constructs.here;

import java.io.Serializable;
import java.util.Arrays;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import utils.FixedRailQueueInt;
import utils.Graph;
import utils.Rmat;
import utils.SplitQueue;
import utils.X10Random;

public class BC extends SplitQueue<Integer> implements Serializable {

  private static final long serialVersionUID = 1L;
  public final int N;
  public final int M;
  public final int[] verticesToWorkOn;
  public final Double[] betweennessMap;
  public final FixedRailQueueInt regularQueue;
  final int[] predecessorCount;
  final long[] distanceMap;
  final long[] sigmaMap;
  final double[] deltaMap;
  public long count = 0;
  public long refTime = 0;
  public long accTime = 0;

  // These are the per-vertex data structures.
  int[] predecessorMap;
  private Graph graph;

  public BC(Rmat rmat, int permute) {
    super(Integer.class);
    graph = rmat.generate();
    graph.compress();
    N = graph.numVertices();
    M = graph.numEdges();
    verticesToWorkOn = new int[N];
    betweennessMap = new Double[N];
    for (int idx = 0; idx < verticesToWorkOn.length; ++idx) {
      verticesToWorkOn[idx] = idx;
      betweennessMap[idx] = 0.0;
    }
    if (permute > 0) {
      permuteVertices();
    }

    predecessorMap = new int[graph.numEdges()];
    predecessorCount = new int[N];
    distanceMap = new long[N];
    Arrays.fill(distanceMap, Long.MAX_VALUE);
    sigmaMap = new long[N];
    regularQueue = new FixedRailQueueInt(N);
    deltaMap = new double[N];
  }

  public BC(Rmat rmat, int permute, double split) {
    super(split, Integer.class);
    graph = rmat.generate();
    graph.compress();
    N = graph.numVertices();
    M = graph.numEdges();
    verticesToWorkOn = new int[N];
    betweennessMap = new Double[N];
    for (int idx = 0; idx < verticesToWorkOn.length; ++idx) {
      verticesToWorkOn[idx] = idx;
      betweennessMap[idx] = 0.0;
    }
    if (permute > 0) {
      permuteVertices();
    }

    predecessorMap = new int[graph.numEdges()];
    predecessorCount = new int[N];
    distanceMap = new long[N];
    Arrays.fill(distanceMap, Long.MAX_VALUE);
    sigmaMap = new long[N];
    regularQueue = new FixedRailQueueInt(N);
    deltaMap = new double[N];
  }

  /** substring helper function */
  public static String sub(String str, int start, int end) {
    return (str.substring(start, Math.min(end, str.length())));
  }

  /** Reads in all the options and calculate betweenness. */
  public static void main(String... args) {

    Options options = new Options();
    options.addOption("timestamps", true, "Seed for the random number");
    options.addOption("n", true, "Number of vertices = 2^n");
    options.addOption("a", true, "Probability a");
    options.addOption("b", true, "Probability b");
    options.addOption("c", true, "Probability c");
    options.addOption("d", true, "Probability d");
    options.addOption("p", true, "Permutation");
    options.addOption("v", true, "Verbose");

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;
    try {
      cmd = parser.parse(options, args);
      int seed = Integer.parseInt(cmd.getOptionValue("timestamps", "2"));
      int n = Integer.parseInt(cmd.getOptionValue("n", "2"));
      double a = Double.parseDouble(cmd.getOptionValue("a", "0.55"));
      double b = Double.parseDouble(cmd.getOptionValue("b", "0.1"));
      double c = Double.parseDouble(cmd.getOptionValue("c", "0.1"));
      double d = Double.parseDouble(cmd.getOptionValue("d", "0.25"));
      int permute = Integer.parseInt(cmd.getOptionValue("p", "1")); // on by default
      int verbose = Integer.parseInt(cmd.getOptionValue("v", "15"));

      System.out.println("Running BC with the following parameters:");
      System.out.println("seed = " + seed);
      System.out.println("N = " + (1 << n));
      System.out.println("a = " + a);
      System.out.println("b = " + b);
      System.out.println("c = " + c);
      System.out.println("d = " + d);
      System.out.println("Seq ");

      long time = System.nanoTime();
      BC bc = new BC(new Rmat(seed, n, a, b, c, d), permute);
      for (int i = 0; i < bc.N; ++i) {
        bc.pushPrivate(i);
      }
      double setupTime = (System.nanoTime() - time) / 1e9;

      time = System.nanoTime();

      while (bc.size() > 0) {
        bc.bfsShortestPath(bc.popPrivate());
      }

      double procTime = (System.nanoTime() - time) / 1e9;

      if (verbose > 0) {
        System.out.println(
            "[" + here().id + "]" + " Time = " + bc.accTime + " Count = " + bc.count);
      }

      if (verbose > 2) {
        bc.printBetweennessMap(6);
      }

      System.out.println(
          "Seq N: "
              + bc.N
              + "  Setup: "
              + setupTime
              + "timestamps  Processing: "
              + procTime
              + "timestamps");
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  /** A function to shuffle the vertices randomly to give better work dist. */
  private void permuteVertices() {
    X10Random prng = new X10Random(1);

    for (int i = 0; i < N; ++i) {
      int indexToPick = prng.nextInt(N - i);
      int v = verticesToWorkOn[i];
      verticesToWorkOn[i] = verticesToWorkOn[i + indexToPick];
      verticesToWorkOn[i + indexToPick] = v;
    }
  }

  /** Dump the betweenness map. */
  public void printBetweennessMap() {
    for (int i = 0; i < N; ++i) {
      if (betweennessMap[i] != 0.0) {
        System.out.println("(" + i + ") -> " + betweennessMap[i]);
      }
    }
  }

  /**
   * Dump the betweenness map.
   *
   * @param numDigit number of digits to println
   */
  public final void printBetweennessMap(final int numDigit) {
    for (int i = 0; i < N; ++i) {
      if (betweennessMap[i] != 0.0) {
        System.out.println("(" + i + ") -> " + sub("" + betweennessMap[i], 0, numDigit));
      }
    }
  }

  protected final void bfsShortestPath1(int s) {
    // Put the values for source vertex
    distanceMap[s] = 0L;
    sigmaMap[s] = 1L;
    regularQueue.push(s);
  }

  protected final void bfsShortestPath2() {
    count++;
    // Pop the node with the least distance
    final int v = regularQueue.pop();

    // Get the start and the end points for the edge list for "v"
    final int edgeStart = graph.begin(v);
    final int edgeEnd = graph.end(v);

    // Iterate over all its neighbors
    for (int wIndex = edgeStart; wIndex < edgeEnd; ++wIndex) {
      // Get the target of the current edge.
      final int w = graph.getAdjacentVertexFromIndex(wIndex);
      final long distanceThroughV = distanceMap[v] + 1L;

      // In BFS, the minimum distance will only be found once --- the
      // first time that a node is discovered. So, add it to the queue.
      if (distanceMap[w] == Long.MAX_VALUE) {
        regularQueue.push(w);
        distanceMap[w] = distanceThroughV;
      }

      // If the distance through "v" for "w" from "timestamps" was the same as its
      // current distance, we found another shortest path. So, add
      // "v" to predecessorMap of "w" and update other maps.
      if (distanceThroughV == distanceMap[w]) {
        sigmaMap[w] = sigmaMap[w] + sigmaMap[v]; // XTENLANG-2027
        predecessorMap[graph.rev(w) + predecessorCount[w]++] = v;
      }
    }
  }

  protected final void bfsShortestPath3() {
    regularQueue.rewind();
  }

  protected final void bfsShortestPath4(int s) {
    final int w = regularQueue.top();
    final int rev = graph.rev(w);
    while (predecessorCount[w] > 0) {
      final int v = predecessorMap[rev + (--predecessorCount[w])];
      deltaMap[v] += (((double) sigmaMap[v]) / sigmaMap[w]) * (1.0 + deltaMap[w]);
    }

    // Accumulate updates locally
    if (w != s) {
      betweennessMap[w] += deltaMap[w];
    }
    distanceMap[w] = Long.MAX_VALUE;
    sigmaMap[w] = 0L;
    deltaMap[w] = 0.0;
  }

  protected final void bfsShortestPath(final int vertexIndex) {
    refTime = System.nanoTime();
    final int s = verticesToWorkOn[vertexIndex];
    bfsShortestPath1(s);
    while (!regularQueue.isEmpty()) {
      bfsShortestPath2();
    }
    bfsShortestPath3();
    while (!regularQueue.isEmpty()) {
      bfsShortestPath4(s);
    }
    accTime += (System.nanoTime() - refTime) / 1e9;
  }
}
