package GLBCoop.examples.BC;

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
import utils.X10Random;

public class BC implements Serializable {

  public transient int N;

  public transient int M;

  public transient int[] verticesToWorkOn;

  public long count = 0; // for test

  public transient long refTime = 0;

  public transient double accTime = 0;

  public transient FixedRailQueueInt regularQueue;

  public double[] realBetweennessMap;

  transient Graph graph;

  // These are the per-vertex data structures.
  transient int[] predecessorMap;

  transient int[] predecessorCount;

  transient long[] distanceMap;

  transient long[] sigmaMap;

  transient double[] deltaMap;

  public BC(Rmat rmat, int permute) {
    this.graph = rmat.generate();
    //    System.out.println(graph);
    this.graph.compress();
    this.N = this.graph.numVertices();
    System.out.println("numVertices: " + this.N);
    this.M = this.graph.numEdges();
    System.out.println("numEdges: " + this.M);
    this.verticesToWorkOn = new int[N];
    Arrays.setAll(this.verticesToWorkOn, i -> i); // i is the array index
    if (permute > 0) {
      this.permuteVertices();
    }

    this.realBetweennessMap = new double[this.N];
    Arrays.fill(this.realBetweennessMap, 0.0d);

    this.predecessorMap = new int[this.graph.numEdges()];
    this.predecessorCount = new int[this.N];
    this.distanceMap = new long[N];
    Arrays.setAll(this.distanceMap, i -> Long.MAX_VALUE); // i is the array index
    this.sigmaMap = new long[this.N];
    this.regularQueue = new FixedRailQueueInt(this.N);
    this.deltaMap = new double[this.N];
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
      int n = Integer.parseInt(cmd.getOptionValue("n", "14"));
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
      double setupTime = (System.nanoTime() - time) / 1e9;

      time = System.nanoTime();
      for (int i = 0; i < bc.N; ++i) {
        bc.bfsShortestPath(i);
      }
      double procTime = (System.nanoTime() - time) / 1e9;

      if (verbose > 0) {
        System.out.println("Time = " + bc.accTime + " Count = " + bc.count);
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

    for (int i = 0; i < this.N; i++) {
      int indexToPick = prng.nextInt(this.N - i);
      int v = this.verticesToWorkOn[i];
      this.verticesToWorkOn[i] = this.verticesToWorkOn[i + indexToPick];
      this.verticesToWorkOn[i + indexToPick] = v;
    }
  }

  /** Dump the betweenness map. */
  public void printBetweennessMap() {
    for (int i = 0; i < this.N; i++) {
      if (this.realBetweennessMap[i] != 0.0) {
        System.out.println("(" + i + ") -> " + this.realBetweennessMap[i]);
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
      if (this.realBetweennessMap[i] != 0.0) {
        System.out.println("(" + i + ") -> " + sub("" + this.realBetweennessMap[i], 0, numDigit));
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

    //    if (edgeStart >= edgeEnd) {
    //      count++;// TODO NEW
    //    }

    // Iterate over all its neighbors
    for (int wIndex = edgeStart; wIndex < edgeEnd; ++wIndex) {
      //      count++; // TODO NEW

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
    //    if (predecessorCount[w] <= 0) {
    //      count++; // TODO NEW
    //    }
    while (predecessorCount[w] > 0) {
      //      count++; // TODO NEW
      final int v = predecessorMap[rev + (--predecessorCount[w])];
      deltaMap[v] += (((double) sigmaMap[v]) / sigmaMap[w]) * (1.0 + deltaMap[w]);
    }

    // Accumulate updates locally
    if (w != s) {
      // old
      // betweennessMap[w] += deltaMap[w];
      this.realBetweennessMap[w] += this.deltaMap[w];
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
