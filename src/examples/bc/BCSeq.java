/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package examples.bc;

import java.util.Arrays;
import utils.FixedRailQueue;
import utils.Graph;
import utils.Rmat;
import utils.X10Random;

/** Created by jonas on 02.04.17. */
public class BCSeq {

  private final int N;
  private final int[] verticesToWorkOn;
  private final Double[] betweennessMap;
  private final FixedRailQueue<Integer> regularQueue;
  private final int[] predecessorCount;
  private final long[] distanceMap;
  private final long[] sigmaMap;
  private final double[] deltaMap;
  private long count = 0;
  private long accTime = 0;

  // These are the per-vertex data structures.
  private final int[] predecessorMap;
  private final Graph graph;

  // Constructor
  public BCSeq(Rmat rmat, int permute) {
    graph = rmat.generate();
    graph.compress();
    N = graph.numVertices();
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
    regularQueue = new FixedRailQueue<>(N, Integer.class);
    deltaMap = new double[N];
  }

  /** substring helper function */
  private static String sub(String str, int start, int end) {
    return (str.substring(start, Math.min(end, str.length())));
  }

  /** Reads in all the options and calculate betweenness. */
  public static void main(String... args) {

    int seed = 2;
    int n = 13;
    double a = 0.55;
    double b = 0.1;
    double c = 0.1;
    double d = 0.25;
    int permute = 1;
    int verbose = 15;

    System.out.println("Running BC with the following parameters:");
    System.out.println("seed = " + seed);
    System.out.println("N = " + (1 << n));
    System.out.println("a = " + a);
    System.out.println("b = " + b);
    System.out.println("c = " + c);
    System.out.println("d = " + d);
    System.out.println("Seq ");

    long time = System.nanoTime();
    BCSeq bc = new BCSeq(new Rmat(seed, n, a, b, c, d), permute);
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
  private void printBetweennessMap(final int numDigit) {
    for (int i = 0; i < N; ++i) {
      if (betweennessMap[i] != 0.0) {
        System.out.println("(" + i + ") -> " + sub("" + betweennessMap[i], 0, numDigit));
      }
    }
  }

  private void bfsShortestPath1(int s) {
    // Put the values for source vertex
    distanceMap[s] = 0L;
    sigmaMap[s] = 1L;
    regularQueue.push(s);
  }

  private void bfsShortestPath2() {
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

  private void bfsShortestPath3() {
    regularQueue.rewind();
  }

  private void bfsShortestPath4(int s) {
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

  private void bfsShortestPath(final int vertexIndex) {
    long refTime = System.nanoTime();
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
