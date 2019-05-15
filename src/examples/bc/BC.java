/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package examples.bc;

import apgas.impl.GlobalRuntimeImpl;
import apgas.impl.Worker;
import java.io.Serializable;
import java.util.Arrays;
import utils.FixedRailQueueInt;
import utils.Graph;
import utils.Rmat;
import utils.X10Random;

public class BC implements Serializable {

  private static final long serialVersionUID = 1L;

  private static int[] verticesToWorkOn;
  private static Graph graph;
  private static int N;
  private static BC[] threadStorage;
  private final FixedRailQueueInt regularQueue;
  private final int[] predecessorCount;
  private final long[] distanceMap;
  private final long[] sigmaMap;
  private final double[] deltaMap;
  // result
  private double[] betweennessMap;
  // These are the per-vertex data structures.
  private final int[] predecessorMap;

  // Constructor
  public BC(int permute) {
    betweennessMap = new double[N];

    Arrays.fill(betweennessMap, 0.0);
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

  public static synchronized void initGraph(Rmat rmat, int permut, int threadCount) {
    if (graph == null) {
      graph = rmat.generate();
      graph.compress();
      N = graph.numVertices();
      verticesToWorkOn = new int[N];
      for (int idx = 0; idx < N; ++idx) {
        verticesToWorkOn[idx] = idx;
      }
      threadStorage = new BC[threadCount];
      for (int i = 0; i < threadCount; i++) {
        threadStorage[i] = new BC(permut);
      }
    }
  }

  /** substring helper function */
  private static String sub(String str, int start, int end) {
    return (str.substring(start, Math.min(end, str.length())));
  }

  public static void compute(final int vertexIndexFrom, final int vertexIndexTo) {
    Worker worker = (Worker) Thread.currentThread();
    final int pos = worker.getMyID();
    BC bc = threadStorage[pos];

    BCResult bcResult = (BCResult) GlobalRuntimeImpl.getRuntime().getResult()[pos];
    if (bcResult == null) {
      bcResult = new BCResult(N);
      GlobalRuntimeImpl.getRuntime().getResult()[pos] = bcResult;
    }
    bc.betweennessMap = bcResult.getResult().internalResult;

    for (int i = vertexIndexFrom; i < vertexIndexTo; i++) {
      bc.bfsShortestPath(i);
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

  /** A function to shuffle the vertices randomly to give better work dist. */
  private static void permuteVertices() {
    X10Random prng = new X10Random(1);

    for (int i = 0; i < N; ++i) {
      int indexToPick = prng.nextInt(N - i);
      int v = verticesToWorkOn[i];
      verticesToWorkOn[i] = verticesToWorkOn[i + indexToPick];
      verticesToWorkOn[i + indexToPick] = v;
    }
  }

  private void bfsShortestPath1(int s) {
    // Put the values for source vertex
    distanceMap[s] = 0L;
    sigmaMap[s] = 1L;
    regularQueue.push(s);
  }

  private void bfsShortestPath2() {
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
    deltaMap[w] = 0;
  }

  private void bfsShortestPath(final int vertexIndex) {
    final int s = verticesToWorkOn[vertexIndex];
    bfsShortestPath1(s);
    while (!regularQueue.isEmpty()) {
      bfsShortestPath2();
    }
    bfsShortestPath3();
    while (!regularQueue.isEmpty()) {
      bfsShortestPath4(s);
    }
  }
}
