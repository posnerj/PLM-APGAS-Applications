package examples.tspCancelable;

import static apgas.Constructs.asyncAt;
import static apgas.Constructs.at;
import static apgas.Constructs.cancelAllCancelableAsyncAny;
import static apgas.Constructs.cancelableAsyncAny;
import static apgas.Constructs.finish;
import static apgas.Constructs.finishAsyncAny;
import static apgas.Constructs.here;
import static apgas.Constructs.immediateAsyncAt;
import static apgas.Constructs.places;

import apgas.Configuration;
import apgas.Place;
import apgas.impl.Config;
import apgas.impl.Worker;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import examples.tsp.TSPLogger;
import examples.tsp.TSPPartialResult;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by Jonas Posner, 30.11.17 Sources:
 * https://people.eecs.berkeley.edu/~demmel/cs267/assignment4.html
 */
public class StartTSPCancelable {

  private static final Object LOCK = new Object();

  private static final String N_PLACES = "2";
  private static final String N_THREADS = "2";

  private static final String APGAS = "apgas";
  private static final String BEST_SO_FAR = "bestSoFar";

  private static final int MIN_DISTANCE_BETWEEN_CITIES = 1;
  private static final int MAX_DISTANCE_BETWEEN_CITIES = 9;

  // parameters
  private static int nCities = 19;
  private static int[] allCities;
  private static int[][] graph;

  // global partial result
  private static IMap<Integer, TSPPartialResult> iMap;

  private static volatile int result_sumOfWeights;
  private static volatile int result_countResults;
  private static volatile int result_count;
  private static volatile int[] result_partialPath;

  private static int seed = 1337;
  private static int threshold = 10;
  private static boolean useHeuristics = true;
  private static int pathLimit = 0;
  private static boolean cancel = false;
  // for debugging purposes
  private static TSPLogger tspLogger;
  private static boolean log = false;
  private static boolean printGraph = false;

  /**
   * Creates and solves a TSP instance using parallel computing and asynchronous partitioned global
   * address space (APGAS) model.
   *
   * @param args A list of arguments that specify the computing process 1st argument: number of
   *     cities for TSP. 2nd argument: threshold between parallel and sequential 3rd argument:
   *     activate tspLogger for debugging (0/1) 4th argument: use heuristics to improve performance
   *     (0/1) 5th argument: seed for creating TSP
   */
  public static void main(String args[]) {

    if (args.length >= 1) {
      nCities = Integer.parseInt(args[0]);
    }
    if (args.length >= 2) {
      threshold = Integer.parseInt(args[1]);
    }
    if (args.length >= 3) {
      log = Boolean.parseBoolean(args[2]);
    }
    if (args.length >= 4) {
      useHeuristics = Boolean.parseBoolean(args[3]);
    }
    if (args.length >= 5) {
      int s = Integer.parseInt(args[4]);
      if (s > 0) {
        seed = s;
      }
    }
    if (args.length >= 6) {
      int pLimit = Integer.parseInt(args[5]);
      if (pLimit > 0) {
        pathLimit = pLimit;
      }
    }

    if (System.getProperty(Configuration.APGAS_PLACES) == null) {
      System.setProperty(Configuration.APGAS_PLACES, N_PLACES);
    }
    if (System.getProperty(Configuration.APGAS_THREADS) == null) {
      System.setProperty(Configuration.APGAS_THREADS, N_THREADS);
    }
    System.setProperty(Config.APGAS_SERIALIZATION, "java");
    final int numThreads = Integer.parseInt(System.getProperty(Configuration.APGAS_THREADS));

    final int _numberOfCities = nCities;
    final int _seed = seed;
    final int _threshold = threshold;
    final boolean _useHeuristics = useHeuristics;
    final int _pathLimit = pathLimit;

    final int _maxThreads;
    if (log == true) {
      _maxThreads = Integer.getInteger(Config.APGAS_MAX_THREADS, 256);
    } else {
      _maxThreads = 0;
    }

    finish(
        () -> {
          for (Place p : places()) {

            if (p.id == 0) {
              graph = createGraph();
              if (printGraph) {
                printGraph();
              }
              allCities = createAllCities(_numberOfCities);
              tspLogger = new TSPLogger(_maxThreads);
              iMap = Hazelcast.getHazelcastInstanceByName(APGAS).getMap(BEST_SO_FAR);
              continue;
            }

            asyncAt(
                p,
                () -> {
                  allCities = createAllCities(_numberOfCities);
                  tspLogger = new TSPLogger(_maxThreads);
                  iMap = Hazelcast.getHazelcastInstanceByName(APGAS).getMap(BEST_SO_FAR);
                  threshold = _threshold;
                  nCities = _numberOfCities;
                  seed = _seed;
                  useHeuristics = _useHeuristics;
                  pathLimit = _pathLimit;
                  // ATTENTION: has to be called after setting nCities etc
                  graph = createGraph();
                });
          }
        });

    System.out.println(
        "Running " + StartTSPCancelable.class.getName() + " with "
            + places().size()
            + " Places and "
            + numThreads
            + " "
            + "Threads, cities="
            + nCities
            + ", threshold="
            + threshold
            + ", tspLogger="
            + log
            + ", heuristic="
            + useHeuristics
            + ", seed="
            + seed
            + ", pathLimit="
            + pathLimit);

    final long startPar = System.nanoTime();

    ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    executor.scheduleAtFixedRate(
        () -> {
          int tmpSumOfWeights;
          if (places().size() == 1) {
            tmpSumOfWeights = Integer.MAX_VALUE;
          } else {
            tmpSumOfWeights = iMap.get(0).sumOfWeights;
          }

          if (result_sumOfWeights <= pathLimit
              || (places().size() > 1 && tmpSumOfWeights <= pathLimit)) {
            System.out.println("found path limit");
            cancel = true;
            for (Place p : places()) {
              if (p.id == here().id) {
                continue;
              }
              immediateAsyncAt(
                  p,
                  () -> {
                    StartTSPCancelable.cancel = true;
                  });
            }
            cancelAllCancelableAsyncAny();
            return;
          }
        },
        1,
        1,
        TimeUnit.SECONDS);

    nativeBranchAndBoundParallel();

    executor.shutdownNow();
    final long endPar = System.nanoTime();
    final long totalTimePar = endPar - startPar;
    System.out.println("Process time: " + (totalTimePar / 1E9D));

    if (false == log) {
      return;
    }

    // merge logging entries on place zero
    finish(
        () -> {
          for (Place p : places()) {
            if (p.id == 0) {
              continue;
            }
            TSPLogger remoteLogger = at(p, () -> tspLogger);
            tspLogger.add(remoteLogger);
          }
        });
    System.out.println(tspLogger);
  }

  /**
   * Creates an initial result and passes it to all places. Then start the calculation with an empty
   * route.
   */
  private static void nativeBranchAndBoundParallel() {

    int numberOfCities = graph.length;
    int weight = 0;

    // create initial Partial Result
    int initialPartialResult_sumOfWeights = 0;
    int initialPartialResult_countResults = 1;
    int initialPartialResult_count = 0;
    int[] initialPartialResult_partialPath = new int[StartTSPCancelable.allCities.length + 1];

    // if heuristic option is active, create better initial result using nearest neighbor heuristic
    if (useHeuristics) {

      // start with city zero
      int idxCurrentCity = 0;
      HashSet<Integer> citiesVisited = new HashSet<>();
      initialPartialResult_partialPath[initialPartialResult_count] = idxCurrentCity;
      initialPartialResult_count++;
      citiesVisited.add(idxCurrentCity);

      // create path using nearest neighbor heuristic
      for (int i = 0; i < numberOfCities - 1; i++) {

        int idxOfNearestCityNotVisited = idxCurrentCity;
        int lowestDistanceSoFar = MAX_DISTANCE_BETWEEN_CITIES + 1;

        // find nearest not visited
        for (int j = 0; j < numberOfCities; j++) {
          if ((j != idxCurrentCity && !citiesVisited.contains(j))
              && (graph[idxCurrentCity][j] < lowestDistanceSoFar)) {
            lowestDistanceSoFar = graph[idxCurrentCity][j];
            idxOfNearestCityNotVisited = j;

            // use first city when there are multiple cities with minimal distance
            if (lowestDistanceSoFar == MIN_DISTANCE_BETWEEN_CITIES) {
              break;
            }
          }
        }
        idxCurrentCity = idxOfNearestCityNotVisited;
        initialPartialResult_partialPath[initialPartialResult_count] = idxOfNearestCityNotVisited;
        initialPartialResult_count++;
        citiesVisited.add(idxOfNearestCityNotVisited);
      }

      // add last step to path
      initialPartialResult_partialPath[initialPartialResult_count] = 0;
      initialPartialResult_count++;

      // calculate weight of this path
      for (int i = 1; i < initialPartialResult_partialPath.length; i++) {
        int startingPoint = initialPartialResult_partialPath[i - 1];
        int endPoint = initialPartialResult_partialPath[i];
        weight += graph[startingPoint][endPoint];
      }
      initialPartialResult_sumOfWeights = weight;

    } else {

      // heuristic is not active...
      // create path: 0 -> 1 -> 2 -> 3 -> ... -> n-1 -> n -> 0
      for (int i = 0; i < numberOfCities; i++) {
        initialPartialResult_partialPath[initialPartialResult_count] = i;
        initialPartialResult_count++;
      }
      initialPartialResult_partialPath[initialPartialResult_count] = 0;
      initialPartialResult_count++;

      // calculate length of this path
      weight = graph[numberOfCities - 1][0]; // weight for path from end to start
      for (int i = 1; i < numberOfCities; i++) {
        weight += graph[i - 1][i];
      }
      initialPartialResult_sumOfWeights = weight;
    }

    // create TSPPartialResult object and put it in the hazelcast Map
    TSPPartialResult bestSolutionSoFar = new TSPPartialResult(nCities);
    bestSolutionSoFar.count = initialPartialResult_count;
    bestSolutionSoFar.sumOfWeights = weight;
    System.arraycopy(
        initialPartialResult_partialPath,
        0,
        bestSolutionSoFar.partialPath,
        0,
        initialPartialResult_partialPath.length);
    iMap.set(0, bestSolutionSoFar);

    // save initial result in constants for lambda expression
    final int _initialPartialResult_sumOfWeights = initialPartialResult_sumOfWeights;
    final int _initialPartialResult_count = initialPartialResult_count;
    final int _initialPartialResult_countResults = initialPartialResult_countResults;
    final int[] _initialPartialResult_partialPath =
        Arrays.copyOf(initialPartialResult_partialPath, initialPartialResult_partialPath.length);

    // distribute initial result to all places
    finish(
        () -> {
          for (Place p : places()) {
            asyncAt(
                p,
                () -> {
                  result_sumOfWeights = _initialPartialResult_sumOfWeights;
                  result_count = _initialPartialResult_count;
                  result_countResults = _initialPartialResult_countResults;
                  result_partialPath = _initialPartialResult_partialPath;
                });
          }
        });

    // Print info about initial result
    System.out.println("Initial result path: " + Arrays.toString(initialPartialResult_partialPath));
    System.out.println("Initial result path length: " + initialPartialResult_sumOfWeights);

    // create start values
    int startPartialResult_sumOfWeights = 0;
    int startPartialResult_count = 1;
    int[] startPartialResult_partialPath = new int[StartTSPCancelable.allCities.length + 1];

    // initialize start values
    startPartialResult_partialPath[0] = 0;

    // wait until all spawn tasks are processed
    finishAsyncAny(
        () -> {
          tspLogger.incrementAsyncAny(((Worker) Thread.currentThread()).getMyID());
          cancelableAsyncAny(
              () -> {
                nbabPar(
                    startPartialResult_partialPath,
                    startPartialResult_sumOfWeights,
                    startPartialResult_count);
              });
        });

    // print final result
    System.out.println(iMap.get(0));
  }

  /**
   * Compute the shortest possible route that visits each city exactly once and returns to the
   * origin city.
   *
   * @param partialResult_partialPath a list of all cities that are already visited
   * @param partialResult_sumOfWeights the weight of this route at the given moment
   * @param partialResult_count number of cites that were visited
   */
  private static void nbabPar(
      int[] partialResult_partialPath, int partialResult_sumOfWeights, int partialResult_count) {

    final int myID = ((Worker) Thread.currentThread()).getMyID();
    tspLogger.incrementCalls(myID); // expensive call

    if (cancel == true) {
      return;
    }

    final int numberOfCitiesInPath = partialResult_count;

    // current path is no feasible result
    if (numberOfCitiesInPath < nCities) {
      // generate new paths
      int[] ac = Arrays.copyOf(allCities, allCities.length);
      int[] citiesNotVisited = new int[allCities.length - numberOfCitiesInPath];

      for (int i = 0; i < numberOfCitiesInPath; i++) {
        ac[partialResult_partialPath[i]] = -1;
      }

      int j = 0;
      for (int i = 0; i < ac.length; i++) {
        if (ac[i] != -1) {
          citiesNotVisited[j] = ac[i];
          j++;
        }
      }

      if (citiesNotVisited.length > threshold) {
        for (int i = 0; i < citiesNotVisited.length; i++) {
          int city = citiesNotVisited[i];

          int lastCityVisited = partialResult_partialPath[partialResult_count - 1];
          int newWeight = partialResult_sumOfWeights + graph[lastCityVisited][city];

          boolean pruningConditionMet = false;
          int tmpSum = StartTSPCancelable.result_sumOfWeights;
          if ((newWeight + ((nCities + 1 - numberOfCitiesInPath - 1) * MIN_DISTANCE_BETWEEN_CITIES))
              > tmpSum) {
            pruningConditionMet = true;
          }

          // prune tree
          if (newWeight >= StartTSPCancelable.result_sumOfWeights
              || (useHeuristics && pruningConditionMet)) {
            tspLogger.incrementCuts(myID); // expensive call
            tspLogger.addToProcessedNodes(myID, partialResult_count);
            continue;
          } else {
            // visit current city next -- build new parameters
            int newPR_sumOfWeights = newWeight;
            int newPR_count = partialResult_count;
            int[] newPR_partialPath =
                Arrays.copyOf(partialResult_partialPath, partialResult_partialPath.length);
            newPR_partialPath[newPR_count] = city;
            newPR_count++;

            final int _newPR_count = newPR_count;
            tspLogger.incrementAsyncAny(myID);

            // generate new tours on a place
            cancelableAsyncAny(
                () -> {
                  nbabPar(newPR_partialPath, newPR_sumOfWeights, _newPR_count);
                });
          }
        }
        // threshold is met...
      } else {
        for (int i = 0; i < citiesNotVisited.length; i++) {
          int city = citiesNotVisited[i];

          int lastCityVisited = partialResult_partialPath[partialResult_count - 1];
          int newWeight = partialResult_sumOfWeights + graph[lastCityVisited][city];

          boolean pruningConditionMet = false;
          int tmpSum = StartTSPCancelable.result_sumOfWeights;
          if ((newWeight + ((nCities + 1 - numberOfCitiesInPath - 1) * MIN_DISTANCE_BETWEEN_CITIES))
              > tmpSum) {
            pruningConditionMet = true;
          }
          // prune
          if (newWeight >= result_sumOfWeights || (useHeuristics && pruningConditionMet)) {
            tspLogger.incrementCuts(myID); // expensive call
            tspLogger.addToProcessedNodes(myID, partialResult_count);
            continue;
          } else {
            // visit current city next -- build new parameters
            int newPR_sumOfWeights = newWeight;
            int newPR_count = partialResult_count;
            int[] newPR_partialPath =
                Arrays.copyOf(partialResult_partialPath, partialResult_partialPath.length);
            newPR_partialPath[newPR_count] = city;
            newPR_count++;

            tspLogger.incrementRecursive(myID); // expensive call

            // generate new tours on this place
            nbabPar(newPR_partialPath, newPR_sumOfWeights, newPR_count);
          }
        }
      }
      // all cities were visited...
    } else if (numberOfCitiesInPath == nCities) {
      // calculate final weight of this tour (including connection from end to starting point)
      int lastVisitedCity = partialResult_partialPath[partialResult_count - 1];
      int firstVisitedCity = partialResult_partialPath[0];

      int newWeight = partialResult_sumOfWeights + graph[lastVisitedCity][firstVisitedCity];

      // if result is better than all results before
      if (newWeight < result_sumOfWeights) {
        tspLogger.addToProcessedNodes(myID, partialResult_count);

        // visit current city next -- build new parameters
        int newBest_sumOfWeights;
        int newBest_countResults = 1;
        int newBest_count;
        int[] newBest_partialPath;
        int[] finalPath = new int[partialResult_partialPath.length];
        System.arraycopy(
            partialResult_partialPath, 0, finalPath, 0, partialResult_partialPath.length);
        finalPath[partialResult_count] = finalPath[0];
        newBest_partialPath = finalPath;
        newBest_sumOfWeights = newWeight;
        newBest_count = partialResult_count + 1;

        boolean foundNewResult = false;
        synchronized (LOCK) {
          if (newWeight < result_sumOfWeights) {
            result_sumOfWeights = newBest_sumOfWeights;
            result_partialPath = Arrays.copyOf(newBest_partialPath, newBest_partialPath.length);
            result_countResults = newBest_countResults;
            result_count = newBest_count;
            foundNewResult = true;
          }
        }

        if (false == foundNewResult) {
          return;
        }

        tspLogger.incrementFoundResultsLocal(myID);

        // create new Partial Result
        TSPPartialResult newBest = new TSPPartialResult(nCities);
        newBest.count = newBest_count;
        newBest.sumOfWeights = newBest_sumOfWeights;
        newBest.countResults = newBest_countResults;
        System.arraycopy(
            newBest_partialPath, 0, newBest.partialPath, 0, newBest_partialPath.length);

        // change value of bestSoFar in global Map
        foundNewResult =
            (Boolean)
                iMap.executeOnKey(
                    0,
                    new EntryProcessor() {
                      @Override
                      public Object process(Map.Entry entry) {
                        TSPPartialResult last = (TSPPartialResult) entry.getValue();
                        if (newWeight < last.sumOfWeights) {
                          entry.setValue(newBest);
                          return true;
                        } else if (newWeight == last.sumOfWeights) {
                          last.countResults++;
                        }
                        return false;
                      }

                      @Override
                      public EntryBackupProcessor getBackupProcessor() {
                        return this::process;
                      }
                    });

        if (true == foundNewResult) {

          tspLogger.incrementFoundResultsGlobal(myID);

          final TSPPartialResult _result = newBest;
          for (Place p : places()) {
            if (p.id == here().id) {
              continue;
            }
            immediateAsyncAt(
                p,
                () -> {
                  synchronized (LOCK) {
                    if (_result.sumOfWeights < result_sumOfWeights) {
                      result_sumOfWeights = _result.sumOfWeights;
                      result_count = _result.count;
                      result_countResults = _result.countResults;
                      System.arraycopy(
                          _result.partialPath,
                          0,
                          result_partialPath,
                          0,
                          _result.partialPath.length);
                    }
                  }
                });
          }
        }
      }
    }
  }

  /**
   * Create an adjacency matrix that represents the road network. Taking in account the
   * MIN_DISTANCE_BETWEEN_CITIES and the MAX_DISTANCE_BETWEEN_CITIES as well as the seed.
   *
   * @return adjacency matrix that represents the road network
   */
  private static int[][] createGraph() {
    int[][] graph = new int[nCities][nCities];
    Random generator = new Random(seed);

    for (int i = 0; i < nCities; i++) {
      for (int j = 0; j < nCities; j++) {
        if (i == j) {
          graph[i][j] = 0;
        } else if (i < j) {
          int distance =
              generator.nextInt(MAX_DISTANCE_BETWEEN_CITIES) + MIN_DISTANCE_BETWEEN_CITIES;
          graph[i][j] = distance;
          graph[j][i] = distance;
        }
      }
    }
    return graph;
  }

  /** Print the adjacency matrix that represents the road network */
  private static void printGraph() {
    for (int i = 0; i < nCities; i++) {
      for (int j = 0; j < nCities; j++) {
        System.out.print(graph[i][j] + " ");
      }
      System.out.println();
    }
  }

  /**
   * Creates an Integer Array which contains all numbers form zero to numberOfCities - 1.
   *
   * @param numberOfCities an integer that specify the city count
   * @return integer array with numberOfCities entries
   */
  private static int[] createAllCities(int numberOfCities) {
    int[] cities = new int[numberOfCities];
    for (int i = 0; i < numberOfCities; i++) {
      cities[i] = i;
    }
    return cities;
  }
}
