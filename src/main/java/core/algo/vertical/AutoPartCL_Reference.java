package core.algo.vertical;

import java.util.ArrayList;

import core.utils.CollectionUtils;
import core.utils.PartitioningUtils;
import db.schema.BenchmarkTables;
import db.schema.utils.WorkloadUtils;
import experiments.AlgorithmResults;
import experiments.AlgorithmRunner;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.jocl.Pointer;

/**
 * Implementation of the AutoPart vertical partitioning algorithm from S.
 * Papadomanolakis and A. Ailamaki, SSDBM '04.
 * 
 * @author Endre Palatinus
 * 
 */
public class AutoPartCL_Reference extends AbstractPartitionsAlgorithm {

	/**
	 * The amount of storage available for attribute replication expressed as a
	 * factor of increase in storage requirements.
	 */
	private double replicationFactor = 0.5;

	/** The minimal number of queries that should access a candidate fragment. */
	private int queryExtentThreshold = 1;

	public AutoPartCL_Reference(AlgorithmConfig config) {
		super(config);
		type = Algo.AUTOPARTCL;
	}
	
	private HashSet<TIntHashSet> part2InternalLoop (HashSet<TIntHashSet> selectedFragments_prev, HashSet<TIntHashSet> atomicFragments, boolean k) {
		HashSet<TIntHashSet> candidateFragments = new HashSet<>();
		
		/* Convert atomicFragments to small array
		 * Convert selectedFragments_prev to Big array
		 * Pass to the kernel a flag k>1
		 * In our kernel we combine each item in the big array with one in the atomicFragments
		 * Test the results for queryExtentThreshold. If it passes, return the fragments into a candidate array.
		 * If k>1, combine the selected fragment with all other and if it pass add the fragments to the candidateFragments*/
		
		for (TIntHashSet CF : selectedFragments_prev) { //This part can be parallelized.
			
			// with atomic fragments
			for (TIntHashSet AF : atomicFragments) { 
				TIntHashSet fragment = new TIntHashSet(CF);
				fragment.addAll(AF);
				
				System.out.println("Fragment Hashset Elements: " + fragment + "\n");

				if (queryExtent(fragment) >= queryExtentThreshold) {
					candidateFragments.add(fragment);
				}
				List <Integer> smallarray = new ArrayList <Integer> ();
				 
		       for (Integer atomicFrag: smallarray) {
		            System.out.println("Smallarray:- " + atomicFrag);
		        }
		      }
			
			if (k) {
				for (TIntHashSet F : selectedFragments_prev) {
					TIntHashSet fragment = new TIntHashSet(CF);
					fragment.addAll(F);
					if (queryExtent(fragment) >= queryExtentThreshold) {
						candidateFragments.add(fragment);
					}
				}
			}

		}

		return candidateFragments;
	}

	@Override
	public void doPartition()  {

        TIntHashSet unReferenced = WorkloadUtils.getNonReferencedAttributes(w.usageMatrix);
        HashSet<TIntHashSet> unRefHashSet = new HashSet<TIntHashSet>();
        unRefHashSet.add(unReferenced);
        int unReferencedSize = getOverlappingPartitionsSize(unRefHashSet);

		/* Atomic fragment selection. */

		HashSet<TIntHashSet> atomicFragments = new HashSet<TIntHashSet>();
		HashSet<TIntHashSet> newFragments = new HashSet<TIntHashSet>();
		HashSet<TIntHashSet> toBeRemovedFragments = new HashSet<TIntHashSet>();
		
		/*Part 1
		  Here we iterate over all queries and for each we get the attributes that the query uses, as a set called QueryExtent.
		  Next, for each previously considered atomic fragments, we create as a set the intersection of attributes with the current query, and the remainders are also added to a new fragments object.
		  */ 
		long init = System.nanoTime();
		for (int q = 0; q < w.queryCount; q++) {
			TIntHashSet queryExtent = new TIntHashSet(w.attributeCount);

			for (int a = 0; a < w.attributeCount; a++) {
				if (w.usageMatrix[q][a] == 1) {
					queryExtent.add(a);
				}
			}

			newFragments.clear();
			toBeRemovedFragments.clear();

			for (TIntHashSet fragment : atomicFragments) {

				TIntHashSet intersection = new TIntHashSet(queryExtent);
				intersection.retainAll(fragment);

				if (!intersection.isEmpty()) {

					toBeRemovedFragments.add(fragment);
					TIntHashSet remainder = new TIntHashSet(fragment);
					remainder.removeAll(intersection);

					if (!remainder.isEmpty()) {
						newFragments.add(remainder);
					}

					if (!intersection.isEmpty()) {
						newFragments.add(intersection);
					}

					queryExtent.removeAll(intersection);

					if (queryExtent.isEmpty()) {
						break;
					}
				}

			}

			if (!queryExtent.isEmpty()) {
				newFragments.add(queryExtent);
			}

			atomicFragments.removeAll(toBeRemovedFragments);
			atomicFragments.addAll(newFragments);
		}
		long now = System.nanoTime();

		
		/* Iteration phase */

		/* The partitions in the current solution. */
		HashSet<TIntHashSet> presentSolution = CollectionUtils.deepClone(atomicFragments);
		/*
		 * The fragments selected for inclusion into the solution in the
		 * previous iteration.
		 */
		HashSet<TIntHashSet> selectedFragments_prev = new HashSet<TIntHashSet>();
		/*
		 * The fragments selected for inclusion into the solution in the current
		 * iteration.
		 */
		HashSet<TIntHashSet> selectedFragments_curr = CollectionUtils.deepClone(atomicFragments);
		/*
		 * The fragments that will be considered for inclusion into the solution
		 * in the current iteration.
		 */
		HashSet<TIntHashSet> candidateFragments = new HashSet<TIntHashSet>();

		/* Iteration count. */
		int k = 0;

		boolean stoppingCondition = false;

		/*Part 2-
		  We consider all possible combinations of both fragments and  add it to the atomic fragments
		 */
		while (!stoppingCondition) {
			
			k++;

			/* composite fragment generation */

			candidateFragments.clear();
			selectedFragments_prev.clear();
			selectedFragments_prev.addAll(selectedFragments_curr);

			
			candidateFragments.addAll(this.part2InternalLoop(selectedFragments_prev, atomicFragments, k>1));
			

			/* candidate fragment selection */

			selectedFragments_curr.clear();
			boolean solutionFound = true;

			double presentCost = costCalculator
					.findPartitionsCost(PartitioningUtils.getPartitioningMap(presentSolution));
			double bestCost = presentCost;
			HashSet<TIntHashSet> bestSolution = presentSolution;
			TIntHashSet selectedFragment = null;
			init = System.nanoTime();
			while (solutionFound) {

				solutionFound = false;

				for (TIntHashSet candidate : candidateFragments) {

					if (presentSolution.contains(candidate)) {//This could be done in parallel, but might not have a big impact.
						continue;
					}

					HashSet<TIntHashSet> newSolution = CollectionUtils.deepClone(presentSolution);
					newSolution = addFragment(newSolution, candidate);

                    if (getOverlappingPartitionsSize(newSolution) + unReferencedSize <= (1 + replicationFactor) * w.rowSize) {

						presentCost = costCalculator.findPartitionsCost(PartitioningUtils
								.getPartitioningMap(newSolution));

						//System.out.println(newSolution + " - " + presentCost + " / " + bestCost);

						if (presentCost < bestCost) {
							bestCost = presentCost;
							bestSolution = newSolution;
							selectedFragment = candidate;

							solutionFound = true;
						}
					}
				}

				if (solutionFound) {
					presentSolution = bestSolution;
					selectedFragments_curr.add(selectedFragment);
					candidateFragments.remove(selectedFragment);
				}
			}

			// update stoppingCondition
			stoppingCondition = selectedFragments_curr.size() == 0;
		}
		now = System.nanoTime();
		//System.out.println("Part 2 duration..."+(now-init));
		profiler.numberOfIterations = k;

		partitions = PartitioningUtils.getPartitioningMap(presentSolution);

		/* pairwise merge phase */
		
		stoppingCondition = false;

		/*Part 3- Pairwise merge*/
		double bestCost = costCalculator.findPartitionsCost(PartitioningUtils.getPartitioningMap(presentSolution));
		int bestI = 0, bestJ = 0; // the indexes of the to-be merged fragments

		/* just a utility representation of the solution */
		TIntObjectHashMap<TIntHashSet> partitionsMap;

		init = System.nanoTime();
		while (!stoppingCondition) {
			stoppingCondition = true;
			//partitionsMap = CollectionUtils.deepClone(partitions);
            partitionsMap = PartitioningUtils.getPartitioningMap(presentSolution);

			HashSet<TIntHashSet> modifiedSolution = null;

			for (int i = 1; i <= partitionsMap.size(); i++) {
				for (int j = i + 1; j <= partitionsMap.size(); j++) {

					modifiedSolution = new HashSet<TIntHashSet>(presentSolution);
					modifiedSolution.remove(partitionsMap.get(i));
					modifiedSolution.remove(partitionsMap.get(j));
					TIntHashSet mergedIJ = new TIntHashSet(w.attributeCount);
					mergedIJ.addAll(partitionsMap.get(i));
					mergedIJ.addAll(partitionsMap.get(j));
					modifiedSolution.add(mergedIJ);

					double presentCost = costCalculator.findPartitionsCost(PartitioningUtils
							.getPartitioningMap(modifiedSolution));

					if (presentCost < bestCost) {
						bestCost = presentCost;

						bestI = i;
						bestJ = j;

						stoppingCondition = false;
					}
				}
			}

			if (!stoppingCondition) {
				presentSolution.remove(partitionsMap.get(bestI));
				presentSolution.remove(partitionsMap.get(bestJ));
				TIntHashSet mergedIJ = new TIntHashSet(w.attributeCount);
				mergedIJ.addAll(partitionsMap.get(bestI));
				mergedIJ.addAll(partitionsMap.get(bestJ));
				presentSolution.add(mergedIJ);
			}
		}
		now = System.nanoTime();
		//System.out.println(" Part 3 Duration..."+(now-init));

        if (unReferenced.size() > 0) {
            presentSolution.add(unReferenced);
        }
		partitions = PartitioningUtils.getPartitioningMap(presentSolution);
        costCalculator.findPartitionsCost(partitions);

        bestSolutions = workload.getBestSolutions();

        /* We reduce the partition IDs by 1 and therefore the values in the best solutions as well. */
        TIntObjectHashMap<TIntHashSet> newPartitions = new TIntObjectHashMap<TIntHashSet>();
        TIntObjectHashMap<TIntHashSet> newBestSolutions = new TIntObjectHashMap<TIntHashSet>();
        
        for (int p : partitions.keys()) {
            newPartitions.put(p - 1, partitions.get(p));
        }
        
        for (int q : bestSolutions.keys()) {
            newBestSolutions.put(q, new TIntHashSet());
            for (int p : bestSolutions.get(q).toArray()) {
                newBestSolutions.get(q).add(p - 1);
            }
        }

        partitions = newPartitions;
        bestSolutions = newBestSolutions;
	}

	/**
	 * Method for determining the query extent of a fragment, that is the
	 * cardinality of the set of queries that reference all of the attributes in
	 * a fragment.
	 * 
	 * @param fragment
	 *            The input.
	 * @return The cardinality of the fragment's query extent.
	 */
	private int queryExtent(TIntSet fragment) {
		int size = 0;

		for (int q = 0; q < w.queryCount; q++) {
			boolean referencesAll = true;

			for (TIntIterator it = fragment.iterator(); it.hasNext(); ) {
				if (w.usageMatrix[q][it.next()] == 0) {
					referencesAll = false;
				}
			}

			if (referencesAll) {
				size++;
			}
		}

		return size;
	}

	/**
	 * Method for adding a fragment to a partitioning with removing any of the
	 * subsets of the fragment from the partitioning. Note that this method does
	 * not clone the input partitioning, therefore it returns the modified input
	 * instead of a cloned one.
	 * 
	 * @param partitioning
	 *            The partitioning to be extended.
	 * @param fragment
	 *            The partition to be added.
	 * @return The modified partitioning.
	 */
	private HashSet<TIntHashSet> addFragment(HashSet<TIntHashSet> partitioning, TIntHashSet fragment) {

		HashSet<TIntHashSet> toBeRemoved = new HashSet<TIntHashSet>();

		for (TIntHashSet F1 : partitioning) {
			boolean subset = true;
			for (TIntIterator it = F1.iterator(); it.hasNext(); ) {
				if (!fragment.contains(it.next())) {
					subset = false;
					break;
				}
			}

			if (subset) {
				toBeRemoved.add(F1);
			}
		}

		partitioning.removeAll(toBeRemoved);
		partitioning.add(fragment);

		return partitioning;
	}

	/**
	 * Method for calculating the row size of the partitioned table considering
	 * overlaps, too.
	 * 
	 * @param partitions
	 *            The set of possibly overlapping partitions.
	 * @return The calculated row size.
	 */
	private int getOverlappingPartitionsSize(HashSet<TIntHashSet> partitions) {
		int size = 0;

		for (TIntHashSet partition : partitions) {
			for (TIntIterator it = partition.iterator(); it.hasNext(); ) {
				size += w.attributeSizes[it.next()];
			}
		}

		return size;
	}

    /**
     * Method for calculating the row size of the partitioned table considering
     * overlaps, too.
     *
     * @param partitions
     *            The set of possibly overlapping partitions.
     * @return The calculated row size.
     */
    public int getOverlappingPartitionsSize(TIntObjectHashMap<TIntHashSet> partitions) {
        int size = 0;

        for (TIntHashSet partition : partitions.valueCollection()) {
            for (TIntIterator it = partition.iterator(); it.hasNext(); ) {
                size += w.attributeSizes[it.next()];
            }
        }

        return size;
    }

	public double getReplicationFactor() {
		return replicationFactor;
	}

	public void setReplicationFactor(double replicationFactor) {
		this.replicationFactor = replicationFactor;
	}

	public int getQueryExtentThreshold() {
		return queryExtentThreshold;
	}

	public void setQueryExtentThreshold(int queryExtentThreshold) {
		this.queryExtentThreshold = queryExtentThreshold;
	}
    
	public static void main (String[] args) {
		 String[] queries = {"A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "A10"};
	        Set<AbstractAlgorithm.Algo> algos_sel = new HashSet<AbstractAlgorithm.Algo>();
//	        AbstractAlgorithm.Algo[] ALL_ALGOS_SEL = {AUTOPART, HILLCLIMB, HYRISE};
//			  AbstractAlgorithm.Algo[] ALL_ALGOS_SEL = {TROJAN};
	        AbstractAlgorithm.Algo[] ALL_ALGOS_SEL = {AbstractAlgorithm.Algo.AUTOPART};
	        for (AbstractAlgorithm.Algo algo : ALL_ALGOS_SEL) {
	            algos_sel.add(algo);
	        }
	        AlgorithmRunner algoRunner = new AlgorithmRunner(algos_sel, 10, queries, new AbstractAlgorithm.HDDAlgorithmConfig(BenchmarkTables.randomTable(1, 1)));
	        algoRunner.runTPC_H_All();
	        String output = AlgorithmResults.exportResults(algoRunner.results);

	        System.out.println(output);
	    }
	}
