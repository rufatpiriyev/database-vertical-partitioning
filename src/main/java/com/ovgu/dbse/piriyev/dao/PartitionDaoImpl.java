package com.ovgu.dbse.piriyev.dao;

import static core.algo.vertical.AbstractAlgorithm.Algo.AUTOPART;
import static core.algo.vertical.AbstractAlgorithm.Algo.COLUMN;
import static core.algo.vertical.AbstractAlgorithm.Algo.DREAM;
import static core.algo.vertical.AbstractAlgorithm.Algo.HILLCLIMB;
import static core.algo.vertical.AbstractAlgorithm.Algo.HYRISE;
import static core.algo.vertical.AbstractAlgorithm.Algo.NAVATHE;
import static core.algo.vertical.AbstractAlgorithm.Algo.O2P;
import static core.algo.vertical.AbstractAlgorithm.Algo.OPTIMAL;
import static core.algo.vertical.AbstractAlgorithm.Algo.ROW;
import static core.algo.vertical.AbstractAlgorithm.Algo.TROJAN;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import core.algo.vertical.AbstractAlgorithm;
import db.schema.BenchmarkTables;
import db.schema.entity.Attribute;
import db.schema.entity.Table;
import experiments.AlgorithmResults;
import experiments.AlgorithmRunner;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;

import com.ovgu.dbse.piriyev.helper.EnumHelper;
import com.ovgu.dbse.piriyev.representation.Algorithm;
import com.ovgu.dbse.piriyev.representation.Partition;

public class PartitionDaoImpl implements PartitionDao {
    	
	Partition partition;
	
	public PartitionDaoImpl() {
		
	}
	
	
	public Partition getByName(List<String> algorithms, List<String> queries) {
		
		partition = new Partition(algorithms);
		
	    
		//String[] queries = {"A1", "A2", "A3"};
    	Set<AbstractAlgorithm.Algo> algos_sel = new HashSet<AbstractAlgorithm.Algo>();
    	
    
    	AbstractAlgorithm.Algo[] ALL_ALGOS_SEL = new AbstractAlgorithm.Algo[algorithms.size()];
    	
    	int i = 0;
    	for(String algorithm: algorithms) {
    		ALL_ALGOS_SEL[i] = AbstractAlgorithm.Algo.valueOf(algorithm);
    		i++;
    	}
    	
    	
        for (AbstractAlgorithm.Algo algo : ALL_ALGOS_SEL) {
            algos_sel.add(algo);
        }
        
        for(AbstractAlgorithm.Algo algo: algos_sel) {
        	 Algorithm algorithmResult = new Algorithm(algo.name());
        	 AlgorithmRunner algoRunner = new AlgorithmRunner(algos_sel, 10, (String[])queries.toArray(), new AbstractAlgorithm.HDDAlgorithmConfig(BenchmarkTables.randomTable(1, 1)));
        	 algoRunner.runTPC_H_All();
        	 AbstractAlgorithm.AlgorithmConfig config = algoRunner.getConfiguration();
             Table tab = config.getTable();
            // List<Attribute> attributes = tab.getAttributes();
           //  algorithmResult.setTableAttributes(attributes);
             HashMap<String, HashMap<AbstractAlgorithm.Algo, Double>>  runtimes = getBestRuntimes(algoRunner.results);
           //  algorithmResult.setResponseTime(runtimes);
           //  Map<Integer, int[]> partitions = AlgorithmResults.getPartititons(tab.name, algo, algoRunner.results);
             //algorithmResult.setPartitions(partitions);
             partition.addAlgorithmResults(algorithmResult);
        }
        
      
    
        
        
        
        
        
        
//        AlgorithmRunner algoRunner = new AlgorithmRunner(algos_sel, 10, queries, new AbstractAlgorithm.HDDAlgorithmConfig(BenchmarkTables.randomTable(1, 1)));
//        algoRunner.runTPC_H_All();
//        
//        
//        AbstractAlgorithm.AlgorithmConfig config = algoRunner.getConfiguration();
//        Table tab = config.getTable();
//        List<Attribute> attributes = tab.getAttributes();
//        partition.setTableAttributes(attributes);
//        
//        HashMap<String, HashMap<AbstractAlgorithm.Algo, Double>>  runtimes = getBestRuntimes(algoRunner.results);
//        Map<Integer, int[]> partitions = AlgorithmResults.getPartititons(tab.name, ALL_ALGOS_SEL[0], algoRunner.results);
//        //HashMap<String, HashMap<AbstractAlgorithm.Algo, TIntObjectHashMap<TIntHashSet>>> bestSolutions = getPartitons(algoRunner.results);
//        partition.setResponseTime(runtimes);
//        //partition.setPartitions(partitions);
//        //partition.setBestSolutions(bestSolutions);
        
		return partition;
	}
	
    public Partition getByJson(String jsonString) {
    	
    	
    	
    	
    	
    	
    	
    	return null;
    	
    }
	
   public static HashMap<String, HashMap<AbstractAlgorithm.Algo, Double>> getBestRuntimes (AlgorithmResults results) {
   	 return results.runTimes;
   }
   
   public static HashMap<String, HashMap<AbstractAlgorithm.Algo, TIntObjectHashMap<TIntHashSet>>> getBestSolutions (AlgorithmResults results) {
  	 return results.bestSolutions;
   }
   
   public static HashMap<String, HashMap<AbstractAlgorithm.Algo, TIntObjectHashMap<TIntHashSet>>> getPartitons (AlgorithmResults results){
	  return results.partitions;
   }
   
   
   
}
