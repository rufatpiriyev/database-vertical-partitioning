package com.ovgu.dbse.piriyev.representation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import core.algo.vertical.AbstractAlgorithm;
import core.utils.ArrayUtils;
import db.schema.entity.Attribute;
import experiments.AlgorithmResults;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;

public class Partition {
	
    private final List<String> algorithmNames;
    
    public List<Algorithm> algorithmResults;
    
  
    public  Partition(List<String> algorithmNames) {
        this.algorithmNames = algorithmNames;
        algorithmResults = new ArrayList<Algorithm>();
    }
    

    public void addAlgorithmResults( Algorithm algorithmResults) {
    	this.algorithmResults.add(algorithmResults);
    }
    
    public List<Algorithm> getAlgorithmResults(){
    	return algorithmResults;
    }

   
}
