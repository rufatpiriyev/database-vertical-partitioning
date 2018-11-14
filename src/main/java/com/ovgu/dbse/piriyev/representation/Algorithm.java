package com.ovgu.dbse.piriyev.representation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import core.algo.vertical.AbstractAlgorithm;
import db.schema.entity.Attribute;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;

public class Algorithm {
	
    private final String algorithmName;
    
    private  double responseTime;
    Map<Integer, int[]> partitions;
    private HashMap<String, HashMap<AbstractAlgorithm.Algo, TIntObjectHashMap<TIntHashSet>>> bestSolutions;
    private List<Attribute> attributes;
    private List<Integer> actionSequence;
    
    
	
	public Algorithm(String algorithmName) {
	        this.algorithmName = algorithmName;
	}
	
	public void setResponseTime( double responseTime) {
    	this.responseTime = responseTime;
    }
    
    public void setPartitions( Map<Integer, int[]> partitions) {
    	this.partitions = partitions;
    }
    
    public void setBestSolutions( HashMap<String, HashMap<AbstractAlgorithm.Algo, TIntObjectHashMap<TIntHashSet>>> bestSolutions) {
    	this.bestSolutions = bestSolutions;
    }
    
    public double getResponseTime() {
        return this.responseTime;
    }
    
    public Map<Integer, int[]> getPartitions() {
        return this.partitions;
    }
    
    public HashMap<String, HashMap<AbstractAlgorithm.Algo, TIntObjectHashMap<TIntHashSet>>> getBestSolutions() {
        return this.bestSolutions;
    }
    
    public void setTableAttributes(List<Attribute> attributes){
      	  this.attributes = attributes;
    }
    
    public List<Attribute> getTableAttributes(){
    	  return this.attributes;
    }
    
    public String getAlgorithmName() {
    	return algorithmName;
    }
    
    
    public void setActionSequence(List<Integer> actionSequence){
    	this.actionSequence = actionSequence;
    }   
    
    public List<Integer> getActionSequence(){

    	return actionSequence;
    }
	
    
    
	
	
	

}
