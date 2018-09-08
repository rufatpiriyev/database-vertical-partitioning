package com.ovgu.dbse.piriyev.dao;

import java.util.List;
import java.util.Set;

import com.ovgu.dbse.piriyev.representation.Partition;


public interface PartitionDao {
	   public  Partition getByName(List<String> algorithms, List<String> queries);
	   public  Partition getByJson(String jsonString);  
}

