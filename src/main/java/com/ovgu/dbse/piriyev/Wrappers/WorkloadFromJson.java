package com.ovgu.dbse.piriyev.Wrappers;

import java.util.Arrays;
import java.util.List;

import com.ovgu.dbse.piriyev.output.SQuery;
import com.ovgu.dbse.piriyev.output.SQueryList;

import db.schema.entity.Attribute;
import db.schema.entity.Workload;

public class WorkloadFromJson {
	
	public static Workload constructWorkoad( List<Attribute> attributes ,SQueryList queryList, int tableSize, String tableName )   {
	    Workload workload = new Workload(attributes, tableSize, tableName);
		
		for(SQuery squery: queryList.getQueries()) {
			if(squery.getType().equals("addProjectionQuery")) {
				workload.addProjectionQuery(squery.getName(), squery.getWeight() , 
						squery.getProjections().stream().mapToInt(i->i).toArray());
			}
			else if(squery.getType().equals("addProjectionQueryWithFiltering")) {
				workload.addProjectionQueryWithFiltering(squery.getName(), squery.getWeight(), 
						squery.getFilteredColumns().stream().mapToInt(i->i).toArray() , 
						squery.getSelectivity(), squery.getProjections().stream().mapToInt(i->i).toArray());
			}
			else {
				// add range query need to discuss!!!
			}
		}
		return workload;
	}

}
