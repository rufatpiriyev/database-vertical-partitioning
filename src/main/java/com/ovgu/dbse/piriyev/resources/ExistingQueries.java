package com.ovgu.dbse.piriyev.resources;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.ovgu.dbse.piriyev.output.SAttributeList;
import com.ovgu.dbse.piriyev.output.SQueryList;
import com.ovgu.dbse.piriyev.output.STable;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

import core.algo.vertical.AbstractAlgorithm;
import core.costmodels.CostModel;
import db.schema.BenchmarkTables;
import db.schema.BenchmarkTables.BenchmarkConfig;
import db.schema.entity.Query;
import db.schema.entity.Table;
import db.schema.types.TableType;

@Path("/existingQueries")
@Produces(MediaType.APPLICATION_JSON)
@Api(value="/existingQueries", description="Get the existing queries based on table")
public class ExistingQueries {
	
    @GET
	@Produces(MediaType.APPLICATION_JSON)
	@Metered(name = "getPartitions-meter")
	@Timed(name = "getPartitions-timer")
	@ApiOperation(value = "Get parttions given inputs",
			notes = "Please describe me.",
			response = ExistingQueries.class)//Evidently we need to change from string to something more structured.
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "Success", response = ExistingQueries.class),
			@ApiResponse(code = 404, message = "Input error."),
			@ApiResponse(code = 500, message = "Internal server error")
	})
public Response getPartitions(
			@ApiParam(value = "Tables: CUSTOMER, LINEITEM etc",
			required = true, allowMultiple=false)
			@QueryParam("table") final String tableName,
			@ApiParam(value = "Tables: CUSTOMER, LINEITEM etc",
			required = true, allowMultiple=false)
			@QueryParam("costModel") final String costModel)		
					throws Throwable {
 
    	    BenchmarkTables.BenchmarkConfig benchmarkConf;
    	    
    	    benchmarkConf = new BenchmarkConfig(null, 1, TableType.Default());
    	    
    	    
    	    
    	    Table table = null;
    	
   
    	    switch (tableName) {
			case "ALL":
				table = BenchmarkTables.tpchAll(benchmarkConf);
				break;
			case "CUSTOMER":
				table = BenchmarkTables.tpchCustomer(benchmarkConf);
				break;
			case "LINEITEM":
				table = BenchmarkTables.tpchLineitem(benchmarkConf);
				break;
			case "PART":
				table = BenchmarkTables.tpchPart(benchmarkConf);
				break;
			case "SUPPLIER":
				table = BenchmarkTables.tpchSupplier(benchmarkConf);
				break;
			case "PARTSUPP":
				table = BenchmarkTables.tpchPartSupp(benchmarkConf);
				break;
			case "ORDERS":
				table = BenchmarkTables.tpchOrders(benchmarkConf);
				break;
			case "NATION":
				table = BenchmarkTables.tpchNation(benchmarkConf);
				break;
			case "REGION":
				table = BenchmarkTables.tpchRegion(benchmarkConf);
				break;
			default:
				break;
    	      }		
    	    
    	    List<String> queryNames  = new ArrayList<>();
    	    
    	    
    	    for(Query q: table.workload.queries) {
    	    	queryNames.add(q.getName() + q.getAttributeUsageVector().toString());
    	    	
    	    }
    	
 
		   return Response.ok(queryNames).build();

    	
    	
    }
    
    
    
	

}
