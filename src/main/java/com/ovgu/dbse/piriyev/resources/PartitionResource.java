package com.ovgu.dbse.piriyev.resources;

import com.ovgu.dbse.piriyev.Wrappers.AttributesFromJson;
import com.ovgu.dbse.piriyev.Wrappers.TableFromJson;
import com.ovgu.dbse.piriyev.Wrappers.WorkloadFromJson;
import com.ovgu.dbse.piriyev.dao.PartitionDao;
import com.ovgu.dbse.piriyev.dao.PartitionDaoImpl;
import com.ovgu.dbse.piriyev.representation.Algorithm;
import com.ovgu.dbse.piriyev.representation.Partition;
import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;

import org.skife.jdbi.v2.DBI;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

import core.algo.vertical.AbstractAlgorithm;
import db.schema.BenchmarkTables;
import db.schema.entity.Attribute;
import db.schema.entity.Table;
import db.schema.entity.Workload;
import experiments.AlgorithmResults;
import experiments.AlgorithmRunner;

import com.ovgu.dbse.piriyev.output.SAttributeList;
import com.ovgu.dbse.piriyev.output.SQueryList;
import com.ovgu.dbse.piriyev.output.STable;
@Path("/partitions")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Api(value="/partitions", description="Stuff you do with algorithms")
public class PartitionResource {
    private final PartitionDao partitionDao;
    
    @Context
    protected UriInfo info;

    public PartitionResource() {
    	partitionDao = new PartitionDaoImpl();
    }

//    @GET
//    public Response indexContacts() {
//    	return null;
//    	//Partition partition = partitionDao.getByName("wwwr");
//        //return Response.ok(partition).build();
//    }

    /*@GET
    @Timed
    @ApiOperation(value="Return the list of algorithms", notes="Anything.")
    @ApiResponses(value={
            @ApiResponse(code=400, message="Invalid ID"),
    })
    public Response testAlgorithm() {
    	
    	List<String> algorithms=info.getQueryParameters().get("algorithms");
    	List<String> queries=info.getQueryParameters().get("queries");
    	
    	//List<String> algorithms = Arrays.asList(algs.split(","));
    	
        Partition partition = partitionDao.getByName(algorithms, queries);
        
        return Response.ok(partition).build();
    }*/
    
    @GET
	@Produces(MediaType.APPLICATION_JSON)
//    @Consumes(MediaType.APPLICATION_JSON)
	@Metered(name = "getPartitions-meter")
	@Timed(name = "getPartitions-timer")
	@ApiOperation(value = "Get parttions given inputs",
			notes = "Please describe me.",
			response = SQueryList.class)//Evidently we need to change from string to something more structured.
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "Success", response = SQueryList.class),
			@ApiResponse(code = 404, message = "Input error."),
			@ApiResponse(code = 500, message = "Internal server error")
	})
	public Response getPartitions(
			
			@ApiParam(value = "Desc Attributes.",
					required = false)
			@QueryParam("attributes") final SAttributeList sattributes,
			@ApiParam(value = "Desc Queries.",
			required = false)
			@QueryParam("queries") final SQueryList queries, 
			@ApiParam(value = "Desc Table.",
			required = true)
			@QueryParam("table") final STable stable,
			@ApiParam(value = "Algorithms: O2P, HillClimb, AutoPart",
			required = true, allowMultiple=true)
			@QueryParam("algorithms") final String csalgorithm) throws Throwable {
		String[] algorithms = csalgorithm.split(",");
		
		List<Attribute> attr = new ArrayList<>();
		attr = AttributesFromJson.getAttributeList(sattributes);
		
		Workload wkld = WorkloadFromJson.constructWorkoad(attr, queries, 60000, "DummyTable"); // need to pass 60000 and Table name as params
		Table table = TableFromJson.getTableFromJson(stable, attr, wkld);
		
		Set<AbstractAlgorithm.Algo> algos_sel = new HashSet<AbstractAlgorithm.Algo>();
		
	    AbstractAlgorithm.Algo[] ALL_ALGOS_SEL = new AbstractAlgorithm.Algo[algorithms.length];
    	
    	int i = 0;
    	for(String algorithm: algorithms) {
    		ALL_ALGOS_SEL[i] = AbstractAlgorithm.Algo.valueOf(algorithm);
    		i++;
    	}
    	
    	
        for (AbstractAlgorithm.Algo algo : ALL_ALGOS_SEL) {
            algos_sel.add(algo);
        }
        
        AbstractAlgorithm.HDDAlgorithmConfig conf = new AbstractAlgorithm.HDDAlgorithmConfig(table);
        
        //String[] namequeries = {"Q1","Q3"};
        
        Partition partition = new Partition(Arrays.asList(algorithms));
        //AlgorithmRunner algoRunner = new AlgorithmRunner(algos_sel, 10, null, conf);
        //algoRunner.runAlgorithms(conf, null);
        //String output = AlgorithmResults.exportResults(algoRunner.results);
        
        //HashMap<String, HashMap<AbstractAlgorithm.Algo, Double>>  runtimes = PartitionDaoImpl.getBestRuntimes(algoRunner.results);
        
        for(AbstractAlgorithm.Algo algo: algos_sel) {
       	 Algorithm algorithmResult = new Algorithm(algo.name());
       	AlgorithmRunner algoRunner = new AlgorithmRunner(algos_sel, 10, null, conf);
       	algoRunner.runAlgorithms(conf, null);
       	 //AlgorithmRunner algoRunner = new AlgorithmRunner(algos_sel, 10, (String[])queries.toArray(), new AbstractAlgorithm.HDDAlgorithmConfig(BenchmarkTables.randomTable(1, 1)));
       	 //algoRunner.runTPC_H_All();
       	 //AbstractAlgorithm.AlgorithmConfig config = algoRunner.getConfiguration();
            //Table tab = config.getTable();
           // List<Attribute> attributes = tab.getAttributes();
          //  algorithmResult.setTableAttributes(attributes);
        double  runtime = AlgorithmResults.getBestRuntime("DummyTable", algo, algoRunner.results);
        algorithmResult.setResponseTime(runtime);
        Map<Integer, int[]> partitions = AlgorithmResults.getPartititons("DummyTable", algo, algoRunner.results);
            algorithmResult.setPartitions(partitions);
            partition.addAlgorithmResults(algorithmResult);
       }
		//Here you call a singleton class, and pass as input all the inputs here. And that class returns an object of type responseOutput (whatever that is, and you need to create that pojo, and to put it as the response type)
    	return Response.ok(partition).build();
		
    }
    
    
}

