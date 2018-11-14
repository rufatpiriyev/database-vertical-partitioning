package com.ovgu.dbse.piriyev.resources;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.ovgu.dbse.piriyev.representation.Algorithm;
import com.ovgu.dbse.piriyev.representation.Partition;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

import core.algo.vertical.AbstractAlgorithm;
import db.schema.BenchmarkTables;
import db.schema.BenchmarkTables.BenchmarkConfig;
import db.schema.entity.Query;
import db.schema.entity.Table;
import db.schema.types.TableType;
import experiments.AlgorithmResults;
import experiments.AlgorithmRunner;

@Path("/EvaluationResults1")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/EvaluationResults1", description = "Evaluation Results first case")
public class EvaluationResultFirstCase {

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Metered(name = "getPartitions-meter")
	@Timed(name = "getPartitions-timer")
	@ApiOperation(value = "Get parttions given inputs", notes = "Please describe me.", response = ExistingQueries.class) 																													// structured.
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Success", response = ExistingQueries.class),
			@ApiResponse(code = 404, message = "Input error."),
			@ApiResponse(code = 500, message = "Internal server error") })
	public Response getPartitions(
			@ApiParam(value = "Tables: One of them: CUSTOMER, LINEITEM etc, User will select it from DropDown list", required = true, allowMultiple = false) @QueryParam("table") final String tableName,
			@ApiParam(value = "Existing queries (Q1, Q2 etc), user will select them from list box from the UI", required = true, allowMultiple = false) @QueryParam("queries") final String queries,
			@ApiParam(value = "One of the Cost models: HDD, HDDSelectivityCostModel or MMCostModel: User will select it from UI ", required = true, allowMultiple = false) @QueryParam("costModel") final String costModel,
			@ApiParam(value = "Algorithms: O2P, HYRISE etc, User will select from UI", required = true, allowMultiple = false) @QueryParam("algorithms") final String algorithms)
			throws Throwable {

		BenchmarkTables.BenchmarkConfig benchmarkConf;

		benchmarkConf = new BenchmarkConfig(null, 1, TableType.Default());
		AbstractAlgorithm.HDDAlgorithmConfig confHd = null;
		AbstractAlgorithm.MMAlgorithmConfig confMM = null;

		Table table = null;
		Partition partition = new Partition(Arrays.asList(algorithms));
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

		switch (costModel) {
		case "HDDCostModel":
			confHd = new AbstractAlgorithm.HDDAlgorithmConfig(table);

			break;
		case "HDDSelectivityCostModel":
			// will be implemented later
			// conf = new AbstractAlgorithm.HDDAlgorithmConfig(table,);
			break;
		case "MMCostModel":
			confMM = new AbstractAlgorithm.MMAlgorithmConfig(table);
			break;
		}

		List<String> queryNames = new ArrayList<>();
		List<Query> queryExisted = new ArrayList<>();

		for (String strQueryName : queries.split(",")) {
			for (Query q : table.workload.queries) {
				if (strQueryName.equals(q.getName())) {
					queryExisted.add(q);
					break;
				}
			}
		}
		
		table.workload.queries = queryExisted;
		
		
		Set<AbstractAlgorithm.Algo> algos_sel = new HashSet<AbstractAlgorithm.Algo>();
		
		AbstractAlgorithm.Algo[] ALL_ALGOS_SEL = new AbstractAlgorithm.Algo[algorithms.split(",").length];
    	
    	int i = 0;
    	for(String algorithm: algorithms.split(",")) {
    		ALL_ALGOS_SEL[i] = AbstractAlgorithm.Algo.valueOf(algorithm);
    		i++;
    	}
    	

        for (AbstractAlgorithm.Algo algo : ALL_ALGOS_SEL) {
            algos_sel.add(algo);
        }
		
		
        for(AbstractAlgorithm.Algo algo: algos_sel) {
          	Algorithm algorithmResult = new Algorithm(algo.name());
          	AlgorithmRunner algoRunner = new AlgorithmRunner(algos_sel, 10, null, confHd !=null?confHd: confMM);
          	algoRunner.runAlgorithms(confHd !=null?confHd: confMM, null);
          	 //AlgorithmRunner algoRunner = new AlgorithmRunner(algos_sel, 10, (String[])queries.toArray(), new AbstractAlgorithm.HDDAlgorithmConfig(BenchmarkTables.randomTable(1, 1)));
          	 //algoRunner.runTPC_H_All();
          	 //AbstractAlgorithm.AlgorithmConfig config = algoRunner.getConfiguration();
               //Table tab = config.getTable();
              // List<Attribute> attributes = tab.getAttributes();
             //  algorithmResult.setTableAttributes(attributes);
           double  runtime = AlgorithmResults.getBestRuntime(tableName.toLowerCase(), algo, algoRunner.results);
           algorithmResult.setResponseTime(runtime);
           Map<Integer, int[]> partitions = AlgorithmResults.getPartititons(tableName.toLowerCase(), algo, algoRunner.results);
               algorithmResult.setPartitions(partitions);
               algorithmResult.setActionSequence(AlgorithmResults.getActionSequence(tableName, algo, algoRunner.results));
               partition.addAlgorithmResults(algorithmResult);       
          }
		
		return Response.ok(partition).build();

	}

}