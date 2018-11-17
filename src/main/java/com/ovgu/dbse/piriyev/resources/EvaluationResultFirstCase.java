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
	@ApiOperation(value = "Get parttions given inputs", notes = "Please describe me.", response = ExistingQueries.class) // structured.
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

		Set<AbstractAlgorithm.Algo> algos_sel = new HashSet<AbstractAlgorithm.Algo>();
		
		List<Query> tableQueries = new ArrayList<>();
		List<String> queryExisted;
		AlgorithmRunner algoRunner;
		String[] Queries;

		// string to enum array set size
		AbstractAlgorithm.Algo[] ALL_ALGOS_SEL = new AbstractAlgorithm.Algo[algorithms.split(",").length];

		int i = 0;

		// strings to enum array
		for (String algorithm : algorithms.split(",")) {
			ALL_ALGOS_SEL[i] = AbstractAlgorithm.Algo.valueOf(algorithm);
			i++;
		}

		// enum set to pass to AlgorithmRunner
		for (AbstractAlgorithm.Algo algo : ALL_ALGOS_SEL) {
			algos_sel.add(algo);
		}

		switch (tableName) {
		case "ALL":
			table = BenchmarkTables.tpchAll(benchmarkConf);
			
			break;
		case "CUSTOMER":
			table = BenchmarkTables.tpchCustomer(new BenchmarkTables.BenchmarkConfig(null, 100, TableType.Default()));
			tableQueries = table.workload.queries;
			Queries = getQueries(queries, tableQueries);
			
			
			algoRunner = new AlgorithmRunner(algos_sel, 10, Queries,
					new AbstractAlgorithm.HDDAlgorithmConfig(table));
			algoRunner.runTPC_H_Customer();			
			partition = AlgorithmResults.exportResults2(algoRunner.results, tableName.toLowerCase());
			partition.tableAttributes = table.attributes;
			break;
		case "LINEITEM":
			table = BenchmarkTables.tpchLineitem(new BenchmarkTables.BenchmarkConfig(null, 100, TableType.Default()));
			tableQueries = table.workload.queries;
			Queries = getQueries(queries, tableQueries);
			algoRunner = new AlgorithmRunner(algos_sel, 10, Queries,
					new AbstractAlgorithm.HDDAlgorithmConfig(table));
			algoRunner.runTPC_H_LineItem(true);			
			partition = AlgorithmResults.exportResults2(algoRunner.results, tableName.toLowerCase());
			partition.tableAttributes = table.attributes;
			break;
		case "PART":
			table = BenchmarkTables.tpchPart(new BenchmarkTables.BenchmarkConfig(null, 100, TableType.Default()));
			tableQueries = table.workload.queries;
			queryExisted = new ArrayList<>();
			Queries = getQueries(queries, tableQueries);
			algoRunner = new AlgorithmRunner(algos_sel, 10, Queries,
					new AbstractAlgorithm.HDDAlgorithmConfig(table));
			algoRunner.runTPC_H_Part();			
			partition = AlgorithmResults.exportResults2(algoRunner.results, tableName.toLowerCase());
			partition.tableAttributes = table.attributes;
			break;
		
		case "SUPPLIER":
			
			table = BenchmarkTables.tpchSupplier(new BenchmarkTables.BenchmarkConfig(null, 100, TableType.Default()));
			tableQueries = table.workload.queries;
			Queries = getQueries(queries, tableQueries);
			algoRunner = new AlgorithmRunner(algos_sel, 10, Queries,
					new AbstractAlgorithm.HDDAlgorithmConfig(table));
			algoRunner.runTPC_H_Supplier();			
			partition = AlgorithmResults.exportResults2(algoRunner.results, tableName.toLowerCase());
			partition.tableAttributes = table.attributes;
			break;
		case "PARTSUPP":
			table = BenchmarkTables.tpchPartSupp(new BenchmarkTables.BenchmarkConfig(null, 100, TableType.Default()));
			tableQueries = table.workload.queries;
			Queries = getQueries(queries, tableQueries);
			algoRunner = new AlgorithmRunner(algos_sel, 10, Queries,
					new AbstractAlgorithm.HDDAlgorithmConfig(table));
			algoRunner.runTPC_H_PartSupp();			
			partition = AlgorithmResults.exportResults2(algoRunner.results, tableName.toLowerCase());
			partition.tableAttributes = table.attributes;
			break;
		case "ORDERS":
			table = BenchmarkTables.tpchOrders(new BenchmarkTables.BenchmarkConfig(null, 100, TableType.Default()));
			tableQueries = table.workload.queries;
			Queries = getQueries(queries, tableQueries);
			algoRunner = new AlgorithmRunner(algos_sel, 10, Queries,
					new AbstractAlgorithm.HDDAlgorithmConfig(table));
			algoRunner.runTPC_H_Orders();			
			partition = AlgorithmResults.exportResults2(algoRunner.results, tableName.toLowerCase());
			partition.tableAttributes = table.attributes;
			break;
		case "NATION":
			table = BenchmarkTables.tpchNation(new BenchmarkTables.BenchmarkConfig(null, 100, TableType.Default()));
			tableQueries = table.workload.queries;
			Queries = getQueries(queries, tableQueries);
			algoRunner = new AlgorithmRunner(algos_sel, 10, Queries,
					new AbstractAlgorithm.HDDAlgorithmConfig(table));
			algoRunner.runTPC_H_Nation();			
			partition = AlgorithmResults.exportResults2(algoRunner.results, tableName.toLowerCase());
			partition.tableAttributes = table.attributes;
			break;
		case "REGION":
			table = BenchmarkTables.tpchRegion(new BenchmarkTables.BenchmarkConfig(null, 100, TableType.Default()));
			tableQueries = table.workload.queries;
			Queries = getQueries(queries, tableQueries);
			algoRunner = new AlgorithmRunner(algos_sel, 10, Queries,
					new AbstractAlgorithm.HDDAlgorithmConfig(table));
			algoRunner.runTPC_H_Region();			
			partition = AlgorithmResults.exportResults2(algoRunner.results, tableName.toLowerCase());
			partition.tableAttributes = table.attributes;
			break;
		default:
			break;
		}
		
		return Response.ok(partition).build();

	}
	
	public static String[] getQueries(String queries, List<Query> tableQueries) {
		List<String> queryExisted = new ArrayList<>();
		
		if (queries.equals("")) {
			for (Query q : tableQueries) {
				queryExisted.add(q.getName());
			}
		} else {
			for (String strQueryName : queries.split(",")) {
				for (Query q : tableQueries) {
					if (strQueryName.equals(q.getName())) {
						queryExisted.add(strQueryName);
						break;
					}
				}
			}
		}
		return queryExisted.toArray(new String[0]);
		
	}
	

}