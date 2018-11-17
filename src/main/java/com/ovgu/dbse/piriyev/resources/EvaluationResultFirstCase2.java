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

@Path("/EvaluationResults2")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/EvaluationResults2", description = "Evaluation Results second,third case")
public class EvaluationResultFirstCase2 {

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Metered(name = "getPartitions-meter")
	@Timed(name = "getPartitions-timer")
	@ApiOperation(value = "Get parttions given inputs", notes = "Please describe me.", response = ExistingQueries.class) // structured.
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Success", response = ExistingQueries.class),
			@ApiResponse(code = 404, message = "Input error."),
			@ApiResponse(code = 500, message = "Internal server error") })
	public Response getPartitions(
			@ApiParam(value = "Random table in a format of [1, 8, 4, 100]", required = true, allowMultiple = false) @QueryParam("tableAttributes") final String tableAttributes,
			@ApiParam(value = "Workload in a format of [[], []]", required = true, allowMultiple = false) @QueryParam("queries") final String queriesMatrix,
			@ApiParam(value = "Number of rows", required = true, allowMultiple = false) @QueryParam("rowNumbers") final String numberOfRows)
			
			throws Throwable {
		

		//TODO: get Table from tableAttributes
		BenchmarkTables.BenchmarkConfig benchmarkConf;

		benchmarkConf = new BenchmarkConfig(null, 1, TableType.Default());
		AbstractAlgorithm.HDDAlgorithmConfig confHd = null;
		AbstractAlgorithm.MMAlgorithmConfig confMM = null;

		Table table = null;
		List<String> algorithms = new ArrayList<>();
		algorithms.add("HILLCLIMB");
		
		Partition partition = new Partition(algorithms);
		

		Set<AbstractAlgorithm.Algo> algos_sel = new HashSet<AbstractAlgorithm.Algo>();
//		
		List<Query> tableQueries = new ArrayList<>();
		List<String> tableQueryNames = new ArrayList<>();
//		List<String> queryExisted;
		AlgorithmRunner algoRunner;
//		String[] Queries;
//
//		// string to enum array set size
		AbstractAlgorithm.Algo[] ALL_ALGOS_SEL = new AbstractAlgorithm.Algo[1];
//
		int i = 0;
//
//		// strings to enum array
		for (String algorithm : algorithms) {
			ALL_ALGOS_SEL[i] = AbstractAlgorithm.Algo.valueOf(algorithm);
			i++;
		}
//
//		// enum set to pass to AlgorithmRunner
		for (AbstractAlgorithm.Algo algo : ALL_ALGOS_SEL) {
			algos_sel.add(algo);
		}
		
		int iNumberOfRows = Integer.parseInt(numberOfRows);
		
		table = BenchmarkTables.randomTable2(tableAttributes, queriesMatrix, iNumberOfRows);
		tableQueries = table.workload.queries;
		
		for(Query tableQuery: tableQueries) {
			tableQueryNames.add(tableQuery.getName());
		}
		
		algoRunner = new AlgorithmRunner(algos_sel, 10, tableQueryNames.toArray(new String[0]), 
				new AbstractAlgorithm.HDDAlgorithmConfig(table));
		
		algoRunner.runAlgorithms(new AbstractAlgorithm.HDDAlgorithmConfig(table), AlgorithmRunner.generalCGrpThreshold);
		partition = AlgorithmResults.exportResults2(algoRunner.results, table.name);
		partition.tableAttributes = table.attributes;
//
//		switch (tableName) {
//		case "ALL":
//			table = BenchmarkTables.tpchAll(benchmarkConf);
//			
//			break;
//		case "CUSTOMER":
//			table = BenchmarkTables.tpchCustomer(new BenchmarkTables.BenchmarkConfig(null, 100, TableType.Default()));
//			tableQueries = table.workload.queries;
//			Queries = getQueries(queries, tableQueries);
//			
//			
//			algoRunner = new AlgorithmRunner(algos_sel, 10, Queries,
//					new AbstractAlgorithm.HDDAlgorithmConfig(table));
//			algoRunner.runTPC_H_Customer();			
//			partition = AlgorithmResults.exportResults2(algoRunner.results, tableName.toLowerCase());
//			partition.tableAttributes = table.attributes;
//			break;
//		case "LINEITEM":
//			table = BenchmarkTables.tpchLineitem(new BenchmarkTables.BenchmarkConfig(null, 100, TableType.Default()));
//			tableQueries = table.workload.queries;
//			Queries = getQueries(queries, tableQueries);
//			algoRunner = new AlgorithmRunner(algos_sel, 10, Queries,
//					new AbstractAlgorithm.HDDAlgorithmConfig(table));
//			algoRunner.runTPC_H_LineItem(true);			
//			partition = AlgorithmResults.exportResults2(algoRunner.results, tableName.toLowerCase());
//			partition.tableAttributes = table.attributes;
//			break;
//		case "PART":
//			table = BenchmarkTables.tpchPart(new BenchmarkTables.BenchmarkConfig(null, 100, TableType.Default()));
//			tableQueries = table.workload.queries;
//			queryExisted = new ArrayList<>();
//			Queries = getQueries(queries, tableQueries);
//			algoRunner = new AlgorithmRunner(algos_sel, 10, Queries,
//					new AbstractAlgorithm.HDDAlgorithmConfig(table));
//			algoRunner.runTPC_H_Part();			
//			partition = AlgorithmResults.exportResults2(algoRunner.results, tableName.toLowerCase());
//			partition.tableAttributes = table.attributes;
//			break;
//		
//		case "SUPPLIER":
//			
//			table = BenchmarkTables.tpchSupplier(new BenchmarkTables.BenchmarkConfig(null, 100, TableType.Default()));
//			tableQueries = table.workload.queries;
//			Queries = getQueries(queries, tableQueries);
//			algoRunner = new AlgorithmRunner(algos_sel, 10, Queries,
//					new AbstractAlgorithm.HDDAlgorithmConfig(table));
//			algoRunner.runTPC_H_Supplier();			
//			partition = AlgorithmResults.exportResults2(algoRunner.results, tableName.toLowerCase());
//			partition.tableAttributes = table.attributes;
//			break;
//		case "PARTSUPP":
//			table = BenchmarkTables.tpchPartSupp(new BenchmarkTables.BenchmarkConfig(null, 100, TableType.Default()));
//			tableQueries = table.workload.queries;
//			Queries = getQueries(queries, tableQueries);
//			algoRunner = new AlgorithmRunner(algos_sel, 10, Queries,
//					new AbstractAlgorithm.HDDAlgorithmConfig(table));
//			algoRunner.runTPC_H_PartSupp();			
//			partition = AlgorithmResults.exportResults2(algoRunner.results, tableName.toLowerCase());
//			partition.tableAttributes = table.attributes;
//			break;
//		case "ORDERS":
//			table = BenchmarkTables.tpchOrders(new BenchmarkTables.BenchmarkConfig(null, 100, TableType.Default()));
//			tableQueries = table.workload.queries;
//			Queries = getQueries(queries, tableQueries);
//			algoRunner = new AlgorithmRunner(algos_sel, 10, Queries,
//					new AbstractAlgorithm.HDDAlgorithmConfig(table));
//			algoRunner.runTPC_H_Orders();			
//			partition = AlgorithmResults.exportResults2(algoRunner.results, tableName.toLowerCase());
//			partition.tableAttributes = table.attributes;
//			break;
//		case "NATION":
//			table = BenchmarkTables.tpchNation(new BenchmarkTables.BenchmarkConfig(null, 100, TableType.Default()));
//			tableQueries = table.workload.queries;
//			Queries = getQueries(queries, tableQueries);
//			algoRunner = new AlgorithmRunner(algos_sel, 10, Queries,
//					new AbstractAlgorithm.HDDAlgorithmConfig(table));
//			algoRunner.runTPC_H_Nation();			
//			partition = AlgorithmResults.exportResults2(algoRunner.results, tableName.toLowerCase());
//			partition.tableAttributes = table.attributes;
//			break;
//		case "REGION":
//			table = BenchmarkTables.tpchRegion(new BenchmarkTables.BenchmarkConfig(null, 100, TableType.Default()));
//			tableQueries = table.workload.queries;
//			Queries = getQueries(queries, tableQueries);
//			algoRunner = new AlgorithmRunner(algos_sel, 10, Queries,
//					new AbstractAlgorithm.HDDAlgorithmConfig(table));
//			algoRunner.runTPC_H_Region();			
//			partition = AlgorithmResults.exportResults2(algoRunner.results, tableName.toLowerCase());
//			partition.tableAttributes = table.attributes;
//			break;
//		default:
//			break;
//		}
		
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