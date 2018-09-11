package com.ovgu.dbse.piriyev.resources;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.ovgu.dbse.piriyev.output.SQueryList;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

//import db.schema.entity.Table;


@Path("/tables")
@Produces(MediaType.APPLICATION_JSON)
@Api(value="/tables", description="Return back tables")
public class TableResource {
	
    @GET
	@Produces(MediaType.APPLICATION_JSON)
	@Metered(name = "getPartitions-meter")
	@Timed(name = "getPartitions-timer")
	@ApiOperation(value = "Get parttions given inputs",
			notes = "Please describe me.",
			response = TableResource.class)//Evidently we need to change from string to something more structured.
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "Success", response = TableResource.class),
			@ApiResponse(code = 404, message = "Input error."),
			@ApiResponse(code = 500, message = "Internal server error")
	})
	public List<String> getAllTables() {
		List<String> allTables = new ArrayList<>();
		
		allTables.add("ALL");
		allTables.add("CUSTOMER");
		allTables.add("LINEITEM");
		allTables.add("PART");
		allTables.add("SUPPLIER");
		allTables.add("PARTSUPP");
		allTables.add("ORDERS");
		allTables.add("NATION");
		allTables.add("REGION");
		
		return allTables;

	}
	

}
