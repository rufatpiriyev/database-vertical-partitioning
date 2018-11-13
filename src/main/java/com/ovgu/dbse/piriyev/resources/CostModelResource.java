package com.ovgu.dbse.piriyev.resources;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

@Path("/costModels")
@Produces(MediaType.APPLICATION_JSON)
@Api(value="/costModels", description="Return cost models")
public class CostModelResource {
	    @GET
		@Produces(MediaType.APPLICATION_JSON)
		@Metered(name = "getPartitions-meter")
		@Timed(name = "getPartitions-timer")
		@ApiOperation(value = "Get parttions given inputs",
				notes = "Please describe me.",
				response = CostModelResource.class)//Evidently we need to change from string to something more structured.
		@ApiResponses(value = {
				@ApiResponse(code = 200, message = "Success", response = TableResource.class),
				@ApiResponse(code = 404, message = "Input error."),
				@ApiResponse(code = 500, message = "Internal server error")
		})
		public List<String> getAllTables() {
			List<String> allTables = new ArrayList<>();
			
			allTables.add("HDDCostModel");
			allTables.add("HDDSelectivityCostModel");
			allTables.add("MMCostModel");
			
			return allTables;

		}
		

	}

