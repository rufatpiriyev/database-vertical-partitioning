package com.ovgu.dbse.piriyev.dao;

import com.ovgu.dbse.piriyev.dao.PartitionDao;
import com.ovgu.dbse.piriyev.dao.PartitionDaoImpl;
import com.ovgu.dbse.piriyev.representation.Partition;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.json.JSONObject;
import org.skife.jdbi.v2.DBI;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

@Path("/testPost")
@Produces(MediaType.APPLICATION_JSON)
public class TestResourcePostTest {
	
	 @POST
	    public Response logEvent(TestClass c) {
		 String properties = c.properties;
		 
		 JSONObject jsonObject = new JSONObject(properties);
		 JSONObject root = jsonObject.getJSONObject("postInput");
		 JSONObject attributes = root.getJSONObject("attributes");
		 root.getJSONArray("attributes");
		 JSONObject attribute = root.getJSONArray("attributes").getJSONObject(0);
		 
		 
		 return Response.ok(properties).build();
	    }

	    public static class TestClass {

	    	@JsonProperty("inputValue")
	        public String properties;
	     
	    }
}


