package com.ovgu.dbse.piriyev.resources;

import com.ovgu.dbse.piriyev.dao.PartitionDao;
import com.ovgu.dbse.piriyev.dao.PartitionDaoImpl;
import com.ovgu.dbse.piriyev.representation.Partition;
import com.codahale.metrics.annotation.Timed;

import org.skife.jdbi.v2.DBI;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

@Path("/algorithms")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Api(value="/algorithms", description="Stuff you do with algorithms")
public class PartitionResource {
    private final PartitionDao partitionDao;

    public PartitionResource() {
    	partitionDao = new PartitionDaoImpl();
    }

//    @GET
//    public Response indexContacts() {
//    	return null;
//    	//Partition partition = partitionDao.getByName("wwwr");
//        //return Response.ok(partition).build();
//    }

    @GET
    @Path("/{algorithms}")
    @Timed
    @ApiOperation(value="Return the list of algorithms", notes="Anything.")
    @ApiResponses(value={
            @ApiResponse(code=400, message="Invalid ID"),
    })
    public Response testAlgorithm(@PathParam("algorithms") String algs) {
    	List<String> algorithms = Arrays.asList(algs.split(","));
    	
        Partition partition = partitionDao.getByName(algorithms);
        
        return Response.ok(partition).build();
    }
    
}

