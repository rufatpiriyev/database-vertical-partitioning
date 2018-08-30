package com.ovgu.dbse.piriyev.resources;

import com.ovgu.dbse.piriyev.dao.PartitionDao;
import com.ovgu.dbse.piriyev.dao.PartitionDaoImpl;
import com.ovgu.dbse.piriyev.representation.Partition;

import org.skife.jdbi.v2.DBI;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

@Path("/algorithms")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
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
    public Response testAlgorithm(@PathParam("algorithms") String algs) {
    	List<String> algorithms = Arrays.asList(algs.split(","));
    	
        Partition partition = partitionDao.getByName(algorithms);
        
        return Response.ok(partition).build();
    }
    
}

