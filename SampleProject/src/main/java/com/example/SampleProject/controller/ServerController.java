package com.example.SampleProject.controller;
import com.example.SampleProject.exception.NegativeNumberException;
import com.example.SampleProject.model.Server;
import com.example.SampleProject.service.ServerService;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

@Path("/server")
public class ServerController {

    ServerService serverService = new ServerService();

    @GET
    @Path("/{size}")
    @Produces(MediaType.APPLICATION_JSON)
    public Server allocateServer(@PathParam("size") int size) throws NegativeNumberException {
        if(size <= 0){
            throw new NegativeNumberException("Size must be positive number, not "+size);
        }
        return serverService.allocate(size);
    }


    @GET
    @Path("/getAll")
    @Produces(MediaType.APPLICATION_JSON)
    public List<Server> getAllServers() {
        return serverService.scanServersPool();
    }


    @DELETE
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/{id}")
    public Response delete(@PathParam("id") Integer id) {
            return Response.ok(serverService.delete(id)).build();
        }


}
