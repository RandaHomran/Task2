package com.example.SampleProject.exception;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class NegativeNumberException extends Exception implements
        ExceptionMapper<NegativeNumberException>
{
    private static final long serialVersionUID = 1L;

    public NegativeNumberException() {
        super("size must be positive value");
    }

    public NegativeNumberException(String string) {
        super(string);
    }

    @Override
    public Response toResponse(NegativeNumberException exception) {
        return Response.status(500).entity(exception.getMessage())
                .type("text/plain").build();
    }
}