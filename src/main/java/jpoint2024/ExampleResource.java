package jpoint2024;


import javax.ws.rs.GET;
import javax.ws.rs.Path;

@Path("/hello")
public class ExampleResource {
    @GET
    public String hello() {
        return "Hello from RESTEasy Reactive";
    }
}
