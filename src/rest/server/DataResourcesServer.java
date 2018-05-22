package rest.server;

import java.net.URI;

import javax.net.ssl.SSLContext;

import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

public class DataResourcesServer {

	public static void main(String[] args) throws Exception {
		
		String URI_BASE = "https://0.0.0.0:9999/";

		ResourceConfig config = new ResourceConfig();
		config.register( new rest.server.DataResources() );

		JdkHttpServerFactory.createHttpServer( URI.create(URI_BASE), config, SSLContext.getDefault());

		System.err.println("Server ready....");            
	}

}
