package rest.client;

import java.net.URI;

import javax.net.ssl.SSLSession;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

public class CriarRecurso {

	public static void main(String[] args) throws Exception {

		Client client = ClientBuilder.newBuilder().hostnameVerifier((String hostname, SSLSession cts) -> true).build();

		URI baseURI = UriBuilder.fromUri("https://localhost:9999/").build();
		WebTarget target = client.target(baseURI);

		Response response = target.path("/some-path/").request()
				.post(Entity.entity(new byte[1024], MediaType.APPLICATION_OCTET_STREAM));

		if (response.hasEntity()) {
			String id = response.readEntity(String.class);
			System.out.println("data resource id: " + id);
		} else
			System.err.println(response.getStatus());
	}
}
