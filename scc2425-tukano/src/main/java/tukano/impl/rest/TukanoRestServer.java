package tukano.impl.rest;

import java.util.HashSet;
import java.util.Set;

import java.util.logging.Logger;

import jakarta.ws.rs.core.Application;
import tukano.impl.Token;
import utils.IP;
import utils.Props;

public class TukanoRestServer extends Application {

	final private static Logger Log = Logger.getLogger(TukanoRestServer.class.getName());

	private static final String TOKEN_SECRET = "Token_secret";

	// https://tukano-1730835561603.azurewebsites.net
	static final String INETADDR_ANY = "0.0.0.0";
	// static String SERVER_BASE_URI = "http://%s/tukano/rest";
	static String SERVER_BASE_URI = "http://%s:%s/tukano/rest";
	static String HOST_NAME = "127.0.0.1";
	// static String HOST_NAME = "tukano-1730835561603.azurewebsites.net";

	public static final int PORT = 8080;

	public static String serverURI;

	private Set<Object> singletons = new HashSet<>();
	private Set<Class<?>> resources = new HashSet<>();

	static {
		System.setProperty("java.util.logging.SimpleFormatter.format", "%4$s: %5$s");
	}

	public TukanoRestServer() {
		Token.setSecret(TOKEN_SECRET);

		// had to hard code - ip.hostname() gave wrong host
		Props.load("azurekeys-region.props"); //place the props file in resources folder under java/main

		// serverURI = String.format(SERVER_BASE_URI, IP.hostname());
		serverURI = String.format(SERVER_BASE_URI, HOST_NAME, PORT);
		resources.add(RestBlobsResource.class);
		resources.add(RestUsersResource.class);
		resources.add(RestShortsResource.class);

	}

	@Override
	public Set<Class<?>> getClasses() {
		return resources;
	}

	@Override
	public Set<Object> getSingletons() {
		return singletons;
	}

	public static void main(String[] args) throws Exception {
		return;
	}
}
