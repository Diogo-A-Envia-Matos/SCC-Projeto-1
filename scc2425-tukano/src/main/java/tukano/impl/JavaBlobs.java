package tukano.impl;

import static java.lang.String.format;
import static tukano.api.Result.error;
import static tukano.api.Result.ErrorCode.FORBIDDEN;

import java.util.function.Consumer;
import java.util.logging.Logger;

import tukano.api.Blobs;
import tukano.api.Result;
import tukano.impl.rest.TukanoRestServer;
import tukano.impl.storage.AzureSystemStorage;
import tukano.impl.storage.BlobStorage;
import utils.Hash;
import utils.Hex;

public class JavaBlobs implements Blobs {
	
	private static Blobs instance;
	private static Logger Log = Logger.getLogger(JavaBlobs.class.getName());

	public String baseURI;
	private BlobStorage storage;
	
	synchronized public static Blobs getInstance() {
		if( instance == null )
			instance = new JavaBlobs();
		return instance;
	}
	
	private JavaBlobs() {
		// storage = new FileSystemStorage();
		storage = new AzureSystemStorage();
		baseURI = String.format("%s/%s/", TukanoRestServer.serverURI, Blobs.NAME);
	}
	
	@Override
	public Result<Void> upload(String blobId, byte[] bytes, String token) {
		Log.info(() -> format("upload : blobId = %s, sha256 = %s, token = %s\n", blobId, Hex.of(Hash.sha256(bytes)), token));

		if (!isValidBlobId(blobId, token))
			return error(FORBIDDEN);

		return storage.write( toPath( blobId ), bytes);
	}

	@Override
	public Result<byte[]> download(String blobId, String token) {
		Log.info(() -> format("download : blobId = %s, token=%s\n", blobId, token));

		if( ! isValidBlobId( blobId, token ) )
			return error(FORBIDDEN);

		return storage.read( toPath( blobId ) );
	}

	@Override
	public Result<Void> delete(String blobId, String token) {
		Log.info(() -> format("delete : blobId = %s, token=%s\n", blobId, token));
	
		if( ! isValidBlobId( blobId, token ) )
			return error(FORBIDDEN);

		return storage.delete( toPath(blobId));
	}
	
	@Override
	public Result<Void> deleteAllBlobs(String userId, String token) {
		Log.info(() -> format("deleteAllBlobs : userId = %s, token=%s\n", userId, token));

		if( ! Token.isValid( token, userId ) )
			return error(FORBIDDEN);
		
		return storage.delete( toPath(userId));
	}
	
	private boolean isValidBlobId(String blobId, String token) {
		System.out.println( "validate blob:" + toURL(blobId));
		return Token.isValid(token, toURL(blobId));
	}

	private String toPath(String blobId) {
		return blobId.replace("+", "/");
	}
	
	private String toURL( String blobId ) {
		return baseURI + blobId ;
	}
}
