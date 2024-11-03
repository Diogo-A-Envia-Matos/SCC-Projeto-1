package tukano.impl.rest;

import jakarta.inject.Singleton;
import tukano.api.Blobs;
import tukano.api.rest.RestBlobs;
import tukano.impl.JavaAzureBlobs;
import tukano.impl.JavaFileBlobs;

@Singleton
public class RestBlobsResource extends RestResource implements RestBlobs {

	//TODO: Add diferent version of JavaBlobs for PostgreSQL
	static final Blobs impl = TukanoRestServer.USE_SQL ? JavaFileBlobs.getInstance() : JavaAzureBlobs.getInstance();

	@Override
	public void upload(String blobId, byte[] bytes, String token) {
		super.resultOrThrow( impl.upload(blobId, bytes, token));
	}

	@Override
	public byte[] download(String blobId, String token) {
		return super.resultOrThrow( impl.download( blobId, token ));
	}

	@Override
	public void delete(String blobId, String token) {
		super.resultOrThrow( impl.delete( blobId, token ));
	}
	
	@Override
	public void deleteAllBlobs(String userId, String password) {
		super.resultOrThrow( impl.deleteAllBlobs( userId, password ));
	}
}
