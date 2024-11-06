package tukano.impl.rest;

import jakarta.inject.Singleton;
import tukano.api.Blobs;
import tukano.api.rest.RestBlobs;
import tukano.impl.JavaAzureBlobs;
import tukano.impl.JavaFileBlobs;
import utils.Props;

@Singleton
public class RestBlobsResource extends RestResource implements RestBlobs {

	static final Blobs impl = Boolean.parseBoolean(Props.get("USE_BLOB_STORAGE", "false")) ?
			JavaAzureBlobs.getInstance() : JavaFileBlobs.getInstance();

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
