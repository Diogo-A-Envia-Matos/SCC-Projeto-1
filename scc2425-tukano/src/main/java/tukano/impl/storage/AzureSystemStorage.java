package tukano.impl.storage;

import static tukano.api.Result.ErrorCode.*;
import static tukano.api.Result.*;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.logging.Logger;
import tukano.api.Result;
import utils.Hash;

public class AzureSystemStorage implements BlobStorage {
	private static final int CHUNK_SIZE = 4096;
	private static final String CONTAINER_NAME = "blobs";
	// Must get the string by hand
	private static final String storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=scc54471;AccountKey=Y3VlAt+RbAknLVeO2MWnKLfi7yFPsdczv0H+VAZxbuaLH2pz304RUpSd0vwB67niUDW9rQZn5C6M+AStR0bxpQ==;EndpointSuffix=core.windows.net";

	private static final Logger log = Logger.getLogger(AzureSystemStorage.class.getName());
	private final BlobContainerClient containerClient;

	public AzureSystemStorage() {
		// TODO check if we can make the StorageAccount + Container here -> solves the ConnectionString hardCoded
		this.containerClient = new BlobContainerClientBuilder()
				.connectionString(storageConnectionString)
				.containerName(CONTAINER_NAME)
				.buildClient();
	}

	@Override
	public Result<Void> write(String path, byte[] bytes) {
		if (path == null)
			return error(BAD_REQUEST);

		try {
			BinaryData data = BinaryData.fromBytes(bytes);

			BlobClient blob = containerClient.getBlobClient(path);

			if (blob.exists()) {
				byte[] blobData = blob.downloadContent().toBytes();
				if (Arrays.equals(Hash.sha256(bytes), Hash.sha256(blobData)))
					return ok();
				else
					return error(CONFLICT);
			}

			blob.upload(data);
			
			log.info( "File uploaded : " + path);
			
			return ok();
		} catch( Exception e) {
			log.warning(e.getMessage());
			return error(INTERNAL_ERROR);
		}
	}

	@Override
	public Result<byte[]> read(String path) {
		if (path == null)
			return error(BAD_REQUEST);
		
		try {
			BlobClient blob = containerClient.getBlobClient(path);

			if(!blob.exists()) {
				return error(NOT_FOUND);
			}

			BinaryData data = blob.downloadContent();

			byte[] arr = data.toBytes();
			
			log.info( "Blob size : " + arr.length);

			return ok(arr);
		} catch( Exception e) {
			log.warning(e.getMessage());
			return error(INTERNAL_ERROR);
		}
	}

	@Override
	public Result<Void> read(String path, Consumer<byte[]> sink) {
		if (path == null)
			return error(BAD_REQUEST);

		try {
			BlobClient blob = containerClient.getBlobClient(path);

			if(!blob.exists()) {
				return error(NOT_FOUND);
			}

			BinaryData data = blob.downloadContent();
			byte[] arr = data.toBytes();
			
			log.info( "Blob size : " + arr.length);

			return read(arr, sink);
		} catch( Exception e) {
			log.warning(e.getMessage());
			return error(INTERNAL_ERROR);
		}
	}
	
	@Override
	public Result<Void> delete(String path) {
		if (path == null)
			return error(BAD_REQUEST);

		try {
			BlobClient blob = containerClient.getBlobClient(path);

			if(!blob.exists()) {
				return error(NOT_FOUND);
			}

			blob.delete();

			String[] filePath = path.split("/");
			String filename = filePath[filePath.length - 1];

			log.info("File deleted : " + filename);

			return ok();
		} catch( Exception e) {
			log.warning(e.getMessage());
			return error(INTERNAL_ERROR);
		}
	}

	private Result<Void> read(byte[] data, Consumer<byte[]> sink) {
		try (InputStream is = new ByteArrayInputStream(data)) {
			int n;
			byte[] chunk = new byte[CHUNK_SIZE];

			while ((n = is.read(chunk, 0, data.length)) != -1) {
				sink.accept(Arrays.copyOf(chunk, n));
			}
			return ok();
		} catch (IOException x) {
			return error(INTERNAL_ERROR);
		}
	}
}
