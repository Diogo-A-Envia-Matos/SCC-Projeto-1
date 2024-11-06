package tukano.impl.storage;

import static tukano.api.Result.ErrorCode.*;
import static tukano.api.Result.*;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;

import exceptions.InvalidClassException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.logging.Logger;
import tukano.api.Result;
import tukano.api.Result.ErrorCode;
import tukano.api.User;
import tukano.impl.data.Following;
import tukano.impl.data.Likes;
import utils.GetId;
import utils.Hash;
import utils.JSON;
import utils.RedisCache;
import utils.Props;

public class AzureSystemStorage implements BlobStorage {
	private static final int CHUNK_SIZE = 4096;
	private static final String CONTAINER_NAME = "blobs";
	// Must get the string by hand - ConnectionString might change in the future
	// private static final String storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=scc54471;AccountKey=Y3VlAt+RbAknLVeO2MWnKLfi7yFPsdczv0H+VAZxbuaLH2pz304RUpSd0vwB67niUDW9rQZn5C6M+AStR0bxpQ==;EndpointSuffix=core.windows.net";
	private static final String storageConnectionString = Props.get("BlobStoreConnection", "");

	private static final Logger log = Logger.getLogger(AzureSystemStorage.class.getName());
	private final BlobContainerClient containerClient;

	public AzureSystemStorage() {
		// TODO check if we can make the StorageAccount + Container here -> solves the ConnectionString hardCoded
		this.containerClient = new BlobContainerClientBuilder()
				.connectionString(storageConnectionString)
				.containerName(CONTAINER_NAME)
				.buildClient();

		// TODO verify if we will have single or mutiple containers
		if(!containerClient.exists()) {
			containerClient.create();
		}
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

			var correctedPath = path.replace("+", "/");

			BlobClient blob = containerClient.getBlobClient(correctedPath);

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
	public Result<Void> delete(String path) {
		if (path == null)
			return error(BAD_REQUEST);

		try {

			// String[] filePath = path.split("+");
			// String filename = filePath[filePath.length - 1];
			
			var correctedPath = path.replace("+", "/");

			BlobClient blob = containerClient.getBlobClient(correctedPath);

			blob.delete();

			log.info("File deleted : " + correctedPath);

			return ok();

		} catch( Exception e) {
			log.warning(e.getMessage());
			return error(INTERNAL_ERROR);
		}
	}
}
