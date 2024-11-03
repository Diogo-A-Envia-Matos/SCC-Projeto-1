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

public class AzureSystemStorage implements BlobStorage {
	private static final int CHUNK_SIZE = 4096;
	private static final String CONTAINER_NAME = "blobs";
	// Must get the string by hand - ConnectionString might change in the future
	// private static final String storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=scc54471;AccountKey=Y3VlAt+RbAknLVeO2MWnKLfi7yFPsdczv0H+VAZxbuaLH2pz304RUpSd0vwB67niUDW9rQZn5C6M+AStR0bxpQ==;EndpointSuffix=core.windows.net";
	private static final String storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=sto70252northeurope;AccountKey=PF3Twe1wi0i3QNvHnvjnHuOoHOgQggijK4pR8653jq1t+xfwOWLfszQA6dkE2iRvsGFzEo82fMWm+ASt15Xozg==;EndpointSuffix=core.windows.net";

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

			try (var jedis = RedisCache.getCachePool().getResource()) {
				var separatedPath = path.split("/");
				var cacheId = getCacheId(separatedPath[separatedPath.length - 1]);
				if (jedis.exists(cacheId)) {
					return Result.error( ErrorCode.CONFLICT );
				}
					
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

				var value = JSON.encode(bytes);
				jedis.set(cacheId, value);
				
				return ok();
			} catch (Exception e) {
				e.printStackTrace();
				throw e;
			}

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

			try (var jedis = RedisCache.getCachePool().getResource()) {
				var separatedPath = path.split("/");
				var cacheId = getCacheId(separatedPath[separatedPath.length - 1]);
				log.info( "Before get");
				var obj = jedis.get(cacheId);
				log.info( "After get");
				if (obj != null) {
					log.info( "Before decode");
					var data = JSON.decode(obj, Object.class);

					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					try (ObjectOutputStream stream = new ObjectOutputStream(baos)) {
						stream.writeObject(data);
						byte[] arr = baos.toByteArray();
						
						log.info( "Blob size : " + arr.length);

						return Result.ok(arr);
					} catch (IOException ioe) {
						ioe.printStackTrace();
					}
					throw new RuntimeException();

				}
			} catch (Exception e) {
				e.printStackTrace();
				throw e;
			}

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
	public Result<Void> delete(String path) {
		if (path == null)
			return error(BAD_REQUEST);

		try {

			String[] filePath = path.split("/");
			String filename = filePath[filePath.length - 1];

			try (var jedis = RedisCache.getCachePool().getResource()) {
				var cacheId = getCacheId(filename);
				jedis.del(cacheId);
				
				BlobClient blob = containerClient.getBlobClient(path);

				if(!blob.exists()) {
					return error(NOT_FOUND);
				}

				blob.delete();

				log.info("File deleted : " + filename);

				return ok();

			} catch (Exception e) {
				e.printStackTrace();
				throw e;
			}

		} catch( Exception e) {
			log.warning(e.getMessage());
			return error(INTERNAL_ERROR);
		}
	}

	private String getCacheId(String id) {
		return "blob:" + id;
	}
}
