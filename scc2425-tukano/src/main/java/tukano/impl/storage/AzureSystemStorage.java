package tukano.impl.storage;


import static tukano.api.Result.error;
import static tukano.api.Result.ok;
import static tukano.api.Result.ErrorCode.BAD_REQUEST;
import static tukano.api.Result.ErrorCode.CONFLICT;
import static tukano.api.Result.ErrorCode.INTERNAL_ERROR;
import static tukano.api.Result.ErrorCode.NOT_FOUND;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.function.Consumer;

import org.hsqldb.types.BlobInputStream;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.common.StorageInputStream;

import jakarta.ws.rs.WebApplicationException;
import tukano.api.Result;
import utils.Hash;
import utils.IO;

//TODO: Use utils.AzureIO to reduce redundancy
public class AzureSystemStorage implements BlobStorage {
	private final String rootDir;
	private static final int CHUNK_SIZE = 4096;

	//TODO: Add this container later
	private static final String DEFAULT_ROOT_DIR = "blobs";

	//ConnectionString might change in the future
	private static final String storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=scc70252;AccountKey=scgLUp7BRjLdo7RUDA5fVb9s9wlCx6lV7HLT49jf+z9ojIhx/vI0+6r08UvdouMtFZ4PZwQdsB+k+AStOxztSA==;EndpointSuffix=core.windows.net";

	public AzureSystemStorage() {
		this.rootDir = DEFAULT_ROOT_DIR;
	}
	
	@Override
	public Result<Void> write(String path, byte[] bytes) {
		if (path == null)
			return error(BAD_REQUEST);

		//var file = toFile( path );

		// if (file.exists()) {
		// 	if (Arrays.equals(Hash.sha256(bytes), Hash.sha256(IO.read(file))))
		// 		return ok();
		// 	else
		// 		return error(CONFLICT);

		// }

		var key = Hash.of(bytes);
		try {
			BinaryData data = BinaryData.fromBytes(bytes);

			// Get container client
			BlobContainerClient containerClient = new BlobContainerClientBuilder()
														.connectionString(storageConnectionString)
														.containerName(DEFAULT_ROOT_DIR)
														.buildClient();

			// Get client to blob
			String filename = key;
			BlobClient blob = containerClient.getBlobClient( filename);

			if (blob.exists()) {
				byte[] blobData = blob.downloadContent().toBytes();
				if (Arrays.equals(Hash.sha256(bytes), Hash.sha256(blobData)))
					return ok();
				else
					return error(CONFLICT);
			}

			// Upload contents from BinaryData (check documentation for other alternatives)
			blob.upload(data);
			
			System.out.println( "File uploaded : " + filename);
			
			return ok();
		} catch( Exception e) {
			//TODO: Perguntar como devia tratar desta excecao
			return error(INTERNAL_ERROR);
		}
	}

	@Override
	public Result<byte[]> read(String path) {
		if (path == null)
			return error(BAD_REQUEST);
		
		//var file = toFile( path );
		
		try {
			// Get container client
			BlobContainerClient containerClient = new BlobContainerClientBuilder()
														.connectionString(storageConnectionString)
														.containerName(DEFAULT_ROOT_DIR)
														.buildClient();

			// Get client to blob
			BlobClient blob = containerClient.getBlobClient(path);

			if( ! blob.exists() )
				return error(NOT_FOUND);

			// Download contents to BinaryData (check documentation for other alternatives)
			BinaryData data = blob.downloadContent();

			byte[] arr = data.toBytes();
			
			System.out.println( "Blob size : " + arr.length);
			return ok(arr);
		} catch( Exception e) {
			//TODO: Perguntar como devia tratar desta excecao
			return error(INTERNAL_ERROR);
		}
	}

	@Override
	public Result<Void> read(String path, Consumer<byte[]> sink) {
		if (path == null)
			return error(BAD_REQUEST);
		
		//var file = toFile( path );
		
		try {
			// Get container client
			BlobContainerClient containerClient = new BlobContainerClientBuilder()
														.connectionString(storageConnectionString)
														.containerName(DEFAULT_ROOT_DIR)
														.buildClient();

			// Get client to blob
			BlobClient blob = containerClient.getBlobClient(path);

			if( ! blob.exists() )
				return error(NOT_FOUND);

			// Download contents to BinaryData (check documentation for other alternatives)
			BinaryData data = blob.downloadContent();

			byte[] arr = data.toBytes();
			
			System.out.println( "Blob size : " + arr.length);

			
			//IO.read( file, CHUNK_SIZE, sink );
			try (InputStream is = new ByteArrayInputStream(arr)) {

				int n;
				byte[] chunk = new byte[CHUNK_SIZE];

				while ((n = is.read(chunk, 0, arr.length)) != -1) {
					sink.accept(Arrays.copyOf(chunk, n));
				}
				return ok();
			} catch (IOException x) {
				return error(INTERNAL_ERROR);
			}
			
		} catch( Exception e) {
			//TODO: Perguntar como devia tratar desta excecao
			return error(INTERNAL_ERROR);
		}
	}
	
	//TODO: Procurar Azure para descobrir como fazer delete
	@Override
	public Result<Void> delete(String path) {
		if (path == null)
			return error(BAD_REQUEST);
		
		//var file = toFile( path );
		
		try {
			// Get container client
			BlobContainerClient containerClient = new BlobContainerClientBuilder()
														.connectionString(storageConnectionString)
														.containerName(DEFAULT_ROOT_DIR)
														.buildClient();

			// Get client to blob
			BlobClient blob = containerClient.getBlobClient(path);

			if( ! blob.exists() )
				return error(NOT_FOUND);

			// Download contents to BinaryData (check documentation for other alternatives)
			blob.delete();

			String[] filePath = path.split("/");
			String filename = filePath[filePath.length - 1];
			
			System.out.println( "File deleted : " + filename);
			
			return ok();
		} catch( Exception e) {
			//TODO: Perguntar como devia tratar desta excecao
			return error(INTERNAL_ERROR);
		}

		// try {
		// 	var file = toFile( path );
		// 	Files.walk(file.toPath())
		// 	.sorted(Comparator.reverseOrder())
		// 	.map(Path::toFile)
		// 	.forEach(File::delete);
		// } catch (IOException e) {
		// 	e.printStackTrace();
		// 	return error(INTERNAL_ERROR);
		// }
		// return ok();
	}
	
	//TODO: Adapt this method to work with Azure
	// private File toFile(String path) {
	// 	var res = new File( rootDir + path );
		
	// 	var parent = res.getParentFile();
	// 	if( ! parent.exists() )
	// 		parent.mkdirs();
		
	// 	return res;
	// }

	private File toFile(String path) {
		var res = new File( rootDir + path );
		
		var parent = res.getParentFile();
		if( ! parent.exists() )
			parent.mkdirs();
		
		return res;
	}
	
}
