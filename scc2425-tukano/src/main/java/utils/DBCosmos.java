package utils;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.function.Supplier;

import org.hsqldb.rights.User;

import java.util.function.Consumer;
import java.util.function.Function;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosBatch;
import com.azure.cosmos.models.CosmosBatchResponse;
import com.azure.cosmos.models.CosmosBulkOperationResponse;
import com.azure.cosmos.models.CosmosBulkOperations;
import com.azure.cosmos.models.CosmosItemOperation;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;

import tukano.api.Result;
import tukano.api.Short;
import tukano.api.Users;
import tukano.api.Result.ErrorCode;
import tukano.impl.data.Following;
import tukano.impl.data.Likes;

//TODO: Update this file
//TODO: ADD Redis Cache
public class DBCosmos implements DB {
	private static final String CONNECTION_URL = "https://sccdb70252.documents.azure.com:443/"; // replace with your own
	private static final String DB_KEY = "6RBrEE6YTE67v9v4h9MNfbgAcj4AjLAwzxQhwNeMAPFrNRcTkNFPdGhvW16Lx0zperURFz4IUgtkACDbGXfDPw==";
	private static final String DB_NAME = "sccdatabase70252";
	private static final String USERS_CONTAINER = "users";
	private static final String SHORTS_CONTAINER = "shorts";
	private static final String FOLLOWINGS_CONTAINER = "followings";
	private static final String LIKES_CONTAINER = "likes";
	//private static final String PARTITION_KEY = "id";
	private static final PartitionKey PARTITION_KEY = new PartitionKey("id");

	// PARTITION_KEY talvez seja "/id"

	private static DBCosmos instance;

	public static synchronized DBCosmos getInstance() {
		if( instance != null)
			return instance;

		CosmosClient client = new CosmosClientBuilder()
		         .endpoint(CONNECTION_URL)
		         .key(DB_KEY)
		         //.directMode()
		         .gatewayMode()
		         // replace by .directMode() for better performance
		         .consistencyLevel(ConsistencyLevel.SESSION)
		         .connectionSharingAcrossClientsEnabled(true)
		         .contentResponseOnWriteEnabled(true)
		         .buildClient();
		instance = new DBCosmos( client);
		return instance;
		
	}
	
	private CosmosClient client;
	private CosmosDatabase db;
	private CosmosContainer container;
	// private CosmosBatch batch;
	
	public DBCosmos(CosmosClient client) {
		this.client = client;
	}

	private synchronized void init() {
		if( db != null)
			return;
		db = client.getDatabase(DB_NAME);
		container = db.getContainer(USERS_CONTAINER);
		// batch = CosmosBatch.createCosmosBatch(PARTITION_KEY);
	}

	public <T> List<T> sql(String query, Class<T> clazz) {
		try {
			init();
			var res = container.queryItems(query, new CosmosQueryRequestOptions(), clazz);
			return res.stream().toList();		
		} catch( CosmosException ce ) {
			//ce.printStackTrace();
			//TODO: ADD error code
			//TODO: Check how to return the error
			//TODO: Check if null is correct
			//return Result.error ( errorCodeFromStatus(ce.getStatusCode() ));		
			throw ce;
		} catch( Exception x ) {
			x.printStackTrace();
			//TODO: ADD error code
			//return Result.error( ErrorCode.INTERNAL_ERROR);	
			throw x;
		}
			
	}
	
	//TODO: Update
	public <T> List<T> sql(Class<T> clazz, String fmt, Object ... args) {
		try {
			init();
			var res = container.queryItems(String.format(fmt, args), new CosmosQueryRequestOptions(), clazz);
			return res.stream().toList();		
		} catch( CosmosException ce ) {
			//TODO: Check if null is correct
			throw ce;
		} catch( Exception x ) {
			x.printStackTrace();
			//TODO: ADD error code		
			throw x;
		}
	}
	
	//TODO: Put objects in separate containers
	//TODO: This function is built for cache, not database. Update it for database
	//TODO: Decide if it's better to keep cache functions on "RedisCache", or have it be only for the cache itself
	public <T> Result<T> getOne(String id, Class<T> clazz) {
		try (var jedis = RedisCache.getCachePool().getResource()) {
			if (clazz.equals(Users.class)) {
				var user = jedis.get(USERS_CONTAINER + ":" + id);
				if (user != null) {
					var object = JSON.decode(user, clazz);
					return Result.ok(object);
				}
			} else if (clazz.equals(Short.class)) {
				var shrt = jedis.get(SHORTS_CONTAINER + ":" + id);
				if (shrt != null) {
					var object = JSON.decode(shrt, clazz);
					return Result.ok(object);
				}
			} else if (clazz.equals(Following.class)) {
				var follow = jedis.get(FOLLOWINGS_CONTAINER + ":" + id);
				if (follow != null) {
					var object = JSON.decode(follow, clazz);
					return Result.ok(object);
				}
			} else if (clazz.equals(Likes.class)) {
				var like = jedis.get(LIKES_CONTAINER + ":" + id);
				if (like != null) {
					var object = JSON.decode(like, clazz);
					return Result.ok(object);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		// try {
		// 	if (clazz.equals(Users.class)) {
				
		// 	} else if (clazz.equals(Short.class)) {
		// 		var shrt = jedis.get(SHORTS_CONTAINER + id);
		// 		if (shrt != null) {
		// 			var object = JSON.decode(shrt, clazz);
		// 			return Result.ok(object);
		// 		}
		// 	} else if (clazz.equals(Following.class)) {
		// 		var follow = jedis.get(FOLLOWINGS_CONTAINER + id);
		// 		if (follow != null) {
		// 			var object = JSON.decode(follow, clazz);
		// 			return Result.ok(object);
		// 		}
		// 	} else if (clazz.equals(Likes.class)) {
		// 		var like = jedis.get(LIKES_CONTAINER + id);
		// 		if (like != null) {
		// 			var object = JSON.decode(like, clazz);
		// 			return Result.ok(object);
		// 		}
		// 	}
		// } catch (Exception e) {
		// 	e.printStackTrace();
		// 	throw e;
		// }
			return tryCatch( () -> container.readItem(id, new PartitionKey(id), clazz).getItem());
	}
	
	//TODO: Lookup how to delete
	//TODO: Check if it was Result<?>
	public <T> Result<T> deleteOne(T obj) {
		try {
			init();
			//return Result.ok(supplierFunc.get());
			//TODO: Get the id of the object
			//TODO: Must delete specific object
			//CosmosItemResponse<?> res = container.deleteItem(GetId.getId(obj), new PartitionKey(PARTITION_KEY), new CosmosItemRequestOptions());
			CosmosItemResponse<?> res = container.deleteItem(obj, new CosmosItemRequestOptions());
			if( res.getStatusCode() < 300)
				return Result.ok(obj);
			else
			return Result.error( ErrorCode.INTERNAL_ERROR);	
		} catch( CosmosException ce ) {
			//ce.printStackTrace();
			return Result.error ( errorCodeFromStatus(ce.getStatusCode() ));		
		} catch( Exception x ) {
			x.printStackTrace();
			return Result.error( ErrorCode.INTERNAL_ERROR);						
		}



		//return tryCatch( () -> container.deleteItem(obj, new CosmosItemRequestOptions()).getItem());
	}
	
	//TODO: Insert partition key (based on object class)
	public <T> Result<T> updateOne(T obj) {
		return tryCatch( () -> container.upsertItem(obj).getItem());
	}
	
	//TODO: Insert partition key (based on object class)
	public <T> Result<T> insertOne( T obj) {
		return tryCatch( () -> container.createItem(obj).getItem());
	}

	
	//TODO: Read azure documentation
	//TODO: Decidir o que fazer com transaction
	//TODO: Separar os varios pedidos
	// Nao pode fazer tudo por falta de batches, e necessario criar situacoes especiais
	public <T> Result<T> transaction( CosmosBatch batch) {
		try {
			init();
			CosmosBatchResponse response = container.executeCosmosBatch(batch);
			if (response.isSuccessStatusCode())
				return Result.ok();
			else {
				return Result.error(ErrorCode.CONFLICT);
			}			
		} catch( CosmosException ce ) {
			//ce.printStackTrace();
			return Result.error ( errorCodeFromStatus(ce.getStatusCode() ));		
		} catch( Exception x ) {
			x.printStackTrace();
			return Result.error( ErrorCode.INTERNAL_ERROR);						
		}
		// return tryCatch( () -> container.executeCosmosBatch(batch).getResults());
		// CosmosBatchResponse response = container.executeCosmosBatch(batch);
		// return Result.ok(obj);
	}
	
	//TODO: Add remaining operations
	//TODO: Add way to cancel changes
	public <T> Result<T> transaction(Map<Operations, List<Object>> operations) {
		try {
			init();
			List<CosmosItemOperation> cosmosItemOperations = new ArrayList<CosmosItemOperation>();
			// var addOperations = operations.get(Operations.ADD);
			// var readOperations = operations.get(Operations.READ);
			// var replaceOperations = operations.get(Operations.REPLACE);
			var deleteOperations = operations.get(Operations.DELETE);
			if (deleteOperations != null) {
				for (Object item: deleteOperations) {
					cosmosItemOperations.add(CosmosBulkOperations.getDeleteItemOperation(GetId.getId(item), PARTITION_KEY));
				}
			}
			var response = container.executeBulkOperations(cosmosItemOperations);
			for (CosmosBulkOperationResponse<Object> res: response) {
				if (!res.getResponse().isSuccessStatusCode())
					return Result.error(ErrorCode.INTERNAL_ERROR);
			}
			return Result.ok();
		} catch( CosmosException ce ) {
			//ce.printStackTrace();
			return Result.error ( errorCodeFromStatus(ce.getStatusCode() ));		
		} catch( Exception x ) {
			x.printStackTrace();
			return Result.error( ErrorCode.INTERNAL_ERROR);						
		}
	}
	
	//TODO: Read azure documentation
	//TODO: Decidir o que fazer com transaction
	// Nao pode fazer tudo por falta de batches, e necessario criar situacoes especiais
	// public <T> Result<T> transaction( CosmosBatch batch) {
	// 	try {
	// 		init();
	// 		CosmosBatchResponse response = container.executeCosmosBatch(batch);
	// 		if (response.isSuccessStatusCode())
	// 			return Result.ok();
	// 		else {
	// 			return Result.error(ErrorCode.CONFLICT);
	// 		}			
	// 	} catch( CosmosException ce ) {
	// 		//ce.printStackTrace();
	// 		return Result.error ( errorCodeFromStatus(ce.getStatusCode() ));		
	// 	} catch( Exception x ) {
	// 		x.printStackTrace();
	// 		return Result.error( ErrorCode.INTERNAL_ERROR);						
	// 	}
	// 	// return tryCatch( () -> container.executeCosmosBatch(batch).getResults());
	// 	// CosmosBatchResponse response = container.executeCosmosBatch(batch);
	// 	// return Result.ok(obj);
	// }
	
	// public <T> Result<T> transaction( Function<Session, Result<T>> func) {
	// 	return Hibernate.getInstance().execute( func );
	// }

	<T> Result<T> tryCatch( Supplier<T> supplierFunc) {
		try {
			init();
			return Result.ok(supplierFunc.get());			
		} catch( CosmosException ce ) {
			//ce.printStackTrace();
			return Result.error ( errorCodeFromStatus(ce.getStatusCode() ));		
		} catch( Exception x ) {
			x.printStackTrace();
			return Result.error( ErrorCode.INTERNAL_ERROR);						
		}
	}
	
	static Result.ErrorCode errorCodeFromStatus( int status ) {
		return switch( status ) {
		case 200 -> ErrorCode.OK;
		case 404 -> ErrorCode.NOT_FOUND;
		case 409 -> ErrorCode.CONFLICT;
		default -> ErrorCode.INTERNAL_ERROR;
		};
	}

}
