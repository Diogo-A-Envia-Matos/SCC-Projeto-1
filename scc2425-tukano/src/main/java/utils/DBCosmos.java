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

import exceptions.InvalidClassException;
import tukano.api.Result;
import tukano.api.Short;
import tukano.api.Users;
import tukano.api.Result.ErrorCode;
import tukano.impl.data.Following;
import tukano.impl.data.Likes;

//TODO: Update this file
//TODO: ADD Redis Cache
//TODO: Separate Cache functions for transaction
public class DBCosmos implements DB {
	private static final String CONNECTION_URL = "https://sccdb70252.documents.azure.com:443/"; // replace with your own
	private static final String DB_KEY = "6RBrEE6YTE67v9v4h9MNfbgAcj4AjLAwzxQhwNeMAPFrNRcTkNFPdGhvW16Lx0zperURFz4IUgtkACDbGXfDPw==";
	private static final String DB_NAME = "sccdatabase70252";
	private static final String USERS_CONTAINER = "users";
	private static final String SHORTS_CONTAINER = "shorts";
	private static final String FOLLOWINGS_CONTAINER = "followings";
	private static final String LIKES_CONTAINER = "likes";
	//private static final String PARTITION_KEY = "id";
	public static final PartitionKey PARTITION_KEY = new PartitionKey("id");

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
	private CosmosContainer userContainer;
	private CosmosContainer shortContainer;
	private CosmosContainer followingsContainer;
	private CosmosContainer likesContainer;
	// private CosmosBatch batch;
	
	public DBCosmos(CosmosClient client) {
		this.client = client;
	}

	private synchronized void init() {
		if( db != null)
			return;
		db = client.getDatabase(DB_NAME);
		userContainer = db.getContainer(USERS_CONTAINER);
		shortContainer = db.getContainer(SHORTS_CONTAINER);
		followingsContainer = db.getContainer(FOLLOWINGS_CONTAINER);
		likesContainer = db.getContainer(LIKES_CONTAINER);
		// batch = CosmosBatch.createCosmosBatch(PARTITION_KEY);
	}

	public <T> List<T> sql(String query, Class<T> clazz) {
		try {
			init();
			var res = getClassContainer(clazz).queryItems(query, new CosmosQueryRequestOptions(), clazz);
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
			var res = getClassContainer(clazz).queryItems(String.format(fmt, args), new CosmosQueryRequestOptions(), clazz);
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
			var cacheId = getCacheId(id, clazz);
			var obj = jedis.get(cacheId);
			if (obj != null) {
				var object = JSON.decode(obj, clazz);
				return Result.ok(object);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		
		return tryCatch( () -> getClassContainer(clazz).readItem(id, PARTITION_KEY, clazz).getItem());
	}
	
	//TODO: Lookup how to delete
	//TODO: Check if it was Result<?>
	public <T> Result<T> deleteOne(T obj) {
		try {
			init();
			//return Result.ok(supplierFunc.get());
			//TODO: Get the id of the object
			//TODO: Must delete specific object
			CosmosItemResponse<?> res = getClassContainer(obj.getClass()).deleteItem(GetId.getId(obj), new PartitionKey(PARTITION_KEY), new CosmosItemRequestOptions());
			// CosmosItemResponse<?> res = container.deleteItem(obj, new CosmosItemRequestOptions());
			if( res.getStatusCode() < 300) {
				try (var jedis = RedisCache.getCachePool().getResource()) {
					var id = GetId.getId(obj);
					var clazz = obj.getClass();
					var cacheId = getCacheId(id, clazz);
					jedis.del(cacheId);
				} catch (Exception e) {
					e.printStackTrace();
					throw e;
				}
				return Result.ok(obj);
			}
			else {
				return Result.error( ErrorCode.INTERNAL_ERROR);	
			}
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
		try {
			var res = tryCatch( () -> getClassContainer(obj.getClass()).replaceItem(obj, GetId.getId(obj), PARTITION_KEY, new CosmosItemRequestOptions()).getItem());

			if (!res.isOK()) {
				return Result.error(res.error());
			}

			try (var jedis = RedisCache.getCachePool().getResource()) {
				var id = GetId.getId(obj);
				var clazz = obj.getClass();
				var cacheId = getCacheId(id, clazz);
				var value = JSON.encode( obj );
				jedis.set(cacheId, value);
			} catch (Exception e) {
				e.printStackTrace();
				throw e;
			}

			return Result.ok(obj);
		} catch( CosmosException ce ) {
			//ce.printStackTrace();
			return Result.error ( errorCodeFromStatus(ce.getStatusCode() ));		
		} catch( Exception x ) {
			x.printStackTrace();
			return Result.error( ErrorCode.INTERNAL_ERROR);						
		}

	}
	
	//TODO: Insert partition key (based on object class)
	public <T> Result<T> insertOne( T obj) {
		try {
			try (var jedis = RedisCache.getCachePool().getResource()) {
				var id = GetId.getId(obj);
				var clazz = obj.getClass();
				var cacheId = getCacheId(id, clazz);
				var value = JSON.encode( obj );
				if (jedis.exists(cacheId)) {
					return Result.error( ErrorCode.CONFLICT );
				}
				var res = tryCatch( () -> getClassContainer(clazz).createItem(obj).getItem());
				if (!res.isOK()) {
					return Result.error(res.error());
				} else {
					jedis.set(cacheId, value);
					return Result.ok(obj);
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw e;
			}
		} catch( CosmosException ce ) {
			//ce.printStackTrace();
			return Result.error ( errorCodeFromStatus(ce.getStatusCode() ));		
		} catch( Exception x ) {
			x.printStackTrace();
			return Result.error( ErrorCode.INTERNAL_ERROR);						
		}

		//TODO: Perguntar como tratar deste erro
		// return null;
	}

	
	//TODO: Read azure documentation
	//TODO: Decidir o que fazer com transaction
	//TODO: Separar os varios pedidos
	//TODO: Usar RedisTransactions
	//TODO: Indicar as classes a serem afetadas (Map<Class<T>, CosmosBatch)
	// Nao pode fazer tudo por falta de batches, e necessario criar situacoes especiais
	public <T> Result<T> transaction(CosmosBatch batch, Class<T> clazz) {
		try {
			init();
			CosmosBatchResponse response;
			if (clazz.equals(User.class)) {
				response = userContainer.executeCosmosBatch(batch);
			} else if (clazz.equals(Short.class)) {
				response = shortContainer.executeCosmosBatch(batch);
			} else if (clazz.equals(Following.class)) {
				response = followingsContainer.executeCosmosBatch(batch);
			} else if (clazz.equals(Likes.class)) {
				response = likesContainer.executeCosmosBatch(batch);
			} else {
				return Result.error( ErrorCode.INTERNAL_ERROR);
			}
			
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
	//TODO: Usar RedisTransactions
	//TODO: Devia usar transaction ou simplesmente colocar em ordem
	//TODO: Devia implementar em lista com Operations e Objetos
	public <T> Result<T> transaction(Map<Operations, List<Object>> operations) {
		try {
			init();
			List<CosmosItemOperation> userCosmosItemOperations = new ArrayList<CosmosItemOperation>();
			List<CosmosItemOperation> shortCosmosItemOperations = new ArrayList<CosmosItemOperation>();
			List<CosmosItemOperation> followingCosmosItemOperations = new ArrayList<CosmosItemOperation>();
			List<CosmosItemOperation> likeCosmosItemOperations = new ArrayList<CosmosItemOperation>();
			var addOperations = operations.get(Operations.ADD);
			var readOperations = operations.get(Operations.READ);
			var replaceOperations = operations.get(Operations.REPLACE);
			var deleteOperations = operations.get(Operations.DELETE);
			if (addOperations != null) {
				for (Object item: addOperations) {
					if (item.getClass().equals(User.class)) {
						userCosmosItemOperations.add(CosmosBulkOperations.getCreateItemOperation(GetId.getId(item), PARTITION_KEY));
					} else if (item.getClass().equals(Short.class)) {
						shortCosmosItemOperations.add(CosmosBulkOperations.getCreateItemOperation(GetId.getId(item), PARTITION_KEY));
					} else if (item.getClass().equals(Following.class)) {
						followingCosmosItemOperations.add(CosmosBulkOperations.getCreateItemOperation(GetId.getId(item), PARTITION_KEY));
					} else if (item.getClass().equals(Likes.class)) {
						likeCosmosItemOperations.add(CosmosBulkOperations.getCreateItemOperation(GetId.getId(item), PARTITION_KEY));
					}
					//TODO: Adicionar a Cache
				}
			}
			if (readOperations != null) {
				for (Object item: readOperations) {
					if (item.getClass().equals(User.class)) {
						userCosmosItemOperations.add(CosmosBulkOperations.getReadItemOperation(GetId.getId(item), PARTITION_KEY));
					} else if (item.getClass().equals(Short.class)) {
						shortCosmosItemOperations.add(CosmosBulkOperations.getReadItemOperation(GetId.getId(item), PARTITION_KEY));
					} else if (item.getClass().equals(Following.class)) {
						followingCosmosItemOperations.add(CosmosBulkOperations.getReadItemOperation(GetId.getId(item), PARTITION_KEY));
					} else if (item.getClass().equals(Likes.class)) {
						likeCosmosItemOperations.add(CosmosBulkOperations.getReadItemOperation(GetId.getId(item), PARTITION_KEY));
					}
					//TODO: Ler da Cache
				}
			}
			if (replaceOperations != null) {
				for (Object item: replaceOperations) {
					if (item.getClass().equals(User.class)) {
						userCosmosItemOperations.add(CosmosBulkOperations.getDeleteItemOperation(GetId.getId(item), PARTITION_KEY));
					} else if (item.getClass().equals(Short.class)) {
						shortCosmosItemOperations.add(CosmosBulkOperations.getDeleteItemOperation(GetId.getId(item), PARTITION_KEY));
					} else if (item.getClass().equals(Following.class)) {
						followingCosmosItemOperations.add(CosmosBulkOperations.getDeleteItemOperation(GetId.getId(item), PARTITION_KEY));
					} else if (item.getClass().equals(Likes.class)) {
						likeCosmosItemOperations.add(CosmosBulkOperations.getDeleteItemOperation(GetId.getId(item), PARTITION_KEY));
					}
					//TODO: Trocar da Cache
				}
			}
			if (deleteOperations != null) {
				for (Object item: deleteOperations) {
					if (item.getClass().equals(User.class)) {
						userCosmosItemOperations.add(CosmosBulkOperations.getDeleteItemOperation(GetId.getId(item), PARTITION_KEY));
					} else if (item.getClass().equals(Short.class)) {
						shortCosmosItemOperations.add(CosmosBulkOperations.getDeleteItemOperation(GetId.getId(item), PARTITION_KEY));
					} else if (item.getClass().equals(Following.class)) {
						followingCosmosItemOperations.add(CosmosBulkOperations.getDeleteItemOperation(GetId.getId(item), PARTITION_KEY));
					} else if (item.getClass().equals(Likes.class)) {
						likeCosmosItemOperations.add(CosmosBulkOperations.getDeleteItemOperation(GetId.getId(item), PARTITION_KEY));
					}
					//TODO: Remover da Cache
				}
			}
			var response1 = userContainer.executeBulkOperations(userCosmosItemOperations);
			var response2 = shortContainer.executeBulkOperations(userCosmosItemOperations);
			var response3 = followingsContainer.executeBulkOperations(userCosmosItemOperations);
			var response4 = likesContainer.executeBulkOperations(userCosmosItemOperations);
			for (CosmosBulkOperationResponse<Object> res: response1) {
				if (!res.getResponse().isSuccessStatusCode())
					return Result.error(ErrorCode.INTERNAL_ERROR);
			}
			for (CosmosBulkOperationResponse<Object> res: response2) {
				if (!res.getResponse().isSuccessStatusCode())
					return Result.error(ErrorCode.INTERNAL_ERROR);
			}
			for (CosmosBulkOperationResponse<Object> res: response3) {
				if (!res.getResponse().isSuccessStatusCode())
					return Result.error(ErrorCode.INTERNAL_ERROR);
			}
			for (CosmosBulkOperationResponse<Object> res: response4) {
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

	private <T> String getCacheId(String id, Class<T> clazz) {
		try {
			if (clazz.equals(User.class)) {
				return "user:" + id;
			} else if (clazz.equals(Short.class)) {
				return "short:" + id;
			} else if (clazz.equals(Following.class)) {
				return "following:" + id;
			} else if (clazz.equals(Likes.class)) {
				return "like:" + id;
			}
			throw new InvalidClassException("Invalid Class: " + clazz.toString());
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	private <T> CosmosContainer getClassContainer(Class<T> clazz) {
		try {
			if (clazz.equals(User.class)) {
				return userContainer;
			} else if (clazz.equals(Short.class)) {
				return shortContainer;
			} else if (clazz.equals(Following.class)) {
				return followingsContainer;
			} else if (clazz.equals(Likes.class)) {
				return likesContainer;
			}
			throw new InvalidClassException("Invalid Class: " + clazz.toString());
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
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
