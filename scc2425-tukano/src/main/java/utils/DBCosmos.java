package utils;

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
import com.azure.cosmos.util.CosmosPagedIterable;
import com.azure.cosmos.models.CosmosItemOperationType;
import com.fasterxml.jackson.databind.JsonNode;
import exceptions.InvalidClassException;
import java.util.function.Consumer;
import java.util.function.Function;
import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Transaction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import tukano.api.Result;
import tukano.api.Result.ErrorCode;
import tukano.api.Short;
import tukano.api.User;
import tukano.impl.data.Following;
import tukano.impl.data.Likes;

//TODO: Update this file
//TODO: Separate Cache functions for transaction
public class DBCosmos implements DB {
	//TODO: Obtain Azure values via "azurekeys-region.props"
	// replace with your own
	private static final String CONNECTION_URL = Props.get("COSMOSDB_URL", ""); // replace with your own
	private static final String DB_KEY = Props.get("COSMOSDB_KEY", "");
	private static final String DB_NAME = Props.get("COSMOSDB_DATABASE", "");

	private static final String USERS_CONTAINER = "users";
	private static final String SHORTS_CONTAINER = "shorts";
	private static final String FOLLOWINGS_CONTAINER = "followings";
	private static final String LIKES_CONTAINER = "likes";
	private static final Logger log = LoggerFactory.getLogger(DBCosmos.class);

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
		instance = new DBCosmos(client);
		return instance;
	}
	
	private final CosmosClient client;
	private CosmosDatabase db;
	private CosmosContainer userContainer;
	private CosmosContainer shortContainer;
	private CosmosContainer followingsContainer;
	private CosmosContainer likesContainer;
	
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
	}

	public <T> List<T> sql(String query, Class<T> containerClazz) {
		try {
			init();
			final CosmosPagedIterable<T> response = getClassContainer(containerClazz)
					.queryItems(query, new CosmosQueryRequestOptions(), containerClazz);
			return response.stream().toList();
		} catch( Exception x ) {
			x.printStackTrace();
			throw x;
		}
	}

	public <T, U> List<U> sql(String query, Class<T> containerClazz, Class<U> expectedClazz) {
		try {
			init();
			final CosmosPagedIterable<JsonNode> response = getClassContainer(containerClazz)
					.queryItems(query, new CosmosQueryRequestOptions(), JsonNode.class);

			return response.stream()
					.map(item -> convertToClass(item, expectedClazz))
					.toList();

		} catch( Exception x ) {
			x.printStackTrace();
			throw x;
		}
	}

	//TODO: Check if it works
	public <T> List<T> sql(Class<T> clazz, String fmt, Object ... args) {
		try {
			init();
			var res = getClassContainer(clazz).queryItems(String.format(fmt, args), new CosmosQueryRequestOptions(), clazz);
			return res.stream().toList();
		} catch( CosmosException ce ) {
			throw ce;
		} catch( Exception x ) {
			x.printStackTrace();
			throw x;
		}
	}

	public <T> Result<T> insertOne( T obj) {
		try (var jedis = RedisCache.getCachePool().getResource()) {
			var id = GetId.getId(obj);
			var clazz = obj.getClass();
			if (clazz == User.class || clazz == Short.class) {
				var cacheId = getCacheId(id, clazz);
				if (jedis.exists(cacheId)) {
					return Result.error( ErrorCode.CONFLICT );
				}
			}

			init();
			CosmosItemResponse<T> response = getClassContainer(obj.getClass()).createItem(obj);
			var res = translateCosmosResponse(response);
			if (!res.isOK()) {
				return Result.error(res.error());
			} else {
				if (clazz == User.class || clazz == Short.class ) {
					var cacheId = getCacheId(id, clazz);
					var value = JSON.encode( obj );
					jedis.set(cacheId, value);
				}
				return Result.ok(obj);
			}

		} catch( CosmosException ce ) {
			ce.printStackTrace();
			return Result.error(errorCodeFromStatus(ce.getStatusCode()));
		} catch( Exception x ) {
			x.printStackTrace();
			return Result.error(ErrorCode.INTERNAL_ERROR);
		}
	}

	public <T> Result<T> getOne(String id, String partition, Class<T> clazz) {
		try (var jedis = RedisCache.getCachePool().getResource()) {
			var cacheId = getCacheId(id, clazz);
			var obj = jedis.get(cacheId);
			if (obj != null) {
				var object = JSON.decode(obj, clazz);
				return Result.ok(object);
			}

			init();
			final PartitionKey partitionKey = new PartitionKey(partition);
			final CosmosItemResponse<T> response = getClassContainer(clazz).readItem(id, partitionKey, clazz);
			var res = translateCosmosResponse(response);
			if (res.isOK()) {
				var value = JSON.encode( obj );
				jedis.set(cacheId, value);
			}
			return res;

		} catch (CosmosException ce) {
			return Result.error(errorCodeFromStatus(ce.getStatusCode()));
		} catch( Exception x ) {
			x.printStackTrace();
			return Result.error(ErrorCode.INTERNAL_ERROR);
		}
	}

	public <T> Result<T> updateOne(T obj) {
		try (var jedis = RedisCache.getCachePool().getResource()) {
			var id = GetId.getId(obj);
			var clazz = obj.getClass();
			if (clazz == User.class || clazz == Short.class) {
				var cacheId = getCacheId(id, clazz);
				var value = JSON.encode( obj );
				jedis.set(cacheId, value);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}

		try {

			init();
			final PartitionKey partitionKey = getPartitionKey(obj);
			CosmosItemResponse<T> response = getClassContainer(obj.getClass())
					.replaceItem(obj, GetId.getId(obj), partitionKey, new CosmosItemRequestOptions());
			return translateCosmosResponse(response);

		} catch( CosmosException ce ) {
			return Result.error ( errorCodeFromStatus(ce.getStatusCode() ));
		} catch( Exception x ) {
			x.printStackTrace();
			return Result.error( ErrorCode.INTERNAL_ERROR);
		}

	}

	public <T> Result<T> deleteOne(T obj) {
		// try {
		try (var jedis = RedisCache.getCachePool().getResource()) {
			var id = GetId.getId(obj);
			var clazz = obj.getClass();
			var cacheId = getCacheId(id, clazz);
			jedis.del(cacheId);

			init();
			CosmosItemResponse<Object> response = getClassContainer(obj.getClass()).deleteItem(obj, new CosmosItemRequestOptions());
			return translateCosmosResponse(response, obj);

		} catch( CosmosException ce ) {
			return Result.error(errorCodeFromStatus(ce.getStatusCode()));
		} catch( Exception x ) {
			x.printStackTrace();
			return Result.error(ErrorCode.INTERNAL_ERROR);
		}
	}

	public <T> List<Result<Void>> deleteCollection(List<T> targets) {
		try {
			init();
			if (targets.isEmpty()) {
				log.warn("DBCosmos deleteCollection received an empty list");
				return List.of();
			}

			List<CosmosItemOperation> operations = targets.stream()
					.map(target -> {
						var targetId = GetId.getId(target);
						PartitionKey partitionKey = getPartitionKey(target);
						return CosmosBulkOperations.getDeleteItemOperation(targetId, partitionKey);
					})
					.toList();

			executeCacheTransactionFromList(operations);

			// try (var jedis = RedisCache.getCachePool().getResource()) {
			// 	var id = GetId.getId(obj);
			// 	var clazz = obj.getClass();
			// 	var cacheId = getCacheId(id, clazz);
			// 	jedis.del(cacheId);
	
			// 	init();
			// 	CosmosItemResponse<Object> response = getClassContainer(obj.getClass()).deleteItem(obj, new CosmosItemRequestOptions());
			// 	return translateCosmosResponse(response, obj);
	
			// } catch( CosmosException ce ) {
			// 	return Result.error(errorCodeFromStatus(ce.getStatusCode()));
			// } catch( Exception x ) {
			// 	x.printStackTrace();
			// 	return Result.error(ErrorCode.INTERNAL_ERROR);
			// }

			Iterable<CosmosBulkOperationResponse<Object>> bulkDeleteOperationResponse = getClassContainer(targets.get(0).getClass()).executeBulkOperations(operations);
			return buildResultList(bulkDeleteOperationResponse);

		} catch( Exception x ) {
			x.printStackTrace();
			return List.of(Result.error(ErrorCode.INTERNAL_ERROR));
		}
	}

	// Nao pode fazer tudo por falta de batches, e necessario criar situacoes especiais
	public <T> Result<T> transaction(CosmosBatch batch, Class<T> clazz) {
		try {
			init();
			CosmosBatchResponse response;
			Transaction cacheTransaction = getCacheTransactionFromBatch(batch, clazz);
			cacheTransaction.exec();
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

	private void executeCacheTransactionFromList(List<CosmosItemOperation> operations) {
		try (var jedis = RedisCache.getCachePool().getResource()) {
			Transaction cacheTransaction = jedis.multi();
				for (CosmosItemOperation op : operations) {
					var item = op.getItem();
					var clazz = item.getClass();
					if (clazz == User.class || clazz == Short.class) {
						var id = GetId.getId(item);
						var value = "";
						switch (op.getOperationType()) {
							case CREATE:
								value = JSON.encode( item );
								cacheTransaction.set(getCacheId(id, clazz), value);
								break;
							case DELETE:
								cacheTransaction.del(getCacheId(id, clazz));
								break;
							case PATCH:
								value = JSON.encode( item );
								cacheTransaction.set(getCacheId(id, clazz), value);
								break;
							case READ:
								cacheTransaction.get(getCacheId(id, clazz));
								break;
							case REPLACE:
								value = JSON.encode( item );
								cacheTransaction.set(getCacheId(op.getItem(), clazz), value);
								break;
							case UPSERT:
								value = JSON.encode( item );
								cacheTransaction.set(getCacheId(op.getItem(), clazz), value);
								break;
							default:
								break;
						}
					}
				}
			cacheTransaction.exec();
			} catch( CosmosException ce ) {
				//ce.printStackTrace();
				throw ce;		
			} catch( Exception x ) {
				x.printStackTrace();
				throw x;
			}
	}

	private <T> Transaction getCacheTransactionFromBatch(CosmosBatch batch, Class<T> clazz) {
		try (var jedis = RedisCache.getCachePool().getResource()) {
			Transaction cacheTransaction = jedis.multi();
			List<CosmosItemOperation> operations = batch.getOperations();
			for (CosmosItemOperation op : operations) {
				var item = op.getItem();
				var id = GetId.getId(item);
				var value = "";
				switch (op.getOperationType()) {
					case CREATE:
						value = JSON.encode( item );
						cacheTransaction.set(getCacheId(id, clazz), value);
						break;
					case DELETE:
						cacheTransaction.del(getCacheId(id, clazz));
						break;
					case PATCH:
						value = JSON.encode( item );
						cacheTransaction.set(getCacheId(id, clazz), value);
						break;
					case READ:
						cacheTransaction.get(getCacheId(id, clazz));
						break;
					case REPLACE:
						value = JSON.encode( item );
						cacheTransaction.set(getCacheId(op.getItem(), clazz), value);
						break;
					case UPSERT:
						value = JSON.encode( item );
						cacheTransaction.set(getCacheId(op.getItem(), clazz), value);
						break;
					default:
						break;
				}
			}
		return cacheTransaction;
		} catch( CosmosException ce ) {
			//ce.printStackTrace();
			throw ce;
		} catch( Exception x ) {
			x.printStackTrace();
			throw x;				
		}
	}
	
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

			Transaction cacheTransaction = null;
			try (var jedis = RedisCache.getCachePool().getResource()) {
				// cacheTransaction = new Transaction(jedis);
				cacheTransaction = jedis.multi();
				if (addOperations != null) {
					for (Object item: addOperations) {
						var id = GetId.getId(item);
						if (item.getClass().equals(User.class)) {
							userCosmosItemOperations.add(CosmosBulkOperations.getCreateItemOperation(id, getPartitionKey((User)item)));
							var value = JSON.encode( item );
							cacheTransaction.set(getCacheId(id, User.class), value);

						} else if (item.getClass().equals(Short.class)) {
							shortCosmosItemOperations.add(CosmosBulkOperations.getCreateItemOperation(id, getPartitionKey((Short)item)));
							var value = JSON.encode( item );
							cacheTransaction.set(getCacheId(id, Short.class), value);

						} else if (item.getClass().equals(Following.class)) {
							followingCosmosItemOperations.add(CosmosBulkOperations.getCreateItemOperation(id, getPartitionKey((Following)item)));

						} else if (item.getClass().equals(Likes.class)) {
							likeCosmosItemOperations.add(CosmosBulkOperations.getCreateItemOperation(id, getPartitionKey((Likes)item)));
						}
					}
				}
				if (readOperations != null) {
					for (Object item: readOperations) {
						var id = GetId.getId(item);
						if (item.getClass().equals(User.class)) {
							userCosmosItemOperations.add(CosmosBulkOperations.getReadItemOperation(id, getPartitionKey((User)item)));
						} else if (item.getClass().equals(Short.class)) {
							shortCosmosItemOperations.add(CosmosBulkOperations.getReadItemOperation(id, getPartitionKey((Short)item)));
						} else if (item.getClass().equals(Following.class)) {
							followingCosmosItemOperations.add(CosmosBulkOperations.getReadItemOperation(id, getPartitionKey((Following)item)));
						} else if (item.getClass().equals(Likes.class)) {
							likeCosmosItemOperations.add(CosmosBulkOperations.getReadItemOperation(id, getPartitionKey((Likes)item)));
						}
					}
				}
				if (replaceOperations != null) {
					for (Object item: replaceOperations) {
						var id = GetId.getId(item);
						if (item.getClass().equals(User.class)) {
							userCosmosItemOperations.add(CosmosBulkOperations.getReplaceItemOperation(GetId.getId(item), item, getPartitionKey((User)item)));
							
							var value = JSON.encode( item );
							cacheTransaction.set(getCacheId(id, User.class), value);
						} else if (item.getClass().equals(Short.class)) {
							userCosmosItemOperations.add(CosmosBulkOperations.getReplaceItemOperation(GetId.getId(item), item, getPartitionKey((Short)item)));
							
							var value = JSON.encode( item );
							cacheTransaction.set(getCacheId(id, Short.class), value);
						} else if (item.getClass().equals(Following.class)) {
							userCosmosItemOperations.add(CosmosBulkOperations.getReplaceItemOperation(GetId.getId(item), item, getPartitionKey((Following)item)));
						} else if (item.getClass().equals(Likes.class)) {
							userCosmosItemOperations.add(CosmosBulkOperations.getReplaceItemOperation(GetId.getId(item), item, getPartitionKey((Likes)item)));
						}
					}
				}
				if (deleteOperations != null) {
					for (Object item: deleteOperations) {
						var id = GetId.getId(item);
						if (item.getClass().equals(User.class)) {
							userCosmosItemOperations.add(CosmosBulkOperations.getDeleteItemOperation(GetId.getId(item), getPartitionKey((User)item)));

							cacheTransaction.del(getCacheId(id, User.class));
						} else if (item.getClass().equals(Short.class)) {
							shortCosmosItemOperations.add(CosmosBulkOperations.getDeleteItemOperation(GetId.getId(item), getPartitionKey((Short)item)));

							cacheTransaction.del(getCacheId(id, Short.class));
						} else if (item.getClass().equals(Following.class)) {
							followingCosmosItemOperations.add(CosmosBulkOperations.getDeleteItemOperation(GetId.getId(item), getPartitionKey((Following)item)));
						} else if (item.getClass().equals(Likes.class)) {
							likeCosmosItemOperations.add(CosmosBulkOperations.getDeleteItemOperation(GetId.getId(item), getPartitionKey((Likes)item)));
						}
					}
				}
				cacheTransaction.exec();
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
			} catch( Exception x ) {
				x.printStackTrace();
				return Result.error( ErrorCode.INTERNAL_ERROR);
			} finally {
				if (cacheTransaction != null) {
					cacheTransaction.close();
				}
			}
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
	}

    private <T> PartitionKey getPartitionKey(T obj) {
        if (obj.getClass().equals(User.class)) {
            return new PartitionKey(GetId.getId(obj));

        } else if (obj.getClass().equals(Short.class)) {
            return new PartitionKey(((Short) obj).getOwnerId());

        } else if (obj.getClass().equals(Following.class)) {
            return new PartitionKey(((Following) obj).getFollowee());

        } else if (obj.getClass().equals(Likes.class)) {
            return new PartitionKey(((Likes) obj).getUserId());
        }

        throw new InvalidClassException("Invalid Class: " + obj.getClass().toString());
    }

	@SuppressWarnings("unchecked")
	private <U> U convertToClass(JsonNode item, Class<U> outputClazz) {
		if (outputClazz.equals(Long.class)) {
			return (U) (Long) item.asLong();
		}

		if (outputClazz.equals(String.class)) {
			return (U) item.elements().next().asText();
		}

		throw new InvalidClassException("The following class is neither String or Long Class: " + outputClazz.toString());
	}

	private List<Result<Void>> buildResultList(Iterable<CosmosBulkOperationResponse<Object>> bulkDeleteOperationResponse) {
		List<Result<Void>> results = List.of();

		for (CosmosBulkOperationResponse<Object> response: bulkDeleteOperationResponse) {
			if (response.getResponse().isSuccessStatusCode()) {
				results.add(Result.ok());
			} else {
				results.add(Result.error(ErrorCode.INTERNAL_ERROR));
			}
		}

		return results;
	}

	private <T> Result<T> translateCosmosResponse(CosmosItemResponse<T> response) {
		if (response.getStatusCode() < 300) {
			return Result.ok(response.getItem());
		}

		return Result.error(errorCodeFromStatus(response.getStatusCode()));
	}

	private <T> Result<T> translateCosmosResponse(CosmosItemResponse<Object> response, T obj) {
		if (response.getStatusCode() < 300) {
			return Result.ok(obj);
		}

		return Result.error(errorCodeFromStatus(response.getStatusCode()));
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
