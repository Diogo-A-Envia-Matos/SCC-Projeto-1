package tukano.impl;

import static java.lang.String.*;
import static tukano.api.Result.ErrorCode.*;
import static tukano.api.Result.*;

import com.azure.cosmos.models.CosmosBatch;
import com.azure.cosmos.models.PartitionKey;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;
import tukano.api.Blobs;
import tukano.api.Result;
import tukano.api.Short;
import tukano.api.Shorts;
import tukano.api.User;
import tukano.impl.data.Following;
import tukano.impl.data.Likes;
import tukano.impl.rest.TukanoRestServer;
import utils.DB;
import utils.DBCosmos;
import utils.DBHibernate;
import utils.GetId;
import utils.Operations;

//TODO: Como fazer funcoes que usam queries
public class JavaHibernateShorts implements Shorts {

	private static Logger Log = Logger.getLogger(JavaHibernateShorts.class.getName());

	private static Shorts instance;

	private static DB database; // Choose between CosmosDB or Hibernate

	synchronized public static Shorts getInstance() {
		if( instance == null )
			instance = new JavaHibernateShorts();
		return instance;
	}

	//TODO: Criar novo ficheiro que usa DBHibernate
	private JavaHibernateShorts() {
		database = DBHibernate.getInstance();
	}
	
	
	@Override
	public Result<Short> createShort(String userId, String password) {
		Log.info(() -> format("createShort : userId = %s, pwd = %s\n", userId, password));

		return errorOrResult( okUser(userId, password), user -> {
			
			var shortId = format("%s+%s", userId, UUID.randomUUID());
			var blobUrl = format("%s/%s/%s", TukanoRestServer.serverURI, Blobs.NAME, shortId); 
			var shrt = new Short(shortId, userId, blobUrl);

			return errorOrValue(database.insertOne(shrt), s -> s.copyWithLikes_And_Token(0));
		});
	}

	@Override
	public Result<Short> getShort(String shortId) {
		Log.info(() -> format("getShort : shortId = %s\n", shortId));

		if( shortId == null )
			return error(BAD_REQUEST);

		var query = format("SELECT count(l.shortId) FROM Likes l WHERE l.id = '%s'", shortId);
		var likes = database.sql(query, Likes.class, Long.class);
		return errorOrValue( database.getOne(shortId, Short.class), shrt -> shrt.copyWithLikes_And_Token( likes.get(0)));
	}

	
	@Override
	public Result<Void> deleteShort(String shortId, String password) {
		Log.info(() -> format("deleteShort : shortId = %s, pwd = %s\n", shortId, password));
		
		return errorOrResult( getShort(shortId), shrt -> {
			
			return errorOrResult( okUser( shrt.getOwnerId(), password), user -> {
				database.deleteOne(shrt);

				var query = format("SELECT * FROM Likes WHERE Likes.shortId = '%s'", shortId);
				var likesToDelete = database.sql(query, Short.class, Likes.class);

				var likesBatch = CosmosBatch.createCosmosBatch(new PartitionKey("userId"));

				for (Likes like: likesToDelete) {
					likesBatch.deleteItemOperation(GetId.getId(like));
				}

				((DBCosmos) database).transaction(likesBatch, Likes.class);

				JavaBlobs.getInstance().delete(shrt.getBlobUrl(), Token.get());
				return null;
			});	
		});
	}

	@Override
	public Result<List<String>> getShorts(String userId) {
		Log.info(() -> format("getShorts : userId = %s\n", userId));

		var query = format("SELECT s.shortId FROM Short s WHERE s.ownerId = '%s'", userId);
		return errorOrValue( okUser(userId), database.sql( query, String.class));
	}

	@Override
	public Result<Void> follow(String userId1, String userId2, boolean isFollowing, String password) {
		Log.info(() -> format("follow : userId1 = %s, userId2 = %s, isFollowing = %s, pwd = %s\n", userId1, userId2, isFollowing, password));
	
		
		return errorOrResult( okUser(userId1, password), user -> {
			var f = new Following(userId1, userId2);
			return errorOrVoid( okUser( userId2), isFollowing ? database.insertOne( f ) : database.deleteOne( f ));	
		});			
	}

	@Override
	public Result<List<String>> followers(String userId, String password) {
		Log.info(() -> format("followers : userId = %s, pwd = %s\n", userId, password));

		var query = format("SELECT f.follower FROM Following f WHERE f.followee = '%s'", userId);		
		return errorOrValue( okUser(userId, password), database.sql(query, String.class));
	}

	@Override
	public Result<Void> like(String shortId, String userId, boolean isLiked, String password) {
		Log.info(() -> format("like : shortId = %s, userId = %s, isLiked = %s, pwd = %s\n", shortId, userId, isLiked, password));

		
		return errorOrResult( getShort(shortId), shrt -> {
			var l = new Likes(userId, shortId, shrt.getOwnerId());
			return errorOrVoid( okUser( userId, password), isLiked ? database.insertOne( l ) : database.deleteOne( l ));	
		});
	}

	@Override
	public Result<List<String>> likes(String shortId, String password) {
		Log.info(() -> format("likes : shortId = %s, pwd = %s\n", shortId, password));

		return errorOrResult( getShort(shortId), shrt -> {
			
			var query = format("SELECT l.userId FROM Likes l WHERE l.shortId = '%s'", shortId);					
			
			return errorOrValue( okUser( shrt.getOwnerId(), password ), database.sql(query, String.class));
		});
	}

	@Override
	public Result<List<String>> getFeed(String userId, String password) {
		Log.info(() -> format("getFeed : userId = %s, pwd = %s\n", userId, password));

		final var QUERY_FMT = """
				SELECT s.shortId, s.timestamp FROM Short s WHERE	s.ownerId = '%s'				
				UNION			
				SELECT s.shortId, s.timestamp FROM Short s, Following f 
					WHERE 
						f.followee = s.ownerId AND f.follower = '%s' 
				ORDER BY s.timestamp DESC""";

		return errorOrValue( okUser( userId, password), database.sql( format(QUERY_FMT, userId, userId), String.class));		
	}
		
	protected Result<User> okUser( String userId, String pwd) {
		return JavaUsers.getInstance().getUser(userId, pwd);
	}
	
	private Result<Void> okUser( String userId ) {
		var res = okUser( userId, "");
		if( res.error() == FORBIDDEN )
			return ok();
		else
			return error( res.error() );
	}
	
	//TODO: Implement way to change the function used based on database type
	@Override
	public Result<Void> deleteAllShorts(String userId, String password, String token) {
		Log.info(() -> format("deleteAllShorts : userId = %s, password = %s, token = %s\n", userId, password, token));

		if( ! Token.isValid( token, userId ) )
			return error(FORBIDDEN);

		// Map<String, Map<String, String>> toRemove = new HashMap<>();

		//Ideia, colocar likes e follows no users
		//TODO: Tentar usar Bulk Operations

		// 3 pesquisas -> 1 batch delete
		var query1 = format("SELECT * FROM Short s WHERE s.ownerId = '%s'", userId);
		List<Short> shortsToRemove = database.sql(query1, Short.class);
		
		//delete follows
		var query2 = format("SELECT * FROM Following f WHERE f.follower = '%s' OR f.followee = '%s'", userId, userId);	
		List<Following> followingsToRemove = database.sql(query2, Following.class);
		
		//delete likes
		var query3 = format("SELECT * FROM Likes l WHERE l.ownerId = '%s' OR l.userId = '%s'", userId, userId);
		List<Likes> likesToRemove = database.sql(query3, Likes.class);

		var shortsBatch = CosmosBatch.createCosmosBatch(new PartitionKey("shortId"));
		var followingsBatch = CosmosBatch.createCosmosBatch(new PartitionKey("followee"));
		var likesBatch = CosmosBatch.createCosmosBatch(new PartitionKey("userId"));

		Map<Operations, List<Object>> operations = new HashMap<>();
		operations.put(Operations.DELETE, new LinkedList<>());

		for (Short s: shortsToRemove) {
			operations.get(Operations.DELETE).add(s);
			shortsBatch.deleteItemOperation(GetId.getId(s));
		}
		for (Following f: followingsToRemove) {
			operations.get(Operations.DELETE).add(f);
			followingsBatch.deleteItemOperation(GetId.getId(f));
		}
		for (Likes l: likesToRemove) {
			operations.get(Operations.DELETE).add(l);
			likesBatch.deleteItemOperation(GetId.getId(l));
		}

		var res1 = ((DBCosmos) database).transaction(shortsBatch, Short.class);
		if (!res1.isOK()) {
			return error(res1.error());
		}
		var res2 = ((DBCosmos) database).transaction(followingsBatch, Following.class);
		if (!res2.isOK()) {
			return error(res2.error());
		}
		var res3 = ((DBCosmos) database).transaction(shortsBatch, Likes.class);
		if (!res3.isOK()) {
			return error(res3.error());
		}

		// return ((DBCosmos) database).transaction(operations);
		return ok();

		// return DB.transaction( (hibernate) -> {
						
		// 	//delete shorts
		// 	var query1 = format("DELETE Short s WHERE s.ownerId = '%s'", userId);		
		// 	hibernate.createQuery(query1, Short.class).executeUpdate();
			
		// 	//delete follows
		// 	var query2 = format("DELETE Following f WHERE f.follower = '%s' OR f.followee = '%s'", userId, userId);		
		// 	hibernate.createQuery(query2, Following.class).executeUpdate();
			
		// 	//delete likes
		// 	var query3 = format("DELETE Likes l WHERE l.ownerId = '%s' OR l.userId = '%s'", userId, userId);		
		// 	hibernate.createQuery(query3, Likes.class).executeUpdate();
			
		// });
	}
	
}