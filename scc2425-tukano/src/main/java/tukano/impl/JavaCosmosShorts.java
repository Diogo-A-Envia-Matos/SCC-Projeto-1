package tukano.impl;

import static java.lang.String.*;
import static tukano.api.Result.ErrorCode.*;
import static tukano.api.Result.*;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
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
import utils.GetId;

//TODO: Como fazer funcoes que usam queries
public class JavaCosmosShorts implements Shorts {

	private static Logger Log = Logger.getLogger(JavaCosmosShorts.class.getName());
	
	private static Shorts instance;
	
	private static DB database; // Choose between CosmosDB or Hibernate
	
	synchronized public static Shorts getInstance() {
		if( instance == null )
			instance = new JavaCosmosShorts();
		return instance;
	}
	
	private JavaCosmosShorts() {
		database = DBCosmos.getInstance();
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
		return errorOrValue( database.getOne(shortId, shortId, Short.class), shrt -> shrt.copyWithLikes_And_Token( likes.get(0)));
	}

	
	@Override
	public Result<Void> deleteShort(String shortId, String password) {
		Log.info(() -> format("deleteShort : shortId = %s, pwd = %s\n", shortId, password));


		return errorOrResult( getShort(shortId), shrt ->
				errorOrResult( okUser( shrt.getOwnerId(), password), user -> {
					var resShort = database.deleteOne(shrt);
					Log.info(() -> format(resShort.isOK() ? "deleteShort : deleted blob \n" : "deleteShort : couldn't delete blob\n"));

					var query = format("SELECT * FROM Likes WHERE Likes.shortId = '%s'", shortId);
					var likesToDelete = database.sql(query, Likes.class);

					var resLikes = database.deleteCollection(likesToDelete);
					Log.info(() -> format("deleteShort : deleted %s likes \n", countDeletedItems(resLikes)));

					String[] url = shrt.getBlobUrl().split("\\?token=");
					String blobUrl = url[0];
					String token = url[1];

					var resBlob = JavaBlobs.getInstance().delete(blobUrl, token);
					Log.info(() -> format(resBlob.isOK() ? "deleteShort : deleted blob \n" : "deleteShort : couldn't delete blob\n"));
					return Result.ok();
			})
		);
	}

	@Override
	public Result<List<String>> getShorts(String userId) {
		Log.info(() -> format("getShorts : userId = %s\n", userId));

		var query = format("SELECT s.id FROM Short s WHERE s.ownerId = '%s'", userId);
		return errorOrValue( okUser(userId), database.sql(query, Short.class, String.class));
	}

	@Override
	public Result<Void> follow(String userId1, String userId2, boolean isFollowing, String password) {
		Log.info(() -> format("follow : userId1 = %s, userId2 = %s, isFollowing = %s, pwd = %s\n", userId1, userId2, isFollowing, password));

		var res = database.getOne(Following.buildId(userId1, userId2), userId1, Following.class);
		if (res.isOK() && isFollowing) {
			return Result.error(CONFLICT);
		}
		
		return errorOrResult( okUser(userId1, password), user -> {
			var f = new Following(userId1, userId2);
			return errorOrVoid( okUser( userId2), isFollowing ? database.insertOne( f ) : database.deleteOne( f ));	
		});			
	}

	@Override
	public Result<List<String>> followers(String userId, String password) {
		Log.info(() -> format("followers : userId = %s, pwd = %s\n", userId, password));

		var query = format("SELECT f.follower FROM Following f WHERE f.followee = '%s'", userId);		
		return errorOrValue( okUser(userId, password), database.sql(query, Following.class, String.class));
	}

	@Override
	public Result<Void> like(String shortId, String userId, boolean isLiked, String password) {
		Log.info(() -> format("like : shortId = %s, userId = %s, isLiked = %s, pwd = %s\n", shortId, userId, isLiked, password));

		var res = database.getOne(Likes.buildId(userId, shortId), userId, Likes.class);
		if (res.isOK() && isLiked) {
			return Result.error(CONFLICT);
		}

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
			
			return errorOrValue( okUser( shrt.getOwnerId(), password ), database.sql(query, Likes.class, String.class));
		});
	}

	@Override
	public Result<List<String>> getFeed(String userId, String password) {
		Log.info(() -> format("getFeed : userId = %s, pwd = %s\n", userId, password));

		return errorOrValue( okUser(userId, password), user -> {
			final String queryFollowee = String.format("SELECT f.followee FROM Followings f WHERE f.follower = '%s'", userId);
			// System.out.println("-------------------------------queryFollowee: "+ queryFollowee);
			final List<String> followees = database.sql(queryFollowee, Following.class, String.class);

			final String queryShorts = String.format("SELECT * FROM Shorts s WHERE s.ownerId IN ('%s', %s)",userId, formatStringCollection(followees));
			// System.out.println("-------------------------------queryShorts: "+ queryShorts);
			return database.sql(queryShorts, Short.class, String.class);
		});

	}
	private String formatStringCollection(List<String> followees) {
		final Iterator<String> iterator = followees.iterator();

		if (! iterator.hasNext()) {
			return "";
		}
		String res = String.format("'%s'", iterator.next());

		while (iterator.hasNext()){
			String s = String.format("', %s'", iterator.next());
			res = res.concat(s);
		}
		return res;
	}

	@Override
	public Result<Void> deleteAllShorts(String userId, String password, String token) {
		Log.info(() -> format("deleteAllShorts : userId = %s, password = %s, token = %s\n", userId, password, token));

		//TODO fix the token
		// if( ! Token.isValid( token, userId ) )
		// 	return error(FORBIDDEN);

		var query1 = format("SELECT * FROM Short s WHERE s.ownerId = '%s'", userId);
		List<Short> shortsToRemove = database.sql(query1, Short.class);
		var resShorts = database.deleteCollection(shortsToRemove);
		Log.info(() -> format("deleteAllShort : deleted %s shorts \n", countDeletedItems(resShorts)));

		//delete follows
		var query2 = format("SELECT * FROM Following f WHERE f.follower = '%s' OR f.followee = '%s'", userId, userId);	
		List<Following> followingsToRemove = database.sql(query2, Following.class);
		var resFollowings = database.deleteCollection(shortsToRemove);
		Log.info(() -> format("deleteAllShort : deleted %s followings \n", countDeletedItems(resFollowings)));

		//delete likes
		var query3 = format("SELECT * FROM Likes l WHERE l.ownerId = '%s' OR l.userId = '%s'", userId, userId);
		List<Likes> likesToRemove = database.sql(query3, Likes.class);
		var resLikes = database.deleteCollection(shortsToRemove);
		Log.info(() -> format("deleteAllShort : deleted %s likes \n", countDeletedItems(resLikes)));

		return Result.ok();

		// var shortsBatch = CosmosBatch.createCosmosBatch(new PartitionKey("shortId"));
		// var followingsBatch = CosmosBatch.createCosmosBatch(new PartitionKey("followee"));
		// var likesBatch = CosmosBatch.createCosmosBatch(new PartitionKey("userId"));
		//
		// Map<Operations, List<Object>> operations = new HashMap<>();
		// operations.put(Operations.DELETE, new LinkedList<>());
		//
		// for (Short s: shortsToRemove) {
		// 	operations.get(Operations.DELETE).add(s);
		// 	shortsBatch.deleteItemOperation(GetId.getId(s));
		// }
		// for (Following f: followingsToRemove) {
		// 	operations.get(Operations.DELETE).add(f);
		// 	followingsBatch.deleteItemOperation(GetId.getId(f));
		// }
		// for (Likes l: likesToRemove) {
		// 	operations.get(Operations.DELETE).add(l);
		// 	likesBatch.deleteItemOperation(GetId.getId(l));
		// }
		//
		// var res1 = ((DBCosmos) database).transaction(shortsBatch, Short.class);
		// if (!res1.isOK()) {
		// 	return error(res1.error());
		// }
		// var res2 = ((DBCosmos) database).transaction(followingsBatch, Following.class);
		// if (!res2.isOK()) {
		// 	return error(res2.error());
		// }
		// var res3 = ((DBCosmos) database).transaction(shortsBatch, Likes.class);
		// if (!res3.isOK()) {
		// 	return error(res3.error());
		// }

		// return ((DBCosmos) database).transaction(operations);
		// return ok();

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

	private <T> long countDeletedItems(List<Result<T>> likes) {
		return likes.stream()
				.filter(res -> res.isOK())
				.count();
	}
}