package tukano.impl;

import static java.lang.String.*;
import static tukano.api.Result.ErrorCode.*;
import static tukano.api.Result.*;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import tukano.api.Blobs;
import tukano.api.Result;
import tukano.api.User;
import tukano.api.Users;
import utils.DB;
import utils.DBCosmos;
// import utils.DBHibernate;
import utils.Props;

public class JavaNoSQLUsers implements Users {
	
	private static Logger Log = Logger.getLogger(JavaNoSQLUsers.class.getName());

	private static Users instance;

	private static DB database; // Choose between CosmosDB or Hibernate

	private static Blobs blobDatabase = Boolean.parseBoolean(Props.get("USE_AZURE_BLOB_STORAGE", "true")) ?
		JavaAzureBlobs.getInstance() : JavaFileBlobs.getInstance();
	
	synchronized public static Users getInstance() {
		if( instance == null )
			instance = new JavaNoSQLUsers();
		return instance;
	}
	
	private JavaNoSQLUsers() {
		database = DBCosmos.getInstance();
	}
	
	@Override
	public Result<String> createUser(User user) {
		Log.info(() -> format("createUser : %s\n", user));

		if( badUserInfo( user ) )
				return error(BAD_REQUEST);

		return errorOrValue( database.insertOne( user), user.getId() );
	}

	@Override
	public Result<User> getUser(String userId, String pwd) {
		Log.info( () -> format("getUser : userId = %s, pwd = %s\n", userId, pwd));

		if (userId == null)
			return error(BAD_REQUEST);
		
		return validatedUserOrError( database.getOne( userId, userId, User.class), pwd);
	}

	@Override
	public Result<User> updateUser(String userId, String pwd, User other) {
		Log.info(() -> format("updateUser : userId = %s, pwd = %s, user: %s\n", userId, pwd, other));

		if (badUpdateUserInfo(userId, pwd, other))
			return error(BAD_REQUEST);

		return errorOrResult( validatedUserOrError(database.getOne( userId, userId, User.class), pwd), user -> database.updateOne( user.updateFrom(other)));
	}

	@Override
	public Result<User> deleteUser(String userId, String pwd) {
		Log.info(() -> format("deleteUser : userId = %s, pwd = %s\n", userId, pwd));

		if (userId == null || pwd == null )
			return error(BAD_REQUEST);

		return errorOrResult( validatedUserOrError(database.getOne( userId, userId, User.class), pwd), user -> {

			// Delete user shorts and related info asynchronously in a separate thread
			Executors.defaultThreadFactory().newThread( () -> {
				JavaNoSQLShorts.getInstance().deleteAllShorts(userId, pwd, Token.get(userId));
				blobDatabase.deleteAllBlobs(userId, Token.get(userId));
			}).start();
			
			return database.deleteOne( user);
		});
	}

	@Override
	public Result<List<User>> searchUsers(String p) {
		String pattern = Objects.toString(p, "");
		Log.info(() -> format("searchUsers : patterns = '%s'\n", pattern));

		String query = format("SELECT * FROM User u WHERE UPPER(u.id) LIKE '%%%s%%'", pattern.toUpperCase());
		var hits = database.sql(query, User.class)
				.stream()
				.map(User::copyWithoutPassword)
				.toList();

		Log.info(() -> format("searchUsers : nHits = %s\n", hits.size()));

		return ok(hits);
	}

	
	private Result<User> validatedUserOrError( Result<User> res, String pwd ) {
		if( res.isOK())
			return res.value().getPwd().equals( pwd ) ? res : error(FORBIDDEN);
		else
			return res;
	}
	
	private boolean badUserInfo( User user) {
		return (user.userId() == null || user.pwd() == null || user.displayName() == null || user.email() == null);
	}
	
	private boolean badUpdateUserInfo( String userId, String pwd, User info) {
		return (userId == null || pwd == null || info.getId() != null && ! userId.equals( info.getId()));
	}
}
