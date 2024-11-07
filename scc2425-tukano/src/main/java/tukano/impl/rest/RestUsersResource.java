package tukano.impl.rest;

import jakarta.inject.Singleton;
import java.util.List;
import tukano.api.User;
import tukano.api.Users;
import tukano.api.rest.RestUsers;
import tukano.impl.JavaNoSQLUsers;
import tukano.impl.JavaHibernateUsers;
import utils.Props;

@Singleton
public class RestUsersResource extends RestResource implements RestUsers {
	
	static final Users impl = Boolean.parseBoolean(Props.get("USE_SQL", "false")) ?
			JavaHibernateUsers.getInstance() : JavaNoSQLUsers.getInstance();

	@Override
	public String createUser(User user) {
		return super.resultOrThrow( impl.createUser( user));
	}

	@Override
	public User getUser(String name, String pwd) {
		return super.resultOrThrow( impl.getUser(name, pwd));
	}
	
	@Override
	public User updateUser(String name, String pwd, User user) {
		return super.resultOrThrow( impl.updateUser(name, pwd, user));
	}

	@Override
	public User deleteUser(String name, String pwd) {
		return super.resultOrThrow( impl.deleteUser(name, pwd));
	}

	@Override
	public List<User> searchUsers(String pattern) {
		return super.resultOrThrow( impl.searchUsers( pattern));
	}
}
