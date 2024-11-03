package utils;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.hibernate.Session;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;

import tukano.api.Result;

public class DBHibernate implements DB {
	
	private static DBHibernate instance;

	public static synchronized DBHibernate getInstance() {
		if( instance != null)
			return instance;
		instance = new DBHibernate();
		return instance;
		
	}

	public DBHibernate() {}

	public <T> List<T> sql(String query, Class<T> clazz) {
		return Hibernate.getInstance().sql(query, clazz);
	}

	public <T, U> List<U> sql(String query, Class<T> containerClazz, Class<U> expectedClazz) {
		return Hibernate.getInstance().sql(query, expectedClazz);
	}

	public <T> List<T> sql(Class<T> clazz, String fmt, Object ... args) {
		return Hibernate.getInstance().sql(String.format(fmt, args), clazz);
	}
	
	
	public <T> Result<T> getOne(String id, String partition, Class<T> clazz) {
		return Hibernate.getInstance().getOne(id, clazz);
	}
	
	public <T> Result<T> deleteOne(T obj) {
		return Hibernate.getInstance().deleteOne(obj);
	}
	public <T> List<Result<Void>> deleteCollection(List<T> targets) {
		return Hibernate.getInstance().deleteCollection(targets);
	}

	public <T> Result<T> updateOne(T obj) {
		return Hibernate.getInstance().updateOne(obj);
	}
	
	public <T> Result<T> insertOne( T obj) {
		System.err.println("DB.insert:" + obj );
		return Result.errorOrValue(Hibernate.getInstance().persistOne(obj), obj);
	}
	
	public <T> Result<T> transaction( Consumer<Session> c) {
		return Hibernate.getInstance().execute( c::accept );
	}
	
	public <T> Result<T> transaction( Function<Session, Result<T>> func) {
		return Hibernate.getInstance().execute( func );
	}
}
