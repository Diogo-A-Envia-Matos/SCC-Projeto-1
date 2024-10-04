package utils;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.hibernate.Session;

import tukano.api.Result;

public interface DB {

	<T> List<T> sql(String query, Class<T> clazz);
	
	<T> List<T> sql(Class<T> clazz, String fmt, Object ... args);
	
	<T> Result<T> getOne(String id, Class<T> clazz);
	
	<T> Result<T> deleteOne(T obj);
	
	<T> Result<T> updateOne(T obj);
	
	<T> Result<T> insertOne( T obj);
	
	<T> Result<T> transaction( Consumer<Session> c);
	
	<T> Result<T> transaction( Function<Session, Result<T>> func);
}
