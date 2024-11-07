package utils;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.hibernate.Session;

import tukano.api.Result;

public interface DB {

	<T> List<T> sql(String query, Class<T> containerClazz);

	/**
	 * Queries the Container relative to the containerClazz.
	 * Returns the value a expectedClazz.
	 * Used for queries with aggregate function for example
	 *
	 * @param query - string with SQL code to be sent to the CosmosDb
	 * @param containerClazz - class that represents the container
	 * @param expectedClazz - class of the desired output (either String or Long)
	 * @return List of expectedClazz
	 */
	<T, U> List<U> sql(String query, Class<T> containerClazz, Class<U> expectedClazz);

	<T> List<T> sql(Class<T> clazz, String fmt, Object ... args);
	
	<T> Result<T> getOne(String id, String partition, Class<T> clazz);
	
	<T> Result<T> deleteOne(T obj);

	<T> List<Result<Void>> deleteCollection(List<T> targets);

	<T> Result<T> updateOne(T obj);
	
	<T> Result<T> insertOne( T obj);
}
