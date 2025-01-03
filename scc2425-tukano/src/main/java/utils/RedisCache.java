package utils;

import com.fasterxml.jackson.databind.ObjectMapper;

import exceptions.InvalidClassException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import tukano.api.Blobs;
import tukano.api.Short;
import tukano.api.User;
import tukano.impl.data.Following;
import tukano.impl.data.Likes;
import utils.Props;

//TODO: Finish this file
//TODO: Implement interface
public class RedisCache {
    
    // Have to make the calls to DBCosmos search this cache first before checking the database

    // TODO: Choose wether to use write-through or write-back

    private static final String RedisHostname = Props.get("REDIS_URL", "");
	private static final String RedisKey = Props.get("REDIS_KEY", "");
	private static final int REDIS_PORT = 6380;
	private static final int REDIS_TIMEOUT = 1000;
	private static final boolean Redis_USE_TLS = true;
	
	private static JedisPool instance;
	
	public synchronized static JedisPool getCachePool() {
		if( instance != null)
			return instance;
		
		var poolConfig = new JedisPoolConfig();
		poolConfig.setMaxTotal(128);
		poolConfig.setMaxIdle(128);
		poolConfig.setMinIdle(16);
		poolConfig.setTestOnBorrow(true);
		poolConfig.setTestOnReturn(true);
		poolConfig.setTestWhileIdle(true);
		poolConfig.setNumTestsPerEvictionRun(3);
		poolConfig.setBlockWhenExhausted(true);
		instance = new JedisPool(poolConfig, RedisHostname, REDIS_PORT, REDIS_TIMEOUT, RedisKey, Redis_USE_TLS);
		return instance;
	}

	// TODO: Decide if id is created inside or outside of RedisCache
	// public <T> void addItem(String id, T item, Class<T> clazz) {
    //     try (Jedis jedis = instance.getResource()) {
	// 		var value = JSON.encode( item );
	// 		var cacheId = getCacheId(id, clazz);
    //         jedis.set(cacheId, value);
    //     } catch (Exception e) {
	// 		e.printStackTrace();
    //     }
    // }
	
	// //TODO: Find better way than else if
	// //TODO: Figure out how to use T directly
	// public <T> T getItem(String id, Class<T> clazz) {
    //     try (Jedis jedis = instance.getResource()) {
	// 		var cacheId = getCacheId(id, clazz);
	// 		var res = JSON.decode( jedis.get(cacheId), clazz);
	// 		return res;
    //     } catch (Exception e) {
	// 		e.printStackTrace();
	// 		throw e;
    //     }
    // }

	// //TODO: Find better way than else if
	// //TODO: Figure out how to use T directly
	// public <T> T deleteItem(String id, T item, Class<T> clazz) {
    //     try (Jedis jedis = instance.getResource()) {
	// 		var cacheId = getCacheId(id, clazz);
	// 		var res = JSON.decode( jedis.get(cacheId), clazz);
	// 		jedis.del(cacheId);
	// 		return res;
    //     } catch (Exception e) {
	// 		e.printStackTrace();
	// 		throw e;
    //     }
    // }

	// private <T> String getCacheId(String id, Class<T> clazz) {
	// 	try {
	// 		if (clazz.equals(Blobs.class)) {
	// 			return "blob:" + id;
	// 		} else if (clazz.equals(User.class)) {
	// 			return "user:" + id;
	// 		} else if (clazz.equals(Short.class)) {
	// 			return "short:" + id;
	// 		} else if (clazz.equals(Following.class)) {
	// 			return "following:" + id;
	// 		} else if (clazz.equals(Likes.class)) {
	// 			return "like:" + id;
	// 		}
	// 		throw new InvalidClassException("Invalid Class: " + clazz.toString());
	// 	} catch (Exception e) {
	// 		e.printStackTrace();
	// 		throw e;
	// 	}
	// }

}
