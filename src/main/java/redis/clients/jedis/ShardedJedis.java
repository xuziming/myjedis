package redis.clients.jedis;

import java.io.Closeable;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.util.Hashing;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import redis.clients.util.Pool;

public class ShardedJedis extends BinaryShardedJedis implements JedisCommands, Closeable {

	protected Pool<ShardedJedis> dataSource = null;

	public ShardedJedis(List<JedisShardInfo> shards) {
		super(shards);
	}

	public ShardedJedis(List<JedisShardInfo> shards, Hashing algo) {
		super(shards, algo);
	}

	public ShardedJedis(List<JedisShardInfo> shards, Pattern keyTagPattern) {
		super(shards, keyTagPattern);
	}

	public ShardedJedis(List<JedisShardInfo> shards, Hashing algo, Pattern keyTagPattern) {
		super(shards, algo, keyTagPattern);
	}

	public String set(String key, String value) {
		Jedis jedis = getShard(key);
		return jedis.set(key, value);
	}

	@Override
	public String set(String key, String value, String nxxx, String expx, long time) {
		Jedis jedis = getShard(key);
		return jedis.set(key, value, nxxx, expx, time);
	}

	public String get(String key) {
		Jedis jedis = getShard(key);
		return jedis.get(key);
	}

	public String echo(String string) {
		Jedis jedis = getShard(string);
		return jedis.echo(string);
	}

	public Boolean exists(String key) {
		Jedis jedis = getShard(key);
		return jedis.exists(key);
	}

	public String type(String key) {
		Jedis jedis = getShard(key);
		return jedis.type(key);
	}

	public Long expire(String key, int seconds) {
		Jedis jedis = getShard(key);
		return jedis.expire(key, seconds);
	}

	public Long pexpire(final String key, final long milliseconds) {
		Jedis jedis = getShard(key);
		return jedis.pexpire(key, milliseconds);
	}

	public Long expireAt(String key, long unixTime) {
		Jedis jedis = getShard(key);
		return jedis.expireAt(key, unixTime);
	}

	public Long pexpireAt(String key, long millisecondsTimestamp) {
		Jedis jedis = getShard(key);
		return jedis.pexpireAt(key, millisecondsTimestamp);
	}

	public Long ttl(String key) {
		Jedis jedis = getShard(key);
		return jedis.ttl(key);
	}

	public Boolean setbit(String key, long offset, boolean value) {
		Jedis jedis = getShard(key);
		return jedis.setbit(key, offset, value);
	}

	public Boolean setbit(String key, long offset, String value) {
		Jedis jedis = getShard(key);
		return jedis.setbit(key, offset, value);
	}

	public Boolean getbit(String key, long offset) {
		Jedis jedis = getShard(key);
		return jedis.getbit(key, offset);
	}

	public Long setrange(String key, long offset, String value) {
		Jedis jedis = getShard(key);
		return jedis.setrange(key, offset, value);
	}

	public String getrange(String key, long startOffset, long endOffset) {
		Jedis jedis = getShard(key);
		return jedis.getrange(key, startOffset, endOffset);
	}

	public String getSet(String key, String value) {
		Jedis jedis = getShard(key);
		return jedis.getSet(key, value);
	}

	public Long setnx(String key, String value) {
		Jedis jedis = getShard(key);
		return jedis.setnx(key, value);
	}

	public String setex(String key, int seconds, String value) {
		Jedis jedis = getShard(key);
		return jedis.setex(key, seconds, value);
	}

	@SuppressWarnings("deprecation")
	public List<String> blpop(String arg) {
		Jedis jedis = getShard(arg);
		return jedis.blpop(arg);
	}

	public List<String> blpop(int timeout, String key) {
		Jedis jedis = getShard(key);
		return jedis.blpop(timeout, key);
	}

	@SuppressWarnings("deprecation")
	public List<String> brpop(String arg) {
		Jedis jedis = getShard(arg);
		return jedis.brpop(arg);
	}

	public List<String> brpop(int timeout, String key) {
		Jedis jedis = getShard(key);
		return jedis.brpop(timeout, key);
	}

	public Long decrBy(String key, long integer) {
		Jedis jedis = getShard(key);
		return jedis.decrBy(key, integer);
	}

	public Long decr(String key) {
		Jedis jedis = getShard(key);
		return jedis.decr(key);
	}

	public Long incrBy(String key, long integer) {
		Jedis jedis = getShard(key);
		return jedis.incrBy(key, integer);
	}

	public Double incrByFloat(String key, double integer) {
		Jedis jedis = getShard(key);
		return jedis.incrByFloat(key, integer);
	}

	public Long incr(String key) {
		Jedis jedis = getShard(key);
		return jedis.incr(key);
	}

	public Long append(String key, String value) {
		Jedis jedis = getShard(key);
		return jedis.append(key, value);
	}

	public String substr(String key, int start, int end) {
		Jedis jedis = getShard(key);
		return jedis.substr(key, start, end);
	}

	public Long hset(String key, String field, String value) {
		Jedis jedis = getShard(key);
		return jedis.hset(key, field, value);
	}

	public String hget(String key, String field) {
		Jedis jedis = getShard(key);
		return jedis.hget(key, field);
	}

	public Long hsetnx(String key, String field, String value) {
		Jedis jedis = getShard(key);
		return jedis.hsetnx(key, field, value);
	}

	public String hmset(String key, Map<String, String> hash) {
		Jedis jedis = getShard(key);
		return jedis.hmset(key, hash);
	}

	public List<String> hmget(String key, String... fields) {
		Jedis jedis = getShard(key);
		return jedis.hmget(key, fields);
	}

	public Long hincrBy(String key, String field, long value) {
		Jedis jedis = getShard(key);
		return jedis.hincrBy(key, field, value);
	}

	public Double hincrByFloat(String key, String field, double value) {
		Jedis jedis = getShard(key);
		return jedis.hincrByFloat(key, field, value);
	}

	public Boolean hexists(String key, String field) {
		Jedis jedis = getShard(key);
		return jedis.hexists(key, field);
	}

	public Long del(String key) {
		Jedis jedis = getShard(key);
		return jedis.del(key);
	}

	public Long hdel(String key, String... fields) {
		Jedis jedis = getShard(key);
		return jedis.hdel(key, fields);
	}

	public Long hlen(String key) {
		Jedis jedis = getShard(key);
		return jedis.hlen(key);
	}

	public Set<String> hkeys(String key) {
		Jedis jedis = getShard(key);
		return jedis.hkeys(key);
	}

	public List<String> hvals(String key) {
		Jedis jedis = getShard(key);
		return jedis.hvals(key);
	}

	public Map<String, String> hgetAll(String key) {
		Jedis jedis = getShard(key);
		return jedis.hgetAll(key);
	}

	public Long rpush(String key, String... strings) {
		Jedis jedis = getShard(key);
		return jedis.rpush(key, strings);
	}

	public Long lpush(String key, String... strings) {
		Jedis jedis = getShard(key);
		return jedis.lpush(key, strings);
	}

	public Long lpushx(String key, String... string) {
		Jedis jedis = getShard(key);
		return jedis.lpushx(key, string);
	}

	public Long strlen(final String key) {
		Jedis jedis = getShard(key);
		return jedis.strlen(key);
	}

	public Long move(String key, int dbIndex) {
		Jedis jedis = getShard(key);
		return jedis.move(key, dbIndex);
	}

	public Long rpushx(String key, String... string) {
		Jedis jedis = getShard(key);
		return jedis.rpushx(key, string);
	}

	public Long persist(final String key) {
		Jedis jedis = getShard(key);
		return jedis.persist(key);
	}

	public Long llen(String key) {
		Jedis jedis = getShard(key);
		return jedis.llen(key);
	}

	public List<String> lrange(String key, long start, long end) {
		Jedis jedis = getShard(key);
		return jedis.lrange(key, start, end);
	}

	public String ltrim(String key, long start, long end) {
		Jedis jedis = getShard(key);
		return jedis.ltrim(key, start, end);
	}

	public String lindex(String key, long index) {
		Jedis jedis = getShard(key);
		return jedis.lindex(key, index);
	}

	public String lset(String key, long index, String value) {
		Jedis jedis = getShard(key);
		return jedis.lset(key, index, value);
	}

	public Long lrem(String key, long count, String value) {
		Jedis jedis = getShard(key);
		return jedis.lrem(key, count, value);
	}

	public String lpop(String key) {
		Jedis jedis = getShard(key);
		return jedis.lpop(key);
	}

	public String rpop(String key) {
		Jedis jedis = getShard(key);
		return jedis.rpop(key);
	}

	public Long sadd(String key, String... members) {
		Jedis jedis = getShard(key);
		return jedis.sadd(key, members);
	}

	public Set<String> smembers(String key) {
		Jedis jedis = getShard(key);
		return jedis.smembers(key);
	}

	public Long srem(String key, String... members) {
		Jedis jedis = getShard(key);
		return jedis.srem(key, members);
	}

	public String spop(String key) {
		Jedis jedis = getShard(key);
		return jedis.spop(key);
	}

	public Set<String> spop(String key, long count) {
		Jedis jedis = getShard(key);
		return jedis.spop(key, count);
	}

	public Long scard(String key) {
		Jedis jedis = getShard(key);
		return jedis.scard(key);
	}

	public Boolean sismember(String key, String member) {
		Jedis jedis = getShard(key);
		return jedis.sismember(key, member);
	}

	public String srandmember(String key) {
		Jedis jedis = getShard(key);
		return jedis.srandmember(key);
	}

	@Override
	public List<String> srandmember(String key, int count) {
		Jedis jedis = getShard(key);
		return jedis.srandmember(key, count);
	}

	public Long zadd(String key, double score, String member) {
		Jedis jedis = getShard(key);
		return jedis.zadd(key, score, member);
	}

	public Long zadd(String key, Map<String, Double> scoreMembers) {
		Jedis jedis = getShard(key);
		return jedis.zadd(key, scoreMembers);
	}

	public Set<String> zrange(String key, long start, long end) {
		Jedis jedis = getShard(key);
		return jedis.zrange(key, start, end);
	}

	public Long zrem(String key, String... members) {
		Jedis jedis = getShard(key);
		return jedis.zrem(key, members);
	}

	public Double zincrby(String key, double score, String member) {
		Jedis jedis = getShard(key);
		return jedis.zincrby(key, score, member);
	}

	public Long zrank(String key, String member) {
		Jedis jedis = getShard(key);
		return jedis.zrank(key, member);
	}

	public Long zrevrank(String key, String member) {
		Jedis jedis = getShard(key);
		return jedis.zrevrank(key, member);
	}

	public Set<String> zrevrange(String key, long start, long end) {
		Jedis jedis = getShard(key);
		return jedis.zrevrange(key, start, end);
	}

	public Set<Tuple> zrangeWithScores(String key, long start, long end) {
		Jedis jedis = getShard(key);
		return jedis.zrangeWithScores(key, start, end);
	}

	public Set<Tuple> zrevrangeWithScores(String key, long start, long end) {
		Jedis jedis = getShard(key);
		return jedis.zrevrangeWithScores(key, start, end);
	}

	public Long zcard(String key) {
		Jedis jedis = getShard(key);
		return jedis.zcard(key);
	}

	public Double zscore(String key, String member) {
		Jedis jedis = getShard(key);
		return jedis.zscore(key, member);
	}

	public List<String> sort(String key) {
		Jedis jedis = getShard(key);
		return jedis.sort(key);
	}

	public List<String> sort(String key, SortingParams sortingParameters) {
		Jedis jedis = getShard(key);
		return jedis.sort(key, sortingParameters);
	}

	public Long zcount(String key, double min, double max) {
		Jedis jedis = getShard(key);
		return jedis.zcount(key, min, max);
	}

	public Long zcount(String key, String min, String max) {
		Jedis jedis = getShard(key);
		return jedis.zcount(key, min, max);
	}

	public Set<String> zrangeByScore(String key, double min, double max) {
		Jedis jedis = getShard(key);
		return jedis.zrangeByScore(key, min, max);
	}

	public Set<String> zrevrangeByScore(String key, double max, double min) {
		Jedis jedis = getShard(key);
		return jedis.zrevrangeByScore(key, max, min);
	}

	public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
		Jedis jedis = getShard(key);
		return jedis.zrangeByScore(key, min, max, offset, count);
	}

	public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
		Jedis jedis = getShard(key);
		return jedis.zrevrangeByScore(key, max, min, offset, count);
	}

	public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
		Jedis jedis = getShard(key);
		return jedis.zrangeByScoreWithScores(key, min, max);
	}

	public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
		Jedis jedis = getShard(key);
		return jedis.zrevrangeByScoreWithScores(key, max, min);
	}

	public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
		Jedis jedis = getShard(key);
		return jedis.zrangeByScoreWithScores(key, min, max, offset, count);
	}

	public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
		Jedis jedis = getShard(key);
		return jedis.zrevrangeByScoreWithScores(key, max, min, offset, count);
	}

	public Set<String> zrangeByScore(String key, String min, String max) {
		Jedis jedis = getShard(key);
		return jedis.zrangeByScore(key, min, max);
	}

	public Set<String> zrevrangeByScore(String key, String max, String min) {
		Jedis jedis = getShard(key);
		return jedis.zrevrangeByScore(key, max, min);
	}

	public Set<String> zrangeByScore(String key, String min, String max, int offset, int count) {
		Jedis jedis = getShard(key);
		return jedis.zrangeByScore(key, min, max, offset, count);
	}

	public Set<String> zrevrangeByScore(String key, String max, String min, int offset, int count) {
		Jedis jedis = getShard(key);
		return jedis.zrevrangeByScore(key, max, min, offset, count);
	}

	public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max) {
		Jedis jedis = getShard(key);
		return jedis.zrangeByScoreWithScores(key, min, max);
	}

	public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min) {
		Jedis jedis = getShard(key);
		return jedis.zrevrangeByScoreWithScores(key, max, min);
	}

	public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max, int offset, int count) {
		Jedis jedis = getShard(key);
		return jedis.zrangeByScoreWithScores(key, min, max, offset, count);
	}

	public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min, int offset, int count) {
		Jedis jedis = getShard(key);
		return jedis.zrevrangeByScoreWithScores(key, max, min, offset, count);
	}

	public Long zremrangeByRank(String key, long start, long end) {
		Jedis jedis = getShard(key);
		return jedis.zremrangeByRank(key, start, end);
	}

	public Long zremrangeByScore(String key, double start, double end) {
		Jedis jedis = getShard(key);
		return jedis.zremrangeByScore(key, start, end);
	}

	public Long zremrangeByScore(String key, String start, String end) {
		Jedis jedis = getShard(key);
		return jedis.zremrangeByScore(key, start, end);
	}

	@Override
	public Long zlexcount(final String key, final String min, final String max) {
		return getShard(key).zlexcount(key, min, max);
	}

	@Override
	public Set<String> zrangeByLex(final String key, final String min, final String max) {
		return getShard(key).zrangeByLex(key, min, max);
	}

	@Override
	public Set<String> zrangeByLex(final String key, final String min, final String max, final int offset,
			final int count) {
		return getShard(key).zrangeByLex(key, min, max, offset, count);
	}

	@Override
	public Set<String> zrevrangeByLex(String key, String max, String min) {
		return getShard(key).zrevrangeByLex(key, max, min);
	}

	@Override
	public Set<String> zrevrangeByLex(String key, String max, String min, int offset, int count) {
		return getShard(key).zrevrangeByLex(key, max, min, offset, count);
	}

	@Override
	public Long zremrangeByLex(final String key, final String min, final String max) {
		return getShard(key).zremrangeByLex(key, min, max);
	}

	public Long linsert(String key, LIST_POSITION where, String pivot, String value) {
		Jedis jedis = getShard(key);
		return jedis.linsert(key, where, pivot, value);
	}

	public Long bitcount(final String key) {
		Jedis jedis = getShard(key);
		return jedis.bitcount(key);
	}

	public Long bitcount(final String key, long start, long end) {
		Jedis jedis = getShard(key);
		return jedis.bitcount(key, start, end);
	}

	@Deprecated
	/**
	 * This method is deprecated due to bug (scan cursor should be unsigned long)
	 * And will be removed on next major release
	 * 
	 * @see https://github.com/xetorthio/jedis/issues/531
	 */
	public ScanResult<Entry<String, String>> hscan(String key, int cursor) {
		Jedis jedis = getShard(key);
		return jedis.hscan(key, cursor);
	}

	@Deprecated
	/**
	 * This method is deprecated due to bug (scan cursor should be unsigned long)
	 * And will be removed on next major release
	 * 
	 * @see https://github.com/xetorthio/jedis/issues/531
	 */
	public ScanResult<String> sscan(String key, int cursor) {
		Jedis jedis = getShard(key);
		return jedis.sscan(key, cursor);
	}

	@Deprecated
	/**
	 * This method is deprecated due to bug (scan cursor should be unsigned long)
	 * And will be removed on next major release
	 * 
	 * @see https://github.com/xetorthio/jedis/issues/531
	 */
	public ScanResult<Tuple> zscan(String key, int cursor) {
		Jedis jedis = getShard(key);
		return jedis.zscan(key, cursor);
	}

	public ScanResult<Entry<String, String>> hscan(String key, final String cursor) {
		Jedis jedis = getShard(key);
		return jedis.hscan(key, cursor);
	}

	public ScanResult<String> sscan(String key, final String cursor) {
		Jedis jedis = getShard(key);
		return jedis.sscan(key, cursor);
	}

	public ScanResult<Tuple> zscan(String key, final String cursor) {
		Jedis jedis = getShard(key);
		return jedis.zscan(key, cursor);
	}

	@Override
	public void close() {
		if (dataSource != null) {
			boolean broken = false;

			for (Jedis jedisedis : getAllShards()) {
				if (jedisedis.getClient().isBroken()) {
					broken = true;
					break;
				}
			}

			if (broken) {
				dataSource.returnBrokenResource(this);
			} else {
				dataSource.returnResource(this);
			}

		} else {
			disconnect();
		}
	}

	public void setDataSource(Pool<ShardedJedis> shardedJedisPool) {
		this.dataSource = shardedJedisPool;
	}

	public void resetState() {
		for (Jedis jedisedis : getAllShards()) {
			jedisedis.resetState();
		}
	}

	public Long pfadd(String key, String... elements) {
		Jedis jedis = getShard(key);
		return jedis.pfadd(key, elements);
	}

	@Override
	public long pfcount(String key) {
		Jedis jedis = getShard(key);
		return jedis.pfcount(key);
	}

}