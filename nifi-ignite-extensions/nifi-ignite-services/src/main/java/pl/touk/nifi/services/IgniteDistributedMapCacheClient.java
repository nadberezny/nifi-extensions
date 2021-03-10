package pl.touk.nifi.services;

import org.apache.nifi.distributed.cache.client.*;
import org.apache.nifi.util.Tuple;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/*
TODO:
1. Implement AtomicDistributedMapCacheClient<byte[]>
2. Add support for DistributedMapCacheClient#removeByPattern
 */
public class IgniteDistributedMapCacheClient extends AbstractIgniteCache<byte[], byte[]> implements DistributedMapCacheClient {

    @Override
    public <K, V> boolean putIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        Tuple<byte[],byte[]> kv = serialize(key, value, keySerializer, valueSerializer);
        return getCache().putIfAbsent(kv.getKey(), kv.getValue());
    }

    @Override
    public <K, V> V getAndPutIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer, Deserializer<V> valueDeserializer) throws IOException {
        Tuple<byte[],byte[]> kv = serialize(key, value, keySerializer, valueSerializer);
        if (getCache().putIfAbsent(kv.getKey(), kv.getValue())) {
            return value;
        } else {
            return null;
        }
    }

    @Override
    public <K> boolean containsKey(K key, Serializer<K> keySerializer) throws IOException {
        byte[] keyBytes = serialize(key, keySerializer);
        return getCache().containsKey(keyBytes);
    }

    @Override
    public <K, V> void put(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        Tuple<byte[],byte[]> kv = serialize(key, value, keySerializer, valueSerializer);
        getCache().put(kv.getKey(), kv.getValue());
    }

    @Override
    public <K, V> V get(K key, Serializer<K> keySerializer, Deserializer<V> valueDeserializer) throws IOException {
        byte[] keyBytes = serialize(key, keySerializer);
        byte[] valueBytes = getCache().get(keyBytes);
        return valueBytes == null ? null : valueDeserializer.deserialize(valueBytes);
    }

    @Override
    public void close() throws IOException {}

    @Override
    public <K> boolean remove(K key, Serializer<K> keySerializer) throws IOException {
        byte[] keyBytes = serialize(key, keySerializer);
        return getCache().remove(keyBytes);
    }

    @Override
    public long removeByPattern(String s) {
        throw new UnsupportedOperationException("Remove by pattern is not supported");
    }

    private <K, V> Tuple<byte[],byte[]> serialize(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        keySerializer.serialize(key, out);
        byte[] k = out.toByteArray();
        out.reset();
        valueSerializer.serialize(value, out);
        byte[] v = out.toByteArray();
        return new Tuple<>(k, v);
    }

    private <K> byte[] serialize(final K key, final Serializer<K> keySerializer) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        keySerializer.serialize(key, out);
        return out.toByteArray();
    }
}
