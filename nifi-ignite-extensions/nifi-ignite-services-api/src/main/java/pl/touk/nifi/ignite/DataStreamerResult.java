package pl.touk.nifi.ignite;

import javax.cache.CacheException;

public class DataStreamerResult {

    private CacheException error;

    public DataStreamerResult() {
        this.error = null;
    }

    public DataStreamerResult(CacheException error) {
        this.error = error;
    }

    public boolean isSuccess() {
        return error == null;
    }

    public CacheException getError() {
        return error;
    }

    public void setError(CacheException e) {
        this.error = e;
    }
}
