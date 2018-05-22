package rest.server;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;

public class DataResources implements api.DataResources {
    
    private static final int ISIZE = 128;
    private final Map<String, byte[]> storage;

    public DataResources() {
        this.storage = new ConcurrentHashMap<>(ISIZE);
    }
    
    @Override
	public String store(byte[] data) {
        String id = Long.toString( System.nanoTime(), 32);
        if( storage.putIfAbsent(id, data) != null)
            throw new WebApplicationException( Status.CONFLICT );
        else
            return id;
    }
    
    @Override
	public byte[] download(String id) {
        byte[]  data = storage.get(id);
        if( data == null )
            throw new WebApplicationException( Status.NOT_FOUND );
        else
            return data;
    }

    @Override
	public void replace(String id, byte[] data) {
        if( storage.replace( id, data ) == null ) {
            throw new WebApplicationException(Status.NOT_FOUND );            
        }
    }
    
    @Override
	public void delete(String id) {
        if( storage.remove( id ) == null ) {
            throw new WebApplicationException(Status.NOT_FOUND );            
        }      
    }
}