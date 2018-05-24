package sys.storage;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;

import api.storage.Datanode;
import utils.dropbox.DropboxClient;

public class DatanodeProxy implements Datanode {
	private String address;

	public DatanodeProxy(String myURL) {
		this.address = myURL;
	}
	public String createBlock(byte[] data) {
		String blockId=null;utils.Random.key64();
		DropboxClient.createFile("/"+blockId, data);
		return address + PATH + "/" + blockId;
	}

	@Override
	public void deleteBlock(String block) {
		if(!DropboxClient.delete(block))
			throw new WebApplicationException(Status.NOT_FOUND);

	}

	@Override
	public byte[] readBlock(String block) {
		byte[] file= DropboxClient.getFile(block);
		if(file==null)
			throw new WebApplicationException(Status.NOT_FOUND);
		return file;
	}

}
