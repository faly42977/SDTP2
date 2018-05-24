package test.misc;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import utils.dropbox.*;

public class DropboxTest {
	
	static String apiKey = "3798wk62thvohz1";
	static String apiSecret ="6kjgm89e1j9vnn0";
	static String token = "_OX6R4XogdAAAAAAAAAALVSrIPpLylLIxZ4WcUYoA-Q";
	static String lastToken = "_OX6R4XogdAAAAAAAAAALn_CrsSsR-dXYgqwDnK2Jk0";
	
	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
			
		DropboxClient.createDir("/dir");
		//DropboxClient.createFile("/a",new byte[] {} );
	}
}
