package warrenfalk.meldfs;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

import warrenfalk.fuselaj.FileInfo;

public class FuseFileHandle {
	final long number;
	Object data;
	
	private static AtomicLong nextHandle = new AtomicLong();
	private static HashMap<Long,FuseFileHandle> map = new HashMap<Long,FuseFileHandle>();
	
	private FuseFileHandle(Object data) {
		this.number = nextHandle.incrementAndGet();
		this.data = data;
	}
	
	public static FuseFileHandle open(FileInfo fi, Object data) {
		FuseFileHandle handle = new FuseFileHandle(data);
		fi.putFileHandle(handle.number);
		synchronized (map) {
			map.put(handle.number, handle);
		}
		return handle;
	}
	
	public static FuseFileHandle release(FileInfo fi) {
		FuseFileHandle handle;
		synchronized (map) {
			handle = map.remove(fi.getFileHandle());
		}
		return handle;
	}
	
	public static FuseFileHandle get(long number) {
		synchronized (map) {
			return map.get(number);
		}
	}
}
