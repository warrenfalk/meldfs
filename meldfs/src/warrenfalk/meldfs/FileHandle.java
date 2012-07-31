package warrenfalk.meldfs;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

import warrenfalk.fuselaj.FileInfo;

public class FileHandle {
	final long number;
	final Object data;
	
	private static AtomicLong nextHandle = new AtomicLong();
	private static HashMap<Long,FileHandle> map = new HashMap<Long,FileHandle>();
	
	private FileHandle(Object data) {
		this.number = nextHandle.incrementAndGet();
		this.data = data;
	}
	
	public static FileHandle open(FileInfo fi, Object data) {
		FileHandle handle = new FileHandle(data);
		fi.putFileHandle(handle.number);
		synchronized (map) {
			map.put(handle.number, handle);
		}
		return handle;
	}
	
	public static FileHandle release(FileInfo fi) {
		FileHandle handle;
		synchronized (map) {
			handle = map.remove(fi.getFileHandle());
		}
		return handle;
	}
	
	public static FileHandle get(long number) {
		synchronized (map) {
			return map.get(number);
		}
	}
}
