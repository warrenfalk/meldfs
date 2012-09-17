package warrenfalk.meldfs;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class MeldFsProperties {
	HashMap<String,Object> values;
	
	public MeldFsProperties() throws IOException {
		values = new HashMap<>();
		// by default, these are loaded from /etc/meldfs/default
		values.put("source", new ArrayList<String>());
		loadFrom(FileSystems.getDefault().getPath("/", "etc", "meldfs", "default"));
	}

	private void loadFrom(Path path) throws IOException {
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path.toFile())))) {
			String line;
			while (null != (line = reader.readLine())) {
				processLine(line);
			}
		}
	}

	private void processLine(String line) {
		line = line.trim();
		if (line.startsWith("#"))
			return;
		int e = line.indexOf('=');
		if (e == -1) {
			setBoolean(line, true);
			return;
		}
		String name = line.substring(0, e).trim();
		String val = line.substring(e + 1).trim();
		setString(name, val);
	}
	
	public void setBoolean(String name, boolean value) {
		values.put(name, true);
	}
	
	@SuppressWarnings("unchecked")
	public void setString(String name, String value) {
		Object curval = values.get(name);
		if (curval instanceof List<?>) {
			((List<Object>)curval).add(value);
			return;
		}
		values.put(name, value);
	}
	
	public Path[] getSources() {
		FileSystem fs = FileSystems.getDefault();
		@SuppressWarnings("unchecked")
		List<String> pathList = (List<String>)values.get("source");
		Path[] result = new Path[pathList.size()];
		for (int i = 0; i < result.length; i++)
			result[i] = fs.getPath(pathList.get(i));
		return result;
	}
}
