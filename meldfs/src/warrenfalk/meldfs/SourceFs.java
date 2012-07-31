package warrenfalk.meldfs;

import java.io.File;

public class SourceFs {
	File location;
	
	public SourceFs(File location) {
		this.location = location;
	}

	public static SourceFs[] fromFiles(File[] dirs) {
		SourceFs[] sources = new SourceFs[dirs.length];
		for (int i = 0; i < dirs.length; i++)
			sources[i] = new SourceFs(dirs[i]);
		return sources;
	}
}
