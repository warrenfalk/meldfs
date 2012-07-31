package warrenfalk.meldfs;

import java.io.IOException;
import java.nio.file.Path;

public class SourceFs {
	Path root;
	
	public SourceFs(Path root) {
		this.root = root;
	}

	public static SourceFs[] fromPaths(Path[] dirs) {
		SourceFs[] sources = new SourceFs[dirs.length];
		for (int i = 0; i < dirs.length; i++)
			sources[i] = new SourceFs(dirs[i]);
		return sources;
	}

	public void onIoException(IOException ioe) {
		// TODO implement
	}
}
