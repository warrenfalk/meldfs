package warrenfalk.meldfs;

import warrenfalk.fuselaj.FilesystemException;

public interface SourceOp {
	void run(int index, SourceFs source) throws FilesystemException;
}
