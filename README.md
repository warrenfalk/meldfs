# Meld Filesystem

The Meld Filesystem creates a virtual filesystem on top of existing filesystems which acts as a union of those filesystems.  Additionally, background processes can take care of spreading those files across the backing filesystems to achieve redundancy or balance free space.

MeldFs (when complete) will support Reed-Solomon arbitrary redundancy so that you can lose any X backing filesystems for any reason and still be able to rebuild.

This allows a trivial way to combine many drives into one huge storage area in the most efficient way possible.  Several mechanisms eliminate the need for most types of backup.
