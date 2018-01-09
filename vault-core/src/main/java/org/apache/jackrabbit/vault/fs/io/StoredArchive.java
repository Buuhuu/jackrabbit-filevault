package org.apache.jackrabbit.vault.fs.io;

import java.io.File;

/**
 * A {@link StoredArchive} is a kind of {@link Archive} that is persisted on disc and accessible with Java's File API.
 */
public interface StoredArchive extends Archive {

    /**
     * Returns the file of the archive.
     *
     * @return
     */
    File getFile();

    /**
     * Returns the file size of the file of the archive.
     *
     * @return
     */
    long getFileSize();
}
