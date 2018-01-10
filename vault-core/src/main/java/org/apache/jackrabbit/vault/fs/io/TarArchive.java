package org.apache.jackrabbit.vault.fs.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import javax.annotation.Nonnull;

import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;

public class TarArchive extends AbstractStreamArchive {

    private final File file;
    private final boolean isTempFile;

    /**
     * Creates a new archive that is based on the given zip file.
     *
     * @param tarFile the tar file
     */
    public TarArchive(@Nonnull File tarFile) {
        this(tarFile, false);
    }

    /**
     * Creates a new archive that is based on the given zip file.
     *
     * @param tarFile    the zip file
     * @param isTempFile if {@code true} if the file is considered temporary and can be deleted after this archive is closed.
     */
    public TarArchive(@Nonnull File tarFile, boolean isTempFile) {
        this.file = tarFile;
        this.isTempFile = isTempFile;
    }

    @Override
    protected ArchiveInputStream openArchiveInputStream() throws IOException {
        // source will be null so
        return new TarArchiveInputStream(new FileInputStream(file));
    }

    @Override
    public void close() {
        super.close();
        if (file != null && isTempFile) {
            FileUtils.deleteQuietly(file);
        }
    }
}
