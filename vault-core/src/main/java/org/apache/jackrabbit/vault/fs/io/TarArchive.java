package org.apache.jackrabbit.vault.fs.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.io.FileUtils;

public class TarArchive extends AbstractStreamArchive implements StoredArchive {

    private final File file;
    private final boolean isTempFile;
    private final String compression;

    /**
     * Creates a new archive that is based on the given zip file.
     *
     * @param tarFile the tar file
     */
    public TarArchive(@Nonnull File tarFile) {
        this(tarFile, null, false);
    }

    /**
     * Creates a new archive that is based on the given zip file.
     *
     * @param tarFile the tar file
     */
    public TarArchive(@Nonnull File tarFile, String compression) {
        this(tarFile, compression, false);
    }

    /**
     * Creates a new archive that is based on the given zip file.
     *
     * @param tarFile    the zip file
     * @param isTempFile if {@code true} if the file is considered temporary and can be deleted after this archive is closed.
     */
    public TarArchive(@Nonnull File tarFile, boolean isTempFile) {
        this(tarFile, null, isTempFile);
    }

    public TarArchive(@Nonnull File tarFile, String compression, boolean isTempFile) {
        TarStreamArchive.requireNonDeflate(compression);
        this.file = tarFile;
        this.isTempFile = isTempFile;
        this.compression = compression;
    }

    @Override
    protected ArchiveInputStream openArchiveInputStream() throws IOException {
        return TarStreamArchive.getDecompressedStream(compression, new FileInputStream(file));
    }

    @Override
    public void close() {
        super.close();
        if (file != null && isTempFile) {
            FileUtils.deleteQuietly(file);
        }
    }

    /**
     * Returns the underlying file or {@code null} if it does not exist.
     * @return the file or null.
     */
    @Nullable
    public File getFile() {
        return file.exists() ? file : null;
    }

    /**
     * Returns the size of the underlying file or -1 if it does not exist.
     * @return the file size
     */
    public long getFileSize() {
        return file.length();
    }

}
