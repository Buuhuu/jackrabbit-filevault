package org.apache.jackrabbit.vault.fs.io;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.annotation.Nonnull;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.jackrabbit.vault.fs.impl.StoredArchiveWrapper;

public class AutodetectArchive extends StoredArchiveWrapper {

    /**
     * Creates a new archive that is based on the given zip file.
     *
     * @param zipFile the zip file
     */
    public AutodetectArchive(@Nonnull File zipFile) throws IOException {
        this(zipFile, false);
    }

    /**
     * Creates a new archive that is based on the given zip file.
     *
     * @param zipFile    the zip file
     * @param isTempFile if {@code true} if the file is considered temporary and can be deleted after this archive is closed.
     */
    public AutodetectArchive(@Nonnull File zipFile, boolean isTempFile) throws IOException {
        super(detectArchive(zipFile, isTempFile));
    }

    private static StoredArchive detectArchive(File file, boolean isTempFile) throws IOException {
        try (InputStream stream = new BufferedInputStream(new FileInputStream(file), 13)) {
            return detectArchive(stream, file, isTempFile);
        }
    }

    private static StoredArchive detectArchive(InputStream stream, File file, boolean isTempFile) throws IOException {
        try {
            String name = ArchiveStreamFactory.detect(stream);
            if ("zip".equals(name)) {
                // deflate has its own implementation so we use it here
                if (isTempFile) {
                    return new ZipArchive(file, isTempFile);
                } else {
                    return new ZipArchive(file);
                }
            }
        } catch (ArchiveException ex) {
            // ignore
        }
        try {
            String name = CompressorStreamFactory.detect(stream);
            // otherwise all other compressions wrap a tarball.
            if (isTempFile) {
                return new TarArchive(file, name, isTempFile);
            } else {
                return new TarArchive(file, name);
            }

        } catch (CompressorException ex) {
            throw new IllegalArgumentException("Unknown format.");
        }
    }
}
