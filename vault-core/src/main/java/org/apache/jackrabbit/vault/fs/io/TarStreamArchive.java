package org.apache.jackrabbit.vault.fs.io;

import java.io.InputStream;

import javax.annotation.Nonnull;

import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

public class TarStreamArchive extends AbstractStreamArchive {

    private final InputStream in;

    /**
     * Creates an new archive stream archive on the given input stream.
     *
     * @param in the input stream to read from.
     */
    public TarStreamArchive(@Nonnull InputStream in) {
        this.in = in;
    }

    /**
     * Creates an ew archive stream archive on the given input stream.
     *
     * @param in            the input stream to read from.
     * @param maxBufferSize size of buffer to keep content in memory.
     */
    public TarStreamArchive(@Nonnull InputStream in, int maxBufferSize) {
        super(maxBufferSize);
        this.in = in;
    }

    @Override
    protected ArchiveInputStream openArchiveInputStream() {
        return new TarArchiveInputStream(in);
    }
}
