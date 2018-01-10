package org.apache.jackrabbit.vault.fs.io;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.utils.ArchiveUtils;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.jackrabbit.vault.fs.impl.ArchiveWrapper;

public class AutodetectStreamArchive extends ArchiveWrapper {

    public AutodetectStreamArchive(InputStream stream) throws IOException {
        this(stream, -1);
    }

    public AutodetectStreamArchive(InputStream stream, int maxBufferSize) throws IOException {
        super(detectArchive(new BufferedInputStream(stream), maxBufferSize));
    }

    private static Archive detectArchive(InputStream stream, int maxBufferSize) throws IOException {
        try {
            String name = ArchiveStreamFactory.detect(stream);
            if ("zip".equals(name)) {
                // deflate has its own implementation so we use it here
                if (maxBufferSize > 0) {
                    return new ZipStreamArchive(stream, maxBufferSize);
                } else {
                    return new ZipStreamArchive(stream);
                }
            }
        } catch (ArchiveException ex) {
            // ignore
        }
        try {
            String name = CompressorStreamFactory.detect(stream);
            if (maxBufferSize > 0) {
                return new TarStreamArchive(stream, name, maxBufferSize);
            } else {
                return new TarStreamArchive(stream, name);
            }
        } catch (CompressorException ex) {
            // ignore
        }
        try {
            // is the stream empty, then we fallback to the previous behaviour creating a ZipStreamArchive
            byte[] sig = new byte[12];
            stream.mark(sig.length);
            IOUtils.readFully(stream, sig);
            stream.reset();
            if (ArchiveUtils.isArrayZero(sig, sig.length)) {
                return new ZipStreamArchive(stream);
            }
        } catch (Exception ex) {
            // ignore
        }
        throw new IllegalArgumentException("Unknown format.");
    }
}
