package org.apache.jackrabbit.vault.fs.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.jcr.RepositoryException;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.utils.CountingOutputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.jackrabbit.vault.fs.api.Artifact;
import org.apache.jackrabbit.vault.fs.api.VaultFile;

public class TarExporter extends AbstractArchiveExporter<TarArchiveOutputStream> {

    private File file;
    private OutputStream out;
    private CappedBuffer recentlyBuffered;

    /**
     * Constructs a new jar exporter that writes to the given file.
     *
     * @param tarFile the jar file
     */
    public TarExporter(File tarFile) {
        this.file = tarFile;
    }

    /**
     * Constructs a new jar exporter that writes to the output stream.
     *
     * @param out the output stream
     */
    public TarExporter(OutputStream out) {
        this.out = out;
    }

    private void closeRecentlyBuffered() throws IOException {
        if (recentlyBuffered != null) {
            recentlyBuffered.close();
            recentlyBuffered = null;
        }
    }

    @Override
    protected void spool(Artifact a, OutputStream out) throws IOException, RepositoryException {
        if (recentlyBuffered != null) {
            // if we have stuff in memory already for the most recent artifact
            IOUtils.copy(recentlyBuffered.getInputStream(), out);
            closeRecentlyBuffered();
        }
        super.spool(a, out);
    }

    @Override
    protected void spool(InputStream in, OutputStream out) throws IOException {
        if (recentlyBuffered != null) {
            super.spool(recentlyBuffered.getInputStream(), out);
            closeRecentlyBuffered();
        } else {
            super.spool(in, out);
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        closeRecentlyBuffered();
    }

    @Override
    protected TarArchiveOutputStream openArchiveOutputStream() throws IOException {
        if (file != null) {
            return new TarArchiveOutputStream(new FileOutputStream(file));
        } else if (out != null) {
            return new TarArchiveOutputStream(out);
        } else {
            throw new IllegalArgumentException("Either out or jarFile needs to be set.");
        }
    }

    @Override
    protected ArchiveEntry createEntry(String name) {
        return createEntry(name, 0); // for directories
    }

    private ArchiveEntry createEntry(String name, long size) {
        return createEntry(name, size, 0);
    }

    private ArchiveEntry createEntry(String name, long size, long lastMod) {
        TarArchiveEntry entry = new TarArchiveEntry(name);
        entry.setSize(size);
        if (lastMod > 0) {
            entry.setModTime(lastMod);
        }
        return entry;
    }

    @Override
    protected ArchiveEntry createEntry(VaultFile file, String name) throws IOException, RepositoryException {
        if (file.length() < 0) {
            // unknown size
            closeRecentlyBuffered();
            recentlyBuffered = new CappedBuffer();
            super.spool(file.getArtifact(), recentlyBuffered);
            return createEntry(name, recentlyBuffered.getBytesWritten(), file.lastModified());
        } else {
            return createEntry(name, file.length(), file.lastModified());
        }
    }

    @Override
    protected ArchiveEntry createEntry(InputStream inputStream, String name) throws IOException {
        if (inputStream instanceof ByteArrayInputStream) {
            return createEntry(name, inputStream.available());
        } else {
            closeRecentlyBuffered();
            recentlyBuffered = new CappedBuffer();
            IOUtils.copy(inputStream, recentlyBuffered);
            return createEntry(name, recentlyBuffered.getBytesWritten());
        }
    }

    protected class CappedBuffer extends OutputStream {
        /**
         * the maximum buffer size used when we have to count the binary input size
         */
        private static final int MAX_BUFFER_SIZE = 10 * 1024 * 104; // 10MB

        private ByteArrayOutputStream inMemory;
        private File tempFile;
        private CountingOutputStream out;

        CappedBuffer() {
            inMemory = new ByteArrayOutputStream();
            out = new CountingOutputStream(inMemory);
        }

        private void checkOverflow() throws IOException {
            if (tempFile == null && out.getBytesWritten() > MAX_BUFFER_SIZE) {
                // we are writing to inMemory and we exceeded the maximum in memory size so we create a temp file and flush the buffer to it
                out.flush();
                inMemory.flush();
                tempFile = File.createTempFile(TarExporter.this.toString(), this.toString());
                out = new CountingOutputStream(new FileOutputStream(tempFile));
                inMemory.writeTo(out);
                inMemory.close();
                inMemory = null;
            }
        }

        public long getBytesWritten() {
            return out.getBytesWritten();
        }

        public InputStream getInputStream() throws IOException {
            if (tempFile == null) {
                return new ByteArrayInputStream(inMemory.toByteArray());
            } else {
                return new FileInputStream(tempFile);
            }
        }

        @Override
        public void write(byte[] b) throws IOException {
            out.write(b);
            checkOverflow();
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
        }

        @Override
        public void flush() throws IOException {
            out.flush();
        }

        @Override
        public void close() throws IOException {
            try {
                out.close();
            } finally {
                if (tempFile != null) {
                    tempFile.delete();
                }
            }
        }
    }

}
