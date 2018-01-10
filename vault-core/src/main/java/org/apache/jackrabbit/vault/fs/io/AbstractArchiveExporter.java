package org.apache.jackrabbit.vault.fs.io;

import static java.util.zip.Deflater.BEST_COMPRESSION;
import static java.util.zip.Deflater.DEFAULT_COMPRESSION;
import static java.util.zip.Deflater.NO_COMPRESSION;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.jcr.RepositoryException;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.jackrabbit.vault.fs.api.Artifact;
import org.apache.jackrabbit.vault.fs.api.VaultFile;

public abstract class AbstractArchiveExporter<O extends ArchiveOutputStream> extends AbstractExporter {

    /**
     * Contains the compression levels for which the binaries are always compressed
     * independently of their actual compressibility.
     */
    private static final Set<Integer> COMPRESSED_LEVELS = new HashSet<Integer>(Arrays.asList(
            DEFAULT_COMPRESSION, NO_COMPRESSION, BEST_COMPRESSION));

    protected O archiveOut;

    private OutputStream out;

    protected abstract O openArchiveOutputStream() throws IOException;

    protected abstract ArchiveEntry createEntry(String name);

    protected abstract ArchiveEntry createEntry(VaultFile file, String name) throws IOException, RepositoryException;

    protected abstract ArchiveEntry createEntry(InputStream file, String name) throws IOException;

    protected void spool(Artifact a, OutputStream out) throws IOException, RepositoryException {
        switch (a.getPreferredAccess()) {
        case NONE:
            throw new RepositoryException("Artifact has no content.");

        case SPOOL:
            OutputStream nout = new CloseShieldOutputStream(out);
            a.spool(nout);
            break;

        case STREAM:
            nout = new CloseShieldOutputStream(out);
            InputStream in = a.getInputStream();
            IOUtils.copy(in, nout);
            in.close();
            break;
        }
    }

    protected void spool(InputStream in, OutputStream out) throws IOException {
        OutputStream nout = new CloseShieldOutputStream(archiveOut);
        IOUtils.copy(in, nout);
    }

    /**
     * Opens the exporter and initializes the undelying structures.
     *
     * @throws IOException if an I/O error occurs
     */
    public void open() throws IOException {
        if (archiveOut == null) {
            archiveOut = openArchiveOutputStream();
        }
    }

    public void close() throws IOException {
        if (archiveOut != null) {
            archiveOut.close();
            archiveOut = null;
        }
    }

    public void createDirectory(VaultFile file, String relPath)
            throws RepositoryException, IOException {
        ArchiveEntry e = createEntry(getPlatformFilePath(file, relPath) + "/");
        archiveOut.putArchiveEntry(e);
        archiveOut.closeArchiveEntry();
        track("A", relPath);
        exportInfo.update(ExportInfo.Type.MKDIR, e.getName());
    }

    public void createDirectory(String relPath) throws IOException {
        ArchiveEntry e = createEntry(relPath + "/");
        archiveOut.putArchiveEntry(e);
        archiveOut.closeArchiveEntry();
        exportInfo.update(ExportInfo.Type.MKDIR, e.getName());
    }

    public void writeFile(VaultFile file, String relPath)
            throws RepositoryException, IOException {
        ArchiveEntry e = createEntry(file, getPlatformFilePath(file, relPath));
        Artifact a = file.getArtifact();
        track("A", relPath);
        exportInfo.update(ExportInfo.Type.ADD, e.getName());
        archiveOut.putArchiveEntry(e);
        spool(a, archiveOut);
        archiveOut.closeArchiveEntry();
    }

    public void writeFile(InputStream in, String relPath) throws IOException {
        // The file input stream to be written is assumed to be compressible
        ArchiveEntry e = createEntry(in, relPath);
        exportInfo.update(ExportInfo.Type.ADD, e.getName());
        archiveOut.putArchiveEntry(e);
        spool(in, archiveOut);
        in.close();
        archiveOut.closeArchiveEntry();
    }
}
