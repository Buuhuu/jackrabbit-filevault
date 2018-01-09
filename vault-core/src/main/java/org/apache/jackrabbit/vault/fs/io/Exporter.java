package org.apache.jackrabbit.vault.fs.io;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Calendar;
import java.util.Properties;

import javax.jcr.Node;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.vault.fs.api.ProgressTrackerListener;
import org.apache.jackrabbit.vault.fs.api.VaultFile;

public interface Exporter {

    boolean isVerbose();

    void setVerbose(ProgressTrackerListener out);

    boolean isRelativePaths();

    void setProperty(String name, String value);

    void setProperty(String name, Calendar value);

    void setProperties(Properties properties);

    String getRootPath();

    void setRootPath(String rootPath);
    boolean isNoMetaInf();

    void setNoMetaInf(boolean noMetaInf);

    ExportInfo getExportInfo();

    /**
     * Defines if the exported files should include their entire path or just
     * be relative to the export root. eg.: exporting /apps/components relative
     * would not include /apps in the path.
     *
     * @param relativePaths relative flag
     */
    void setRelativePaths(boolean relativePaths);

    /**
     * Exports the given vault file and writes the META-INF data.
     * @param parent the vault file
     * @throws RepositoryException if an error occurs
     * @throws IOException if an I/O error occurs
     */
    void export(VaultFile parent) throws RepositoryException, IOException;

    /**
     * Exports the given vault file and writes the META-INF data.
     * @param parent the vault file
     * @param noClose if {@code true} exporter will not be closed after export
     * @throws RepositoryException if an error occurs
     * @throws IOException if an I/O error occurs
     */
    void export(VaultFile parent, boolean noClose) throws RepositoryException, IOException;

    /**
     * Exports the vault file to the relative path.
     * @param parent the file
     * @param relPath the path
     * @throws RepositoryException if an error occurs
     * @throws IOException if an I/O error occurs
     */
    void export(VaultFile parent, String relPath) throws RepositoryException, IOException;

    /**
     * Opens the exporter and initializes the undelying structures.
     * @throws IOException if an I/O error occurs
     * @throws RepositoryException if a repository error occurs
     */
    void open() throws IOException;

    /**
     * Closes the exporter and releases the undelying structures.
     * @throws IOException if an I/O error occurs
     * @throws RepositoryException if a repository error occurs
     */
    void close() throws IOException;

    void createDirectory(String relPath)
            throws IOException;

    void createDirectory(VaultFile file, String relPath)
            throws RepositoryException, IOException;

    void writeFile(InputStream in, String relPath)
            throws IOException;

    void writeFile(VaultFile file, String relPath)
            throws RepositoryException, IOException;

    /**
     * Copies the given {@link org.apache.jackrabbit.vault.fs.io.Archive.Entry} from the given {@link Archive} to the newly exported one.
     *
     * @param fromArchive
     * @param fromEntry
     * @throws IOException
     */
    void write(Archive fromArchive, Archive.Entry fromEntry) throws IOException;

    interface ExporterFactory { }

    interface FileExporterFactory extends ExporterFactory {
        Exporter createExporter(File target, int compressionLevel);
    }
    interface StreamExporterFactory extends ExporterFactory {
        Exporter createExporter(OutputStream target, int compressionLevel);
    }
    interface NodeExporterFactory extends ExporterFactory {
        Exporter createExporter(Node target, int compressionLevel);
    }
}
