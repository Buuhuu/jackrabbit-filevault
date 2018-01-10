/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.vault.fs.io;

import static java.util.zip.Deflater.BEST_COMPRESSION;
import static java.util.zip.Deflater.DEFAULT_COMPRESSION;
import static java.util.zip.Deflater.NO_COMPRESSION;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.jcr.RepositoryException;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.jar.JarArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.vault.fs.api.Artifact;
import org.apache.jackrabbit.vault.fs.api.VaultFile;
import org.apache.jackrabbit.vault.fs.impl.io.CompressionUtil;
import org.apache.jackrabbit.vault.util.PlatformNameFormat;

/**
 * Implements a Vault filesystem exporter that exports Vault files to a jar file.
 * The entries are stored compressed in the jar (as {@link ZipArchiveEntry} zip entries.
 * <p>
 * The exporter can optimize the export throughput for binaries, by avoiding to
 * compress incompressible binaries.
 * The optimization is enabled for all {@link java.util.zip.Deflater} compression levels but
 * {@link java.util.zip.Deflater#DEFAULT_COMPRESSION}, {@link java.util.zip.Deflater#NO_COMPRESSION} and
 * {@link java.util.zip.Deflater#BEST_COMPRESSION}.
 * <p>
 * The exporter uses the {@link PlatformNameFormat} for formatting the jcr file
 * names to local ones.
 */
public class JarExporter extends AbstractArchiveExporter<ZipArchiveOutputStream> {

    /**
     * Contains the compression levels for which the binaries are always compressed
     * independently of their actual compressibility.
     */
    private static final Set<Integer> COMPRESSED_LEVELS = new HashSet<Integer>(Arrays.asList(
            DEFAULT_COMPRESSION, NO_COMPRESSION, BEST_COMPRESSION));

    private OutputStream out;

    private File jarFile;

    private final int level;

    private final boolean compressedLevel;

    /**
     * Constructs a new jar exporter that writes to the given file.
     *
     * @param jarFile the jar file
     */
    public JarExporter(File jarFile) {
        this(jarFile, DEFAULT_COMPRESSION);
    }

    /**
     * Constructs a new jar exporter that writes to the given file.
     *
     * @param jarFile the jar file
     * @param level   level the compression level
     */
    public JarExporter(File jarFile, int level) {
        compressedLevel = COMPRESSED_LEVELS.contains(level);
        this.jarFile = jarFile;
        this.level = level;

    }

    /**
     * Constructs a new jar exporter that writes to the output stream.
     *
     * @param out the output stream
     */
    public JarExporter(OutputStream out) {
        this(out, DEFAULT_COMPRESSION);
    }

    /**
     * Constructs a new jar exporter that writes to the output stream.
     *
     * @param out   the output stream
     * @param level level the compression level
     */
    public JarExporter(OutputStream out, int level) {
        compressedLevel = COMPRESSED_LEVELS.contains(level);
        this.out = out;
        this.level = level;
    }

    @Override
    protected ZipArchiveOutputStream openArchiveOutputStream() throws IOException {
        JarArchiveOutputStream outputStream;
        if (jarFile != null) {
            outputStream = new JarArchiveOutputStream(new FileOutputStream(jarFile));
        } else if (out != null) {
            outputStream = new JarArchiveOutputStream(out);
        } else {
            throw new IllegalArgumentException("Either out or jarFile needs to be set.");
        }

        outputStream.setLevel(level);
        return outputStream;
    }

    @Override
    protected ArchiveEntry createEntry(String name) {
        return new ZipArchiveEntry(name);
    }

    @Override
    protected ArchiveEntry createEntry(VaultFile file, String name) {
        Artifact a = file.getArtifact();
        ZipArchiveEntry entry = new ZipArchiveEntry(name);
        if (a.getLastModified() > 0) {
            entry.setTime(a.getLastModified());
        }
        return entry;
    }

    @Override
    protected ArchiveEntry createEntry(InputStream file, String name) {
        return createEntry(name); // we don't get further information about the entry from the InputStream.
    }

    public void writeFile(VaultFile file, String relPath)
            throws RepositoryException, IOException {
        boolean compress = compressedLevel || CompressionUtil.isCompressible(file.getArtifact()) >= 0;
        if (!compress) {
            archiveOut.setLevel(NO_COMPRESSION);
        }

        super.writeFile(file, relPath);

        if (!compress) {
            archiveOut.setLevel(level);
        }
    }

    public void write(Archive archive, Archive.Entry entry) throws IOException {
        if (archive instanceof ZipArchive) {
            ZipFile zipFile = ((ZipArchive) archive).getZipFile();
            ZipArchiveEntry zipEntry = zipFile.getEntry(entry.getRelPath());
            if (zipEntry == null) {
                // the entry doesn't exists, so it might have been created as intermediate of an existing entry.
                // as we are copying entries from one archive to another, we skip that case as well
                return;
            }
            track("A", entry.getName());
            if (!compressedLevel) {
                // The entry to be written is assumed to be incompressible
                archiveOut.setLevel(NO_COMPRESSION);
            }
            exportInfo.update(ExportInfo.Type.ADD, entry.getName());
            ZipArchiveEntry copy = new ZipArchiveEntry(zipEntry);
            copy.setCompressedSize(-1);
            archiveOut.putArchiveEntry(copy);
            if (!entry.isDirectory()) {
                // copy
                InputStream in = zipFile.getInputStream(zipEntry);
                IOUtils.copy(in, archiveOut);
                in.close();
            }
            archiveOut.closeArchiveEntry();
            if (!compressedLevel) {
                archiveOut.setLevel(level);
            }
        } else {
            super.write(archive, entry);
        }
    }
}