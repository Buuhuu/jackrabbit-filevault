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
