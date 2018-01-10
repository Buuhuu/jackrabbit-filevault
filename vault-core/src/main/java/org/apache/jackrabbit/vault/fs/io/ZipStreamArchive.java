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

import java.io.InputStream;

import javax.annotation.Nonnull;

import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;

/**
 * Implements an archive based on a zip stream, but deflates the entries first into a buffer and later into a temporary
 * file, if the content length exceeds the buffer size.
 */
public class ZipStreamArchive extends AbstractStreamArchive {

    /**
     * Creates an ew archive stream archive on the given input stream.
     * @param in the input stream to read from.
     */
    public ZipStreamArchive(@Nonnull InputStream in) {
        super(in);
    }

    /**
     * Creates an ew archive stream archive on the given input stream.
     * @param in the input stream to read from.
     * @param maxBufferSize size of buffer to keep content in memory.
     */
    public ZipStreamArchive(@Nonnull InputStream in, int maxBufferSize) {
        super(in, maxBufferSize);
    }

    @Override
    protected ArchiveInputStream openArchiveInputStream(InputStream source) {
        return new ZipArchiveInputStream(source);
    }
}