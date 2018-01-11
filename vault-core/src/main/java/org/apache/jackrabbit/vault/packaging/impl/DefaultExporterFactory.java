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

package org.apache.jackrabbit.vault.packaging.impl;

import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.Nonnull;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.jackrabbit.vault.fs.io.AbstractExporter;
import org.apache.jackrabbit.vault.fs.io.JarExporter;
import org.apache.jackrabbit.vault.fs.io.TarExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyFramedOutputStream;

public class DefaultExporterFactory {

    private static final CompressorStreamFactory COMPRESSOR_STREAM_FACTORY = new CompressorStreamFactory();
    private static final Logger LOG = LoggerFactory.getLogger(DefaultExporterFactory.class);

    @Nonnull
    public AbstractExporter createExporter(@Nonnull OutputStream outputStream, @Nonnull String compressionMethod, int compressionLevel)
            throws IOException {
        try {
            if ("deflate".equals(compressionMethod)) {
                return new JarExporter(outputStream, compressionLevel);
            }

            if ("snappy-framed".equals(compressionMethod)) {
                try {
                    new TarExporter(new SnappyFramedOutputStream(outputStream));
                } catch (NoClassDefFoundError ex) {
                    LOG.trace("Native (JNI) snappy implementation not found.", ex);
                }
            }

            return new TarExporter(COMPRESSOR_STREAM_FACTORY.createCompressorOutputStream(compressionMethod, outputStream));
        } catch (CompressorException ex) {
            throw new IllegalArgumentException("Unknown compression format.");
        }
    }
}
