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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.jar.JarFile;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.vault.fs.Mounter;
import org.apache.jackrabbit.vault.fs.api.RepositoryAddress;
import org.apache.jackrabbit.vault.fs.api.VaultFileSystem;
import org.apache.jackrabbit.vault.fs.api.VaultFsConfig;
import org.apache.jackrabbit.vault.fs.config.DefaultMetaInf;
import org.apache.jackrabbit.vault.fs.config.MetaInf;
import org.apache.jackrabbit.vault.fs.impl.AggregateManagerImpl;
import org.apache.jackrabbit.vault.fs.io.AbstractExporter;
import org.apache.jackrabbit.vault.fs.io.Archive;
import org.apache.jackrabbit.vault.fs.spi.ProgressTracker;
import org.apache.jackrabbit.vault.packaging.ExportOptions;
import org.apache.jackrabbit.vault.packaging.PackageId;
import org.apache.jackrabbit.vault.packaging.PackageManager;
import org.apache.jackrabbit.vault.packaging.PackageProperties;
import org.apache.jackrabbit.vault.packaging.VaultPackage;
import org.apache.jackrabbit.vault.packaging.events.PackageEvent;
import org.apache.jackrabbit.vault.packaging.events.impl.PackageEventDispatcher;
import org.apache.jackrabbit.vault.util.Constants;

/**
 * Implements the package manager
 */
public class PackageManagerImpl implements PackageManager {

    /**
     * event dispatcher
     */
    @Nullable
    private PackageEventDispatcher dispatcher;

    /**
     * {@inheritDoc}
     */
    public VaultPackage open(File file) throws IOException {
        return open(file, false);
    }

    /**
     * {@inheritDoc}
     */
    public VaultPackage open(File file, boolean strict) throws IOException {
        return new VaultPackageImpl(file, false, strict);
    }

    /**
     * {@inheritDoc}
     */
    public VaultPackage assemble(Session s, ExportOptions opts, File file)
            throws IOException, RepositoryException {
        OutputStream out = null;
        boolean isTmp = false;
        boolean success = false;
        try {
            if (file == null) {
                file = File.createTempFile("filevault", ".zip");
                isTmp = true;
            }
            out = FileUtils.openOutputStream(file);
            assemble(s, opts, out);
            IOUtils.closeQuietly(out);
            success = true;
            return new VaultPackageImpl(file, isTmp);
        } finally {
            IOUtils.closeQuietly(out);
            if (isTmp && !success) {
                FileUtils.deleteQuietly(file);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    public void assemble(Session s, ExportOptions opts, OutputStream out)
            throws IOException, RepositoryException {
        RepositoryAddress addr;
        try {
            String mountPath = opts.getMountPath();
            if (mountPath == null || mountPath.length() == 0) {
                mountPath = "/";
            }
            addr = new RepositoryAddress("/" + s.getWorkspace().getName() + mountPath);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
        MetaInf metaInf = opts.getMetaInf();
        if (metaInf == null) {
            metaInf = new DefaultMetaInf();
        }

        VaultFsConfig config = metaInf.getConfig();
        if (metaInf.getProperties() != null) {
            if ("true".equals(metaInf.getProperties().getProperty(PackageProperties.NAME_USE_BINARY_REFERENCES))) {
                config = AggregateManagerImpl.getDefaultBinaryReferencesConfig();
            }
        }

        VaultFileSystem jcrfs = Mounter.mount(config, metaInf.getFilter(), addr, opts.getRootPath(), s);
        AbstractExporter exporter = new DefaultExporterFactory().createExporter(out, opts.getCompressionMethod(), opts.getCompressionLevel());
        exporter.setProperties(metaInf.getProperties());
        if (opts.getListener() != null) {
            exporter.setVerbose(opts.getListener());
        }
        if (opts.getPostProcessor() != null) {
            exporter.export(jcrfs.getRoot(), true);
            opts.getPostProcessor().process(exporter);
            exporter.close();
        } else {
            exporter.export(jcrfs.getRoot());
        }
        jcrfs.unmount();
    }

    /**
     * {@inheritDoc}
     */
    public VaultPackage rewrap(ExportOptions opts, VaultPackage src, File file)
            throws IOException, RepositoryException {
        OutputStream out = null;
        boolean isTmp = false;
        boolean success = false;
        try {
            if (file == null) {
                file = File.createTempFile("filevault", ".zip");
                isTmp = true;
            }
            out = FileUtils.openOutputStream(file);
            rewrap(opts, src, out);
            IOUtils.closeQuietly(out);
            success = true;
            VaultPackage pack = new VaultPackageImpl(file, isTmp);
            dispatch(PackageEvent.Type.REWRAPP, pack.getId(), null);
            return pack;
        } finally {
            IOUtils.closeQuietly(out);
            if (isTmp && !success) {
                FileUtils.deleteQuietly(file);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    public void rewrap(ExportOptions opts, VaultPackage src, OutputStream out)
            throws IOException {
        MetaInf metaInf = opts.getMetaInf();
        if (metaInf == null) {
            metaInf = new DefaultMetaInf();
        }
        AbstractExporter exporter = new DefaultExporterFactory().createExporter(out, opts.getCompressionMethod(), opts.getCompressionLevel());
        exporter.open();
        exporter.setProperties(metaInf.getProperties());
        ProgressTracker tracker = null;
        if (opts.getListener() != null) {
            tracker = new ProgressTracker();
            exporter.setVerbose(opts.getListener());
        }

        // merge
        MetaInf inf = opts.getMetaInf();
        Archive archive = src.getArchive();
        archive.open(false);
        EntryTraversalIterator allEntries = new EntryTraversalIterator(archive);
        StringBuilder pathBuilder = new StringBuilder();
        if (opts.getPostProcessor() == null) {
            // no post processor, we keep all files except the properties
            while (allEntries.hasNext()) {
                EntryHolder entry = allEntries.next();
                String path = entry.relPath;
                if (!path.equals(Constants.META_DIR + "/" + Constants.PROPERTIES_XML)) {
                    exporter.write(archive, entry.entry, path);
                }
            }
        } else {
            Set<String> keep = new HashSet<String>();
            keep.add(Constants.META_DIR);
            keep.add(Constants.META_DIR + "/" + Constants.NODETYPES_CND);
            keep.add(Constants.META_DIR + "/" + Constants.CONFIG_XML);
            keep.add(Constants.META_DIR + "/" + Constants.FILTER_XML);
            while (allEntries.hasNext()) {
                EntryHolder entry = allEntries.next();
                String path = entry.relPath;
                if (!path.startsWith(Constants.META_DIR + "/") || keep.contains(path)) {
                    exporter.write(archive, entry.entry, path);
                }
            }
        }

        // write updated properties
        ByteArrayOutputStream tmpOut = new ByteArrayOutputStream();
        inf.getProperties().storeToXML(tmpOut, "FileVault Package Properties", "utf-8");
        exporter.writeFile(new ByteArrayInputStream(tmpOut.toByteArray()), Constants.META_DIR + "/" + Constants.PROPERTIES_XML);
        if (tracker != null) {
            tracker.track("A", Constants.META_DIR + "/" + Constants.PROPERTIES_XML);
        }

        if (opts.getPostProcessor() != null) {
            opts.getPostProcessor().process(exporter);
        }
        exporter.close();
    }

    @Nullable
    PackageEventDispatcher getDispatcher() {
        return dispatcher;
    }

    public void setDispatcher(@Nullable PackageEventDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    void dispatch(@Nonnull PackageEvent.Type type, @Nonnull PackageId id, @Nullable PackageId[] related) {
        if (dispatcher == null) {
            return;
        }
        dispatcher.dispatch(type, id, related);
    }

    protected static class EntryHolder {

        protected final Archive.Entry entry;
        protected final String relPath;

        private EntryHolder(Archive.Entry entry, String relPath) {
            this.entry = entry;
            this.relPath = relPath;
        }
    }

    /**
     * A Wrapper around {@link EntryTraversalIterator} that orders the {@link JarFile#MANIFEST_NAME} to the front.
     */
    protected static class AllEntries implements Iterator<EntryHolder> {

        private final EntryTraversalIterator entries;
        private Iterator<EntryHolder> cached;
        private EntryHolder next;

        AllEntries(Archive root) throws IOException {
            entries = new EntryTraversalIterator(root);
            // we want to return the manifest first so we iterate as long as we found it caching the results and moving it to the first
            // position. If the original archive was a jar this is a iteration of 2 META-INF and META-INF/MANIFEST.MF, if not the
            // entire entry tree will be duplicated in memory.
            Deque<EntryHolder> cachedEntries = new LinkedList<>();
            while (entries.hasNext()) {
                EntryHolder nextEntry = entries.next();
                if (JarFile.MANIFEST_NAME.equals(nextEntry.relPath)) {
                    cachedEntries.addFirst(nextEntry);
                    break;
                } else {
                    cachedEntries.addLast(nextEntry);
                }
            }
            cached = cachedEntries.iterator();
            seek();
        }

        private void seek() {
            next = null;
            if (cached != null) {
                if (cached.hasNext()) {
                    next = cached.next();
                } else {
                    cached = null;
                }
            }
            if (next == null && entries.hasNext()) {
                next = entries.next();
            }
        }

        @Override public boolean hasNext() {
            return next != null;
        }

        @Override public EntryHolder next() {
            EntryHolder current = next;
            seek();
            return current;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * An {@link Iterator} implementation traversing all entries of a given root excluding the root itself. A parent
     * is guaranteed to be returned before its children.
     */
    protected static class EntryTraversalIterator implements Iterator<EntryHolder> {

        private final Archive.Entry self;
        private final EntryTraversalIterator parent;
        private EntryTraversalIterator current;
        private Iterator<? extends Archive.Entry> children;
        private EntryHolder next;

        EntryTraversalIterator(Archive archive) throws IOException {
            this(archive.getRoot(), null);
            seek(); // we skip root
        }

        private EntryTraversalIterator(Archive.Entry entry, EntryTraversalIterator parent) {
            this.self = entry;
            this.parent = parent;
            this.children = entry.getChildren().iterator();

            StringBuilder path = getPath(new StringBuilder());
            this.next = new EntryHolder(entry, path.length() > 0 ? path.substring(1) : path.toString());
        }

        private StringBuilder getPath(StringBuilder builder) {
            return parent == null ? builder : parent.getPath(builder).append('/').append(self.getName());
        }

        private String getChildPath(Archive.Entry child) {
            return getPath(new StringBuilder()).append('/').append(child.getName()).substring(1);
        }

        private void seek() {
            next = null;
            if (next == null && current == null) {
                if (children.hasNext()) {
                    Archive.Entry child = children.next();
                    if (child.isDirectory()) {
                        current = new EntryTraversalIterator(child, this);
                    } else {
                        next = new EntryHolder(child, getChildPath(child));
                    }
                }
            }
            if (next == null && current != null) {
                if (current.hasNext()) {
                    next = current.next();
                } else {
                    current = null;
                    seek();
                }
            }
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public EntryHolder next() {
            EntryHolder current = next;
            seek();
            return current;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}