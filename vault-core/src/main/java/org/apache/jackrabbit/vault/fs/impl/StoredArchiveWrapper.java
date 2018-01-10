package org.apache.jackrabbit.vault.fs.impl;

import java.io.File;

import org.apache.jackrabbit.vault.fs.io.StoredArchive;

public abstract class StoredArchiveWrapper extends ArchiveWrapper implements StoredArchive {

    private final StoredArchive delegate;

    protected StoredArchiveWrapper(StoredArchive delegate) {
        super(delegate);
        this.delegate = delegate;
    }

    @Override
    public File getFile() {
        return delegate.getFile();
    }

    @Override
    public long getFileSize() {
        return delegate.getFileSize();
    }
}
