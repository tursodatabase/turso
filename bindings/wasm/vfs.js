class VFS {
  constructor() {
    this.files = new Map();
    this.nextFd = 1;
  }

  open(path, flags) {
    const fileHandle = {
      path,
      flags,
      position: 0,
      data: new Uint8Array(),
    };
    const fd = this.nextFd++;
    this.files.set(fd, fileHandle);
    return fd;
  }

  close(fd) {
    this.files.delete(fd);
  }

  pread(fd, buffer, offset) {
    const fileHandle = this.files.get(fd);
    if (!fileHandle) {
      throw new Error(`File descriptor ${fd} not found`);
    }
    const bytesRead = fileHandle.data.subarray(offset, offset + buffer.length);
    buffer.set(bytesRead);
    return bytesRead.length;
  }

  pwrite(fd, buffer, offset) {
    const fileHandle = this.files.get(fd);
    if (!fileHandle) {
      throw new Error(`File descriptor ${fd} not found`);
    }
    const newData = new Uint8Array(offset + buffer.length);
    newData.set(fileHandle.data.subarray(0, offset));
    newData.set(buffer, offset);
    fileHandle.data = newData;
    return buffer.length;
  }

  size(fd) {
    const fileHandle = this.files.get(fd);
    if (!fileHandle) {
      throw new Error(`File descriptor ${fd} not found`);
    }
    return BigInt(fileHandle.data.length);
  }

  sync(fd) {
    // No-op for in-memory VFS
  }
}

module.exports = { VFS };
