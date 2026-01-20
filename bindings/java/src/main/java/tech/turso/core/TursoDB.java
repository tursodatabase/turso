package tech.turso.core;

import static tech.turso.utils.ByteArrayUtils.stringToUtf8ByteArray;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import tech.turso.TursoErrorCode;
import tech.turso.annotations.NativeInvocation;
import tech.turso.annotations.Nullable;
import tech.turso.annotations.VisibleForTesting;
import tech.turso.utils.Logger;
import tech.turso.utils.LoggerFactory;
import tech.turso.utils.TursoExceptionUtils;

/** This class provides a thin JNI layer over the SQLite3 C API. */
public final class TursoDB implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(TursoDB.class);
  // Pointer to database instance
  private long dbPointer;
  private boolean isOpen;

  private final String url;
  private final String filePath;

  static {
    if ("The Android Project".equals(System.getProperty("java.vm.vendor"))) {
      // TODO
    } else {
      // continue with non Android execution path
    }
  }

  /**
   * Enum representing different architectures and their corresponding library paths and file
   * extensions.
   */
  private enum Architecture {
    MACOS_ARM64("libs/macos_arm64/lib_turso_java.dylib", ".dylib"),
    MACOS_X86("libs/macos_x86/lib_turso_java.dylib", ".dylib"),
    LINUX_X86("libs/linux_x86/lib_turso_java.so", ".so"),
    WINDOWS("libs/windows/lib_turso_java.dll", ".dll"),
    UNSUPPORTED("", "");

    private final String libPath;
    private final String fileExtension;

    Architecture(String libPath, String fileExtension) {
      this.libPath = libPath;
      this.fileExtension = fileExtension;
    }

    public String getLibPath() {
      return libPath;
    }

    public String getFileExtension() {
      return fileExtension;
    }

    public static Architecture detect() {
      String osName = System.getProperty("os.name").toLowerCase();
      String osArch = System.getProperty("os.arch").toLowerCase();

      // TODO: add support for arm64 on Linux
      if (osName.contains("linux")) {
        if (osArch.contains("aarch64") || osArch.contains("arm64")) {
          throw new UnsupportedOperationException(
              "ARM64 architecture is not supported on Linux yet");
        } else if (osArch.contains("x86_64") || osArch.contains("amd64")) {
          return LINUX_X86;
        }
      }

      if (osName.contains("mac")) {
        if (osArch.contains("aarch64") || osArch.contains("arm64")) {
          return MACOS_ARM64;
        } else if (osArch.contains("x86_64") || osArch.contains("amd64")) {
          return MACOS_X86;
        }
      } else if (osName.contains("win")) {
        return WINDOWS;
      }

      return UNSUPPORTED;
    }
  }

  /**
   * This method attempts to load the native library required for turso operations. It first tries
   * to load the library from the system's library path using {@link #loadFromSystemPath()}. If that
   * fails, it attempts to load the library from the JAR file using {@link #loadFromJar()}. If
   * either method succeeds, the `isLoaded` flag is set to true. If both methods fail, an {@link
   * InternalError} is thrown indicating that the necessary native library could not be loaded.
   *
   * @throws InternalError if the native library cannot be loaded from either the system path or the
   *     JAR file.
   */
  private static void load() {
    new SingletonHolder();
  }

  // "lazy initialization holder class idiom" (Effective Java #83)
  private static class SingletonHolder {
    static {
      if (!loadFromSystemPath() && !loadFromJar()) {
        throw new InternalError("Unable to load necessary native library");
      }
    }
  }

  /**
   * Load the native library from the system path.
   *
   * <p>This method attempts to load the native library named "_turso_java" from the system's
   * library path. If the library is successfully loaded, the `isLoaded` flag is set to true.
   *
   * @return true if the library was successfully loaded, false otherwise.
   */
  private static boolean loadFromSystemPath() {
    try {
      System.loadLibrary("_turso_java");
      return true;
    } catch (Throwable t) {
      logger.info("Unable to load from default path: {}", String.valueOf(t));
    }

    return false;
  }

  /**
   * Load the native library from the JAR file.
   *
   * <p>By default, native libraries are packaged within the JAR file. This method extracts the
   * appropriate native library for the current operating system and architecture from the JAR and
   * loads it.
   *
   * @return true if the library was successfully loaded, false otherwise.
   */
  private static boolean loadFromJar() {
    Architecture arch = Architecture.detect();
    if (arch == Architecture.UNSUPPORTED) {
      logger.info("Unsupported OS or architecture");
      return false;
    }

    try {
      InputStream is = TursoDB.class.getClassLoader().getResourceAsStream(arch.getLibPath());
      assert is != null;
      File file = convertInputStreamToFile(is, arch);
      System.load(file.getPath());
      return true;
    } catch (Throwable t) {
      logger.info("Unable to load from jar: {}", String.valueOf(t));
    }

    return false;
  }

  private static File convertInputStreamToFile(InputStream is, Architecture arch)
      throws IOException {
    File tempFile = File.createTempFile("lib", arch.getFileExtension());
    tempFile.deleteOnExit();

    try (FileOutputStream os = new FileOutputStream(tempFile)) {
      int read;
      byte[] bytes = new byte[1024];

      while ((read = is.read(bytes)) != -1) {
        os.write(bytes, 0, read);
      }
    }

    return tempFile;
  }

  /**
   * @param url eTurso.gTursoTurso. "jdbc:turso:fileName
   * @param filePath e.g. path to file
   */
  public static TursoDB create(String url, String filePath) throws SQLException {
    return new TursoDB(url, filePath, null, null);
  }

  /**
   * Creates a TursoDB instance with encryption support.
   *
   * @param url e.g. "jdbc:turso:fileName"
   * @param filePath e.g. path to file
   * @param cipher the encryption cipher to use
   * @param hexkey the hex-encoded encryption key
   */
  public static TursoDB createWithEncryption(
      String url, String filePath, TursoEncryptionCipher cipher, String hexkey) throws SQLException {
    return new TursoDB(url, filePath, cipher, hexkey);
  }

  // TODO: receive config as argument
  private TursoDB(String url, String filePath, @Nullable TursoEncryptionCipher cipher, @Nullable String hexkey) throws SQLException {
    this.url = url;
    this.filePath = filePath;
    load();
    open(0, cipher != null ? cipher.getValue() : null, hexkey);
  }

  // TODO: add support for JNI
  public native void interrupt();

  public boolean isClosed() {
    return !this.isOpen;
  }

  public boolean isOpen() {
    return this.isOpen;
  }

  private void open(int openFlags, @Nullable String cipher, @Nullable String hexkey) throws SQLException {
    open0(filePath, openFlags, cipher, hexkey);
  }

  private void open0(String filePath, int openFlags, @Nullable String cipher, @Nullable String hexkey)
      throws SQLException {
    byte[] filePathBytes = stringToUtf8ByteArray(filePath);
    if (filePathBytes == null) {
      throw TursoExceptionUtils.buildTursoException(
          TursoErrorCode.TURSO_ETC.code,
          "File path cannot be converted to byteArray. File name: " + filePath);
    }

    if (cipher != null && hexkey != null) {
      dbPointer = openWithEncryptionUtf8(filePathBytes, openFlags, cipher, hexkey);
    } else {
      dbPointer = openUtf8(filePathBytes, openFlags);
    }
    isOpen = true;
  }

  private native long openUtf8(byte[] file, int openFlags) throws SQLException;

  private native long openWithEncryptionUtf8(byte[] file, int openFlags, String cipher, String hexkey)
      throws SQLException;

  public long connect() throws SQLException {
    return connect0(dbPointer);
  }

  private native long connect0(long databasePtr) throws SQLException;

  @Override
  public void close() throws Exception {
    if (!isOpen) return;

    close0(dbPointer);
    isOpen = false;
  }

  private native void close0(long databasePtr) throws SQLException;

  @VisibleForTesting
  native void throwJavaException(int errorCode) throws SQLException;

  /**
   * Throws formatted SQLException with error code and message.
   *
   * @param errorCode Error code.
   * @param errorMessageBytes Error message.
   */
  @NativeInvocation(invokedFrom = "turso_db.rs")
  private void throwTursoException(int errorCode, byte[] errorMessageBytes) throws SQLException {
    TursoExceptionUtils.throwTursoException(errorCode, errorMessageBytes);
  }
}
