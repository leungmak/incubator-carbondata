/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.core.datastore.filesystem;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.util.zip.GZIPInputStream;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonUtil;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

public class LocalCarbonFile implements CarbonFile {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(LocalCarbonFile.class.getName());
  private File file;

  public LocalCarbonFile(String filePath) {
    Path pathWithoutSchemeAndAuthority = Path.getPathWithoutSchemeAndAuthority(new Path(filePath));
    file = new File(pathWithoutSchemeAndAuthority.toString());
  }

  public LocalCarbonFile(File file) {
    this.file = file;
  }

  @Override public String getAbsolutePath() {
    return file.getAbsolutePath();
  }

  @Override public CarbonFile[] listFiles(final CarbonFileFilter fileFilter) {
    if (!file.isDirectory()) {
      return new CarbonFile[0];
    }

    File[] files = file.listFiles(new FileFilter() {

      @Override public boolean accept(File pathname) {
        return fileFilter.accept(new LocalCarbonFile(pathname));
      }
    });

    if (files == null) {
      return new CarbonFile[0];
    }

    CarbonFile[] carbonFiles = new CarbonFile[files.length];

    for (int i = 0; i < carbonFiles.length; i++) {
      carbonFiles[i] = new LocalCarbonFile(files[i]);
    }

    return carbonFiles;
  }

  @Override public String getName() {
    return file.getName();
  }

  @Override public boolean isDirectory() {
    return file.isDirectory();
  }

  @Override public boolean exists() {
    if (file != null) {
      return file.exists();
    }
    return false;
  }

  @Override public String getCanonicalPath() {
    try {
      return file.getCanonicalPath();
    } catch (IOException e) {
      LOGGER
          .error(e, "Exception occured" + e.getMessage());
    }
    return null;
  }

  @Override public CarbonFile getParentFile() {
    return new LocalCarbonFile(file.getParentFile());
  }

  @Override public String getPath() {
    return file.getPath();
  }

  @Override public long getSize() {
    return file.length();
  }

  public boolean renameTo(String changetoName) {
    return file.renameTo(new File(changetoName));
  }

  public boolean delete() {
    return file.delete();
  }

  @Override public CarbonFile[] listFiles() {

    if (!file.isDirectory()) {
      return new CarbonFile[0];
    }
    File[] files = file.listFiles();
    if (files == null) {
      return new CarbonFile[0];
    }
    CarbonFile[] carbonFiles = new CarbonFile[files.length];
    for (int i = 0; i < carbonFiles.length; i++) {
      carbonFiles[i] = new LocalCarbonFile(files[i]);
    }

    return carbonFiles;

  }

  @Override public boolean createNewFile() {
    try {
      return file.createNewFile();
    } catch (IOException e) {
      return false;
    }
  }

  @Override public long getLastModifiedTime() {
    return file.lastModified();
  }

  @Override public boolean setLastModifiedTime(long timestamp) {
    return file.setLastModified(timestamp);
  }

  /**
   * This method will delete the data in file data from a given offset
   */
  @Override public boolean truncate(String fileName, long validDataEndOffset) {
    FileChannel source = null;
    FileChannel destination = null;
    boolean fileTruncatedSuccessfully = false;
    // temporary file name
    String tempWriteFilePath = fileName + CarbonCommonConstants.TEMPWRITEFILEEXTENSION;
    FileFactory.FileType fileType = FileFactory.getFileType(fileName);
    try {
      CarbonFile tempFile = null;
      // delete temporary file if it already exists at a given path
      if (FileFactory.isFileExist(tempWriteFilePath, fileType)) {
        tempFile = FileFactory.getCarbonFile(tempWriteFilePath, fileType);
        tempFile.delete();
      }
      // create new temporary file
      FileFactory.createNewFile(tempWriteFilePath, fileType);
      tempFile = FileFactory.getCarbonFile(tempWriteFilePath, fileType);
      source = new FileInputStream(fileName).getChannel();
      destination = new FileOutputStream(tempWriteFilePath).getChannel();
      long read = destination.transferFrom(source, 0, validDataEndOffset);
      long totalBytesRead = read;
      long remaining = validDataEndOffset - totalBytesRead;
      // read till required data offset is not reached
      while (remaining > 0) {
        read = destination.transferFrom(source, totalBytesRead, remaining);
        totalBytesRead = totalBytesRead + read;
        remaining = remaining - totalBytesRead;
      }
      CarbonUtil.closeStreams(source, destination);
      // rename the temp file to original file
      tempFile.renameForce(fileName);
      fileTruncatedSuccessfully = true;
    } catch (IOException e) {
      LOGGER.error("Exception occured while truncating the file " + e.getMessage());
    } finally {
      CarbonUtil.closeStreams(source, destination);
    }
    return fileTruncatedSuccessfully;
  }

  /**
   * This method will be used to check whether a file has been modified or not
   *
   * @param fileTimeStamp time to be compared with latest timestamp of file
   * @param endOffset     file length to be compared with current length of file
   * @return
   */
  @Override public boolean isFileModified(long fileTimeStamp, long endOffset) {
    boolean isFileModified = false;
    if (getLastModifiedTime() > fileTimeStamp || getSize() > endOffset) {
      isFileModified = true;
    }
    return isFileModified;
  }


  @Override public boolean renameForce(String changetoName) {
    File destFile = new File(changetoName);
    if (destFile.exists()) {
      LOGGER.warn("File already exists: " + destFile + ". Trying to delete it");
      if (destFile.delete()) {
        return file.renameTo(new File(changetoName));
      } else {
        LOGGER.error("Failed to delete file: " + destFile);
        return false;
      }
    }

    return file.renameTo(new File(changetoName));

  }

  /**
   * below method will be used to update the file path
   * for local type
   * it removes the file:/ from the path
   *
   * @param filePath
   * @return updated file path without url for local
   */
  private static String getUpdatedFilePath(String filePath) {
    if (filePath != null && !filePath.isEmpty()) {
      // If the store path is relative then convert to absolute path.
      if (filePath.startsWith("./")) {
        try {
          return new File(filePath).getCanonicalPath();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      } else {
        Path pathWithoutSchemeAndAuthority =
            Path.getPathWithoutSchemeAndAuthority(new Path(filePath));
        return pathWithoutSchemeAndAuthority.toString();
      }
    } else {
      return filePath;
    }
  }

  @Override public DataOutputStream getDataOutputStream(String path, FileFactory.FileType fileType,
      int bufferSize, boolean append) throws FileNotFoundException {
    path = getUpdatedFilePath(path);
    return new DataOutputStream(
        new BufferedOutputStream(new FileOutputStream(path, append), bufferSize));
  }

  @Override public DataInputStream getDataInputStream(String path, FileFactory.FileType fileType,
      int bufferSize, Configuration configuration) throws IOException {
    path = path.replace("\\", "/");
    boolean gzip = path.endsWith(".gz");
    boolean bzip2 = path.endsWith(".bz2");
    InputStream stream;
    path = FileFactory.getUpdatedFilePath(path, fileType);
    if (gzip) {
      stream = new GZIPInputStream(new FileInputStream(path));
    } else if (bzip2) {
      stream = new BZip2CompressorInputStream(new FileInputStream(path));
    } else {
      stream = new FileInputStream(path);
    }
    return new DataInputStream(new BufferedInputStream(stream));
  }

  /**
   * return the datainputStream which is seek to the offset of file
   *
   * @param path
   * @param fileType
   * @param bufferSize
   * @param offset
   * @return DataInputStream
   * @throws IOException
   */
  @Override public DataInputStream getDataInputStream(String path, FileFactory.FileType fileType,
      int bufferSize, long offset) throws IOException {
    path = path.replace("\\", "/");
    path = FileFactory.getUpdatedFilePath(path, fileType);
    FileInputStream fis = new FileInputStream(path);
    long actualSkipSize = 0;
    long skipSize = offset;
    try {
      while (actualSkipSize != offset) {
        actualSkipSize += fis.skip(skipSize);
        skipSize = skipSize - actualSkipSize;
      }
    } catch (IOException ioe) {
      CarbonUtil.closeStream(fis);
      throw ioe;
    }
    return new DataInputStream(new BufferedInputStream(fis));
  }

  @Override public DataOutputStream getDataOutputStream(String path, FileFactory.FileType fileType)
      throws IOException {
    path = path.replace("\\", "/");
    return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path)));
  }

  @Override public DataOutputStream getDataOutputStream(String path, FileFactory.FileType fileType,
      int bufferSize, long blockSize) throws IOException {
    path = path.replace("\\", "/");
    path = FileFactory.getUpdatedFilePath(path, fileType);
    return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path), bufferSize));
  }

  @Override public boolean isFileExist(String filePath, FileFactory.FileType fileType,
      boolean performFileCheck) throws IOException {
    filePath = filePath.replace("\\", "/");
    filePath = FileFactory.getUpdatedFilePath(filePath, fileType);
    File defaultFile = new File(filePath);

    if (performFileCheck) {
      return defaultFile.exists() && defaultFile.isFile();
    } else {
      return defaultFile.exists();
    }
  }

  @Override public boolean isFileExist(String filePath, FileFactory.FileType fileType)
      throws IOException {
    filePath = filePath.replace("\\", "/");
    filePath = FileFactory.getUpdatedFilePath(filePath, fileType);
    File defaultFile = new File(filePath);
    return defaultFile.exists();
  }

  @Override public boolean createNewFile(String filePath, FileFactory.FileType fileType)
      throws IOException {
    filePath = filePath.replace("\\", "/");
    filePath = FileFactory.getUpdatedFilePath(filePath, fileType);
    File file = new File(filePath);
    return file.createNewFile();
  }

  @Override
  public boolean createNewFile(String filePath, FileFactory.FileType fileType, boolean doAs,
      final FsPermission permission) throws IOException {
    filePath = filePath.replace("\\", "/");
    filePath = FileFactory.getUpdatedFilePath(filePath, fileType);
    File file = new File(filePath);
    return file.createNewFile();
  }

  @Override public boolean deleteFile(String filePath, FileFactory.FileType fileType)
      throws IOException {
    filePath = filePath.replace("\\", "/");
    filePath = FileFactory.getUpdatedFilePath(filePath, fileType);
    File file = new File(filePath);
    return FileFactory.deleteAllFilesOfDir(file);
  }

  @Override public boolean mkdirs(String filePath, FileFactory.FileType fileType)
      throws IOException {
    filePath = filePath.replace("\\", "/");
    filePath = FileFactory.getUpdatedFilePath(filePath, fileType);
    File file = new File(filePath);
    return file.mkdirs();
  }

  @Override
  public DataOutputStream getDataOutputStreamUsingAppend(String path, FileFactory.FileType fileType)
      throws IOException {
    path = path.replace("\\", "/");
    path = FileFactory.getUpdatedFilePath(path, fileType);
    return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path, true)));
  }

  @Override public boolean createNewLockFile(String filePath, FileFactory.FileType fileType)
      throws IOException {
    filePath = filePath.replace("\\", "/");
    filePath = FileFactory.getUpdatedFilePath(filePath, fileType);
    File file = new File(filePath);
    return file.createNewFile();
  }

  @Override
  public void setPermission(String directoryPath, FsPermission permission, String username,
      String group) throws IOException {
  }
}
