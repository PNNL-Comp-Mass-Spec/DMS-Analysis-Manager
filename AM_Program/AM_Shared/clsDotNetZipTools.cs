
using System;
using System.Collections.Generic;
using System.IO;
using Ionic.Zip;
using PRISM;

namespace AnalysisManagerBase
{
    /// <summary>
    /// DotNet Zip Tools (aka Ionic Zip Tools)
    /// </summary>
    public class clsDotNetZipTools : clsEventNotifier
    {
        /// <summary>
        /// DotNetZip name (used for logging)
        /// </summary>
        public const string DOTNET_ZIP_NAME = "DotNetZip";

        /// <summary>
        /// Ionic zip name (used for logging)
        /// </summary>
        [Obsolete("Use DOTNET_ZIP_NAME")]
        public const string IONIC_ZIP_NAME = "IonicZip (DotNetZip)";

        private readonly string m_WorkDir;

        #region "Properties"

        /// <summary>
        /// Debug level
        /// </summary>
        public int DebugLevel { get; set; }

        /// <summary>
        /// Status message
        /// </summary>
        /// <remarks>Tracks the most recent status, warning, or error message</remarks>
        public string Message { get; private set; } = string.Empty;

        /// <summary>
        /// Path to the zip file created most recently
        /// </summary>
        public string MostRecentZipFilePath { get; private set; } = string.Empty;

        /// <summary>
        /// Returns the files most recently unzipped
        /// Keys in the KeyValuePairs are filenames while values are relative paths (in case the .zip file has folders)
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public List<KeyValuePair<string, string>> MostRecentUnzippedFiles { get; } = new List<KeyValuePair<string, string>>();

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="debugLevel"></param>
        /// <param name="workDir"></param>
        public clsDotNetZipTools(int debugLevel, string workDir)
        {
            DebugLevel = debugLevel;
            m_WorkDir = workDir;
        }

        private void DeleteFile(FileSystemInfo fiFile)
        {

            try
            {
                if (DebugLevel >= 3)
                {
                    OnStatusEvent("Deleting source file: " + fiFile.FullName);
                }

                fiFile.Refresh();

                if (fiFile.Exists)
                {
                    // Now delete the source file
                    fiFile.Delete();
                }
            }
            catch (Exception ex)
            {
                // Log this as an error, but don't treat this as fatal
                LogError("Error deleting " + fiFile.FullName, ex);
            }

        }

        /// <summary>
        /// Gets the .zip file path to create when zipping a single file
        /// </summary>
        /// <param name="sourceFilePath"></param>
        /// <returns></returns>
        public static string GetZipFilePathForFile(string sourceFilePath)
        {
            var fiFile = new FileInfo(sourceFilePath);

            if (fiFile.DirectoryName == null)
                return Path.GetFileNameWithoutExtension(fiFile.Name) + ".zip";

            return Path.Combine(fiFile.DirectoryName, Path.GetFileNameWithoutExtension(fiFile.Name) + ".zip");
        }

        /// <summary>
        /// Unzip gzipFilePath into the working directory defined when this class was instantiated
        /// Existing files will be overwritten
        /// </summary>
        /// <param name="gzipFilePath">.gz file to unzip</param>
        /// <returns>True if success; false if an error</returns>
        public bool GUnzipFile(string gzipFilePath)
        {
            return GUnzipFile(gzipFilePath, m_WorkDir);
        }

        /// <summary>
        /// Unzip gzipFilePath into the specified target directory
        /// Existing files will be overwritten
        /// </summary>
        /// <param name="gzipFilePath">.gz file to unzip</param>
        /// <param name="targetDirectory">Folder to place the unzipped files</param>
        /// <returns>True if success; false if an error</returns>
        public bool GUnzipFile(string gzipFilePath, string targetDirectory)
        {
            return GUnzipFile(gzipFilePath, targetDirectory, Ionic.Zip.ExtractExistingFileAction.OverwriteSilently);
        }

        /// <summary>
        /// Unzip gzipFilePath into the specified target directory
        /// </summary>
        /// <param name="gzipFilePath">.gz file to unzip</param>
        /// <param name="targetDirectory">Folder to place the unzipped files</param>
        /// <param name="overwriteBehavior">Defines what to do when existing files could be ovewritten</param>
        /// <returns>True if success; false if an error</returns>
        public bool GUnzipFile(string gzipFilePath, string targetDirectory, Ionic.Zip.ExtractExistingFileAction overwriteBehavior)
        {

            MostRecentZipFilePath = string.Copy(gzipFilePath);
            MostRecentUnzippedFiles.Clear();

            try
            {
                var fileToGUnzip = new FileInfo(gzipFilePath);

                if (!fileToGUnzip.Exists)
                {
                    LogError("GZip file not found: " + fileToGUnzip.FullName);
                    return false;
                }

                if (fileToGUnzip.Extension.ToLower() != ".gz")
                {
                    LogError("Not a GZipped file; must have extension .gz: " + fileToGUnzip.FullName);
                    return false;
                }

                if (DebugLevel >= 3)
                {
                    OnStatusEvent("Unzipping file: " + fileToGUnzip.FullName);
                }

                var dtStartTime = DateTime.UtcNow;

                // Get original file extension, for example "doc" from report.doc.gz
                var curFile = fileToGUnzip.Name;
                var decompressedFilePath = Path.Combine(targetDirectory, curFile.Remove(curFile.Length - fileToGUnzip.Extension.Length));

                var decompressedFile = new FileInfo(decompressedFilePath);

                if (decompressedFile.Exists)
                {
                    if (overwriteBehavior == ExtractExistingFileAction.DoNotOverwrite)
                    {
                        Message = "Decompressed file already exists; will not overwrite: " + decompressedFile.FullName;
                        OnStatusEvent(Message);
                        return true;
                    }

                    if (overwriteBehavior == ExtractExistingFileAction.Throw)
                    {
                        throw new Exception("Decompressed file already exists: " + decompressedFile.FullName);
                    }
                }
                else
                {
                    // Make sure the target directory exists
                    if (decompressedFile.Directory != null && !decompressedFile.Directory.Exists)
                        decompressedFile.Directory.Create();
                }

                var actualDecompressedFilePath = clsFileTools.GZipDecompressWithMetadata(fileToGUnzip, decompressedFile.DirectoryName);
                var actualDecompressedFile = new FileInfo(actualDecompressedFilePath);

                if (!string.Equals(decompressedFile.FullName, actualDecompressedFile.FullName))
                {
                    OnWarningEvent(string.Format("GZipDecompressWithMetadata created a different file than expected; {0} vs. expected {1}",
                                   actualDecompressedFile.FullName, decompressedFile.FullName));
                }

                MostRecentUnzippedFiles.Add(new KeyValuePair<string, string>(actualDecompressedFile.Name, actualDecompressedFile.FullName));

                var dtEndTime = DateTime.UtcNow;

                if (DebugLevel >= 2)
                {
                    ReportZipStats(fileToGUnzip, dtStartTime, dtEndTime, false, "GZipStream");
                }

                // Update the file modification time of the decompressed file if the date is newer than the date of the original .gz file
                if (actualDecompressedFile.LastWriteTimeUtc > fileToGUnzip.LastWriteTimeUtc)
                {
                    actualDecompressedFile.LastWriteTimeUtc = fileToGUnzip.LastWriteTimeUtc;
                }

            }
            catch (Exception ex)
            {
                LogError("Error unzipping .gz file " + gzipFilePath, ex);
                return false;
            }

            return true;

        }

        /// <summary>
        /// Stores sourceFilePath in a GZip file with the same name, but with extension .gz appended to the name (e.g. Dataset.mzid.gz)
        /// </summary>
        /// <param name="sourceFilePath">Full path to the file to be zipped</param>
        /// <param name="deleteSourceAfterZip">If True, will delete the source file after zipping it</param>
        /// <returns>True if success; false if an error</returns>
        public bool GZipFile(string sourceFilePath, bool deleteSourceAfterZip)
        {
            var fiFile = new FileInfo(sourceFilePath);
            return GZipFile(sourceFilePath, fiFile.DirectoryName, deleteSourceAfterZip);
        }

        /// <summary>
        /// Stores sourceFilePath in a GZip file with the same name, but with extension .gz appended to the name (e.g. Dataset.mzid.gz)
        /// </summary>
        /// <param name="sourceFilePath">Full path to the file to be zipped</param>
        /// <param name="targetFolderPath">Target directory to create the .gz file</param>
        /// <param name="deleteSourceAfterZip">If True, will delete the source file after zipping it</param>
        /// <returns>True if success; false if an error</returns>
        public bool GZipFile(string sourceFilePath, string targetFolderPath, bool deleteSourceAfterZip)
        {

            var fiFile = new FileInfo(sourceFilePath);

            var gzipFilePath = Path.Combine(targetFolderPath, fiFile.Name + ".gz");

            Message = string.Empty;
            MostRecentZipFilePath = string.Copy(gzipFilePath);

            try
            {

                if (File.Exists(gzipFilePath))
                {
                    if (DebugLevel >= 3)
                    {
                        OnStatusEvent("Deleting target .gz file: " + gzipFilePath);
                    }

                    File.Delete(gzipFilePath);
                }
            }
            catch (Exception ex)
            {
                LogError("Error deleting target .gz file prior to zipping " + sourceFilePath, ex);
                return false;
            }

            var success = GZipUsingGZipStream(fiFile, gzipFilePath);

            if (!success)
            {
                return false;
            }

            if (deleteSourceAfterZip)
            {
                DeleteFile(fiFile);
            }

            return true;

        }

        /// <summary>
        /// Compress the file using GZipStream (as implemented in PRISM.dll)
        /// </summary>
        /// <param name="fileToGZip">File to compress</param>
        /// <param name="gzipFilePath"></param>
        /// <returns></returns>
        /// <remarks>The .gz file created by PRISM.dll will include header information (filename and timestamp of the original file)</remarks>
        private bool GZipUsingGZipStream(FileInfo fileToGZip, string gzipFilePath)
        {
            try
            {
                if (DebugLevel >= 3)
                {
                    OnStatusEvent("Creating .gz file using GZipStream: " + gzipFilePath);
                }

                var dtStartTime = DateTime.UtcNow;

                var gzipFile = new FileInfo(gzipFilePath);

                clsFileTools.GZipCompressWithMetadata(fileToGZip, gzipFile.DirectoryName, gzipFile.Name);

                var dtEndTime = DateTime.UtcNow;

                if (DebugLevel >= 2)
                {
                    ReportZipStats(fileToGZip, dtStartTime, dtEndTime, true, "GZipStream");
                }

                // Update the file modification time of the .gz file to use the modification time of the original file
                var fiGZippedFile = new FileInfo(gzipFilePath);

                if (!fiGZippedFile.Exists)
                {
                    LogError("GZipStream did not create a .gz file: " + gzipFilePath);
                    return false;
                }

                fiGZippedFile.LastWriteTimeUtc = fileToGZip.LastWriteTimeUtc;

            }
            catch (Exception ex)
            {
                LogError("Error gzipping file " + fileToGZip.FullName, ex);
                return false;
            }

            return true;

        }

        private void LogError(string errorMessage)
        {
            Message = errorMessage;
            OnErrorEvent(errorMessage);
        }

        private void LogError(string errorMessage, Exception ex)
        {
            Message = errorMessage;
            OnErrorEvent(errorMessage, ex);
        }

        /// <summary>
        /// Update Message with stats on the most recent zip file created
        /// </summary>
        /// <param name="fileOrFolderZippedOrUnzipped"></param>
        /// <param name="dtStartTime"></param>
        /// <param name="dtEndTime"></param>
        /// <param name="fileWasZipped"></param>
        /// <param name="zipProgramName"></param>
        /// <remarks>If DebugLevel is 2 or larger, also raises event StatusEvent</remarks>
        private void ReportZipStats(
            FileSystemInfo fileOrFolderZippedOrUnzipped,
            DateTime dtStartTime,
            DateTime dtEndTime,
            bool fileWasZipped,
            string zipProgramName)
        {

            long totalSizeBytes = 0;
            double unzipSpeedMBPerSec;

            if (zipProgramName == null)
                zipProgramName = "??";

            var unzipTimeSeconds = dtEndTime.Subtract(dtStartTime).TotalSeconds;

            if (fileOrFolderZippedOrUnzipped is FileInfo processedFile)
            {
                totalSizeBytes = processedFile.Length;

            }
            else if (fileOrFolderZippedOrUnzipped is DirectoryInfo processedDirectory)
            {
                totalSizeBytes = 0;
                foreach (var item in processedDirectory.GetFiles("*", SearchOption.AllDirectories))
                {
                    totalSizeBytes += item.Length;
                }
            }

            if (unzipTimeSeconds > 0)
            {
                unzipSpeedMBPerSec = clsGlobal.BytesToMB(totalSizeBytes) / unzipTimeSeconds;
            }
            else
            {
                unzipSpeedMBPerSec = 0;
            }

            string zipAction;
            if (fileWasZipped)
            {
                zipAction = "Zipped ";
            }
            else
            {
                zipAction = "Unzipped ";
            }

            Message = zipAction + fileOrFolderZippedOrUnzipped.Name + " using " + zipProgramName + "; " +
                "elapsed time = " + unzipTimeSeconds.ToString("0.0") + " seconds; " +
                "rate = " + unzipSpeedMBPerSec.ToString("0.0") + " MB/sec";

            if (DebugLevel >= 2)
            {
                OnStatusEvent(Message);
            }

        }

        /// <summary>
        /// Unzip zipFilePath into the working directory defined when this class was instantiated
        /// Existing files will be overwritten
        /// </summary>
        /// <param name="zipFilePath">File to unzip</param>
        /// <returns>True if success; false if an error</returns>
        public bool UnzipFile(string zipFilePath)
        {
            return UnzipFile(zipFilePath, m_WorkDir);
        }

        /// <summary>
        /// Unzip zipFilePath into the specified target directory
        /// Existing files will be overwritten
        /// </summary>
        /// <param name="zipFilePath">File to unzip</param>
        /// <param name="targetDirectory">Folder to place the unzipped files</param>
        /// <returns>True if success; false if an error</returns>
        public bool UnzipFile(string zipFilePath, string targetDirectory)
        {
            return UnzipFile(zipFilePath, targetDirectory, string.Empty, Ionic.Zip.ExtractExistingFileAction.OverwriteSilently);
        }

        /// <summary>
        /// Unzip zipFilePath into the specified target directory, applying the specified file filter
        /// Existing files will be overwritten
        /// </summary>
        /// <param name="zipFilePath">File to unzip</param>
        /// <param name="targetDirectory">Folder to place the unzipped files</param>
        /// <param name="fileFilter">Filter to apply when unzipping</param>
        /// <returns>True if success; false if an error</returns>
        public bool UnzipFile(string zipFilePath, string targetDirectory, string fileFilter)
        {

            return UnzipFile(zipFilePath, targetDirectory, fileFilter, Ionic.Zip.ExtractExistingFileAction.OverwriteSilently);
        }


        /// <summary>
        /// Unzip zipFilePath into the specified target directory, applying the specified file filter
        /// </summary>
        /// <param name="zipFilePath">File to unzip</param>
        /// <param name="targetDirectory">Folder to place the unzipped files</param>
        /// <param name="fileFilter">Filter to apply when unzipping</param>
        /// <param name="overwriteBehavior">Defines what to do when existing files could be ovewritten</param>
        /// <returns>True if success; false if an error</returns>
        public bool UnzipFile(string zipFilePath, string targetDirectory, string fileFilter, Ionic.Zip.ExtractExistingFileAction overwriteBehavior)
        {

            Message = string.Empty;
            MostRecentZipFilePath = string.Copy(zipFilePath);
            MostRecentUnzippedFiles.Clear();

            try
            {
                var fileToUnzip = new FileInfo(zipFilePath);

                if (!File.Exists(zipFilePath))
                {
                    LogError("Zip file not found: " + fileToUnzip.FullName);
                    return false;
                }

                if (DebugLevel >= 3)
                {
                    OnStatusEvent("Unzipping file: " + fileToUnzip.FullName);
                }

                // Ionic.Zip.ZipFile
                using (var zipper = new ZipFile(zipFilePath))
                {

                    var dtStartTime = DateTime.UtcNow;

                    if (string.IsNullOrEmpty(fileFilter))
                    {
                        zipper.ExtractAll(targetDirectory, overwriteBehavior);

                        foreach (var item in zipper.Entries)
                        {
                            if (!item.IsDirectory)
                            {
                                // Note that objItem.FileName contains the relative path of the file, for example "Filename.txt" or "Subfolder/Filename.txt"
                                var fiUnzippedItem = new FileInfo(Path.Combine(targetDirectory, item.FileName.Replace('/', Path.DirectorySeparatorChar)));
                                MostRecentUnzippedFiles.Add(new KeyValuePair<string, string>(fiUnzippedItem.Name, fiUnzippedItem.FullName));
                            }
                        }
                    }
                    else
                    {
                        var zipEntries = zipper.SelectEntries(fileFilter);

                        foreach (var item in zipEntries)
                        {
                            item.Extract(targetDirectory, overwriteBehavior);
                            if (!item.IsDirectory)
                            {
                                // Note that objItem.FileName contains the relative path of the file, for example "Filename.txt" or "Subfolder/Filename.txt"
                                var fiUnzippedItem = new FileInfo(Path.Combine(targetDirectory, item.FileName.Replace('/', Path.DirectorySeparatorChar)));
                                MostRecentUnzippedFiles.Add(new KeyValuePair<string, string>(fiUnzippedItem.Name, fiUnzippedItem.FullName));
                            }
                        }
                    }

                    var dtEndTime = DateTime.UtcNow;

                    if (DebugLevel >= 2)
                    {
                        ReportZipStats(fileToUnzip, dtStartTime, dtEndTime, false, DOTNET_ZIP_NAME);
                    }

                }

            }
            catch (Exception ex)
            {
                LogError("Error unzipping file " + zipFilePath, ex);
                return false;
            }

            return true;

        }

        /// <summary>
        /// Verifies that the zip file exists and is not corrupt
        /// If the file size is less than 4 GB, also performs a full CRC check of the data
        /// </summary>
        /// <param name="zipFilePath">Zip file to check</param>
        /// <returns>True if a valid zip file, otherwise false</returns>
        public bool VerifyZipFile(string zipFilePath)
        {
            return VerifyZipFile(zipFilePath, crcCheckThresholdGB: 4);
        }

        /// <summary>
        /// Verifies that the zip file exists.
        /// If the file size is less than crcCheckThresholdGB, also performs a full CRC check of the data
        /// </summary>
        /// <param name="zipFilePath">Zip file to check</param>
        /// <param name="crcCheckThresholdGB">Threshold (in GB) below which a full CRC check should be performed</param>
        /// <returns>True if a valid zip file, otherwise false</returns>
        public bool VerifyZipFile(string zipFilePath, float crcCheckThresholdGB)
        {

            try
            {

                // Confirm that the zip file was created
                var fiZipFile = new FileInfo(zipFilePath);
                if (!fiZipFile.Exists)
                {
                    LogError("Zip file not found: " + zipFilePath);
                    return false;
                }

                // Perform a quick check of the zip file (simply iterates over the directory entries)
                var blnsuccess = Ionic.Zip.ZipFile.CheckZip(zipFilePath);

                if (!blnsuccess)
                {
                    if (string.IsNullOrEmpty(Message))
                    {
                        LogError("Zip quick check failed for " + zipFilePath);
                    }
                    return false;
                }

                // For zip files less than 4 GB in size, perform a full unzip test to confirm that the file is not corrupted
                var crcCheckThresholdBytes = (long)(crcCheckThresholdGB * 1024 * 1024 * 1024);

                if (fiZipFile.Length > crcCheckThresholdBytes)
                {
                    // File is too big; do not verify it
                    return true;
                }


                // Unzip each zipped file to a byte buffer (no need to actually write to disk)
                // Ionic.Zip.ZipFile
                using (var zipper = new ZipFile(zipFilePath))
                {

                    var entries = zipper.SelectEntries("*");

                    foreach (var entry in entries)
                    {
                        if (entry.IsDirectory)
                            continue;

                        var success = VerifyZipFileEntry(zipFilePath, entry);
                        if (!success)
                            return false;
                    }

                }


            }
            catch (Exception ex)
            {
                LogError("Error verifying zip file " + zipFilePath, ex);
                return false;
            }

            return true;

        }

        private bool VerifyZipFileEntry(string zipFilePath, ZipEntry entry)
        {
            var bytBuffer = new byte[8096];
            long totalBytesRead = 0;

            using (var srReader = entry.OpenReader())
            {
                int n;
                do
                {
                    n = srReader.Read(bytBuffer, 0, bytBuffer.Length);
                    totalBytesRead += n;
                } while (n > 0);

                if (srReader.Crc != entry.Crc)
                {
                    Message = string.Format("Zip entry " + entry.FileName + " failed the CRC Check in " + zipFilePath +
                                              " (0x{0:X8} != 0x{1:X8})", srReader.Crc, entry.Crc);
                    OnErrorEvent(Message);
                    return false;
                }

                if (totalBytesRead != entry.UncompressedSize)
                {
                    Message = string.Format("Unexpected number of bytes for entry " + entry.FileName + " in " + zipFilePath +
                                              " ({0} != {1})", totalBytesRead, entry.UncompressedSize);
                    OnWarningEvent(Message);
                    return false;
                }

            }

            return true;
        }

        /// <summary>
        /// Stores sourceFilePath in a zip file with the same name, but extension .zip
        /// </summary>
        /// <param name="sourceFilePath">Full path to the file to be zipped</param>
        /// <param name="deleteSourceAfterZip">If True, will delete the source file after zipping it</param>
        /// <returns>True if success; false if an error</returns>
        public bool ZipFile(string sourceFilePath, bool deleteSourceAfterZip)
        {

            var zipFilePath = GetZipFilePathForFile(sourceFilePath);

            return ZipFile(sourceFilePath, deleteSourceAfterZip, zipFilePath);

        }

        /// <summary>
        /// Stores sourceFilePath in a zip file named zipFilePath
        /// </summary>
        /// <param name="sourceFilePath">Full path to the file to be zipped</param>
        /// <param name="deleteSourceAfterZip">If True, will delete the source file after zipping it</param>
        /// <param name="zipFilePath">Full path to the .zip file to be created.  Existing files will be overwritten</param>
        /// <returns>True if success; false if an error</returns>
        public bool ZipFile(string sourceFilePath, bool deleteSourceAfterZip, string zipFilePath)
        {

            var fileToZip = new FileInfo(sourceFilePath);

            Message = string.Empty;
            MostRecentZipFilePath = string.Copy(zipFilePath);

            try
            {

                if (File.Exists(zipFilePath))
                {
                    if (DebugLevel >= 3)
                    {
                        OnStatusEvent("Deleting target .zip file: " + zipFilePath);
                    }

                    File.Delete(zipFilePath);
                }
            }
            catch (Exception ex)
            {
                LogError("Error deleting target .zip file prior to zipping file " + sourceFilePath + " using " + DOTNET_ZIP_NAME, ex);
                return false;
            }

            try
            {
                if (DebugLevel >= 3)
                {
                    OnStatusEvent("Creating .zip file: " + zipFilePath);
                }

                // Ionic.Zip.ZipFile
                using (var zipper = new ZipFile(zipFilePath))
                {
                    zipper.UseZip64WhenSaving = Zip64Option.AsNecessary;

                    var dtStartTime = DateTime.UtcNow;
                    zipper.AddItem(fileToZip.FullName, string.Empty);
                    zipper.Save();
                    var dtEndTime = DateTime.UtcNow;

                    if (DebugLevel >= 2)
                    {
                        ReportZipStats(fileToZip, dtStartTime, dtEndTime, true, DOTNET_ZIP_NAME);
                    }

                }

            }
            catch (Exception ex)
            {
                LogError("Error zipping file " + fileToZip.FullName, ex);
                return false;
            }

            // Verify that the zip file is not corrupt
            // Files less than 4 GB get a full CRC check
            // Large files get a quick check
            if (!VerifyZipFile(zipFilePath))
            {
                return false;
            }

            if (deleteSourceAfterZip)
            {
                DeleteFile(fileToZip);
            }

            return true;

        }

        /// <summary>
        /// Zip all files in a directory
        /// </summary>
        /// <param name="sourceDirectoryPath"></param>
        /// <param name="zipFilePath"></param>
        /// <returns></returns>
        public bool ZipDirectory(string sourceDirectoryPath, string zipFilePath)
        {

            return ZipDirectory(sourceDirectoryPath, zipFilePath, true, string.Empty);

        }

        /// <summary>
        /// Zip all files in a directory
        /// </summary>
        /// <param name="sourceDirectoryPath"></param>
        /// <param name="zipFilePath"></param>
        /// <param name="recurse"></param>
        /// <returns></returns>
        public bool ZipDirectory(string sourceDirectoryPath, string zipFilePath, bool recurse)
        {

            return ZipDirectory(sourceDirectoryPath, zipFilePath, recurse, string.Empty);

        }

        /// <summary>
        /// Stores all files in a source directory into a zip file named zipFilePath
        /// </summary>
        /// <param name="sourceDirectoryPath">Full path to the directory to be zipped</param>
        /// <param name="zipFilePath">Full path to the .zip file to be created.  Existing files will be overwritten</param>
        /// <param name="recurse">If True, recurse through all subfolders</param>
        /// <param name="fileFilter">Filter to apply when zipping</param>
        /// <returns>True if success; false if an error</returns>
        public bool ZipDirectory(string sourceDirectoryPath, string zipFilePath, bool recurse, string fileFilter)
        {

            var directoryToZip = new DirectoryInfo(sourceDirectoryPath);

            Message = string.Empty;
            MostRecentZipFilePath = string.Copy(zipFilePath);

            try
            {
                if (File.Exists(zipFilePath))
                {
                    if (DebugLevel >= 3)
                    {
                        OnStatusEvent("Deleting target .zip file: " + zipFilePath);
                    }

                    File.Delete(zipFilePath);
                }
            }
            catch (Exception ex)
            {
                LogError("Error deleting target .zip file prior to zipping folder " + sourceDirectoryPath + " using " + DOTNET_ZIP_NAME, ex);
                return false;
            }

            try
            {
                if (DebugLevel >= 3)
                {
                    OnStatusEvent("Creating .zip file: " + zipFilePath);
                }

                // Ionic.Zip.ZipFile
                using (var zipper = new ZipFile(zipFilePath))
                {
                    zipper.UseZip64WhenSaving = Zip64Option.AsNecessary;

                    var dtStartTime = DateTime.UtcNow;

                    if (string.IsNullOrEmpty(fileFilter) && recurse)
                    {
                        zipper.AddDirectory(directoryToZip.FullName);
                    }
                    else
                    {
                        if (string.IsNullOrEmpty(fileFilter))
                        {
                            fileFilter = "*";
                        }

                        zipper.AddSelectedFiles(fileFilter, directoryToZip.FullName, string.Empty, recurse);
                    }

                    zipper.Save();

                    var dtEndTime = DateTime.UtcNow;

                    if (DebugLevel >= 2)
                    {
                        ReportZipStats(directoryToZip, dtStartTime, dtEndTime, true, DOTNET_ZIP_NAME);
                    }

                }

            }
            catch (Exception ex)
            {
                LogError("Error zipping directory " + directoryToZip.FullName, ex);
                return false;
            }

            // Verify that the zip file is not corrupt
            // Files less than 4 GB get a full CRC check
            // Large files get a quick check
            if (!VerifyZipFile(zipFilePath))
            {
                return false;
            }

            return true;

        }

    }

}