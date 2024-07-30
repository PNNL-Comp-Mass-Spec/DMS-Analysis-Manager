using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using Microsoft.Extensions.FileSystemGlobbing;
using PRISM;
using PRISM.Logging;

namespace AnalysisManagerBase.FileAndDirectoryTools
{
    /// <summary>
    /// System.IO.Compression.ZipFile Tools
    /// </summary>
    public class ZipFileTools : EventNotifier
    {
        // ReSharper disable once CommentTypo
        // Ignore Spelling: crc, gzip, gzipping

        /// <summary>
        /// System.IO.Compression.ZipFile (used for logging)
        /// </summary>
        public const string SYSTEM_IO_COMPRESSION_ZIP_NAME = "System.IO.Compression.ZipFile";

        /// <summary>
        /// Action to take when extracting files and an existing file is found
        /// </summary>
        public enum ExtractExistingFileBehavior
        {
            /// <summary>
            /// Replace the existing file
            /// </summary>
            OverwriteSilently,

            /// <summary>
            /// Do not replace the existing file
            /// </summary>
            DoNotOverwrite,

            /// <summary>
            /// Throw an exception
            /// </summary>
            Throw
        }

        private readonly string mWorkDir;

        /// <summary>
        /// Debug level: ranges from 0 (minimum output) to 5 (max detail)
        /// </summary>
        /// <remarks>
        /// In this class, ZipStats are reported if DebugLevel is 2 or higher
        /// Status event messages will be shown if DebugLevel is 3 or higher
        /// </remarks>
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
        /// Keys in the KeyValuePairs are filenames while values are full file paths
        /// </summary>
        public List<KeyValuePair<string, string>> MostRecentUnzippedFiles { get; } = new();

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="debugLevel"></param>
        /// <param name="workDir"></param>
        public ZipFileTools(int debugLevel, string workDir)
        {
            DebugLevel = debugLevel;
            mWorkDir = workDir;
        }

        /// <summary>
        /// Add a file to an existing .zip file
        /// </summary>
        /// <param name="zipFilePath"></param>
        /// <param name="fileToAdd"></param>
        /// <returns>True if successful, false if an error</returns>
        public bool AddToZipFile(string zipFilePath, FileInfo fileToAdd)
        {
            try
            {
                if (DebugLevel >= 3)
                {
                    OnStatusEvent("Adding {0} to .zip file {1}", fileToAdd.Name, zipFilePath);
                }

                using var zipFile = System.IO.Compression.ZipFile.Open(zipFilePath, ZipArchiveMode.Create);

                zipFile.CreateEntryFromFile(fileToAdd.FullName, fileToAdd.Name);

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error adding file to existing zip file " + zipFilePath, ex);
                return false;
            }
        }

        private void DeleteFile(FileSystemInfo targetFile)
        {
            try
            {
                if (DebugLevel >= 3)
                {
                    OnStatusEvent("Deleting file: " + targetFile.FullName);
                }

                targetFile.Refresh();

                if (targetFile.Exists)
                {
                    targetFile.Delete();
                }
            }
            catch (Exception ex)
            {
                // Log this as an error, but don't treat this as fatal
                LogError("Error deleting " + targetFile.FullName, ex);
            }
        }

        /// <summary>
        /// Compare two strings and return the characters that are in common from the start of each string
        /// </summary>
        /// <param name="item1">First string</param>
        /// <param name="item2">Second string</param>
        /// <returns>Common characters</returns>
        private string GetCommonStartString(string item1, string item2)
        {
            var commonCharCount = 0;

            for (var i = 0; i < Math.Min(item1.Length, item2.Length); i++)
            {
                if (item1[i] != item2[i])
                    break;

                commonCharCount++;
            }

            return item1.Substring(0, commonCharCount);
        }

        private string GetRelativeFilePath(FileInfo file, FileSystemInfo parentDirectory)
        {
            if (parentDirectory == null || file.DirectoryName == null || file.DirectoryName.Length == 0)
            {
                return file.Name;
            }

            if (parentDirectory.FullName.Equals(file.DirectoryName))
            {
                return file.Name;
            }

            // Use Linux style directory separators (forward slashes)
            return file.FullName.Substring(parentDirectory.FullName.Length + 1).Replace('\\', '/');
        }

        /// <summary>
        /// Gets the .zip file path to create when zipping a single file
        /// </summary>
        /// <param name="sourceFilePath"></param>
        public static string GetZipFilePathForFile(string sourceFilePath)
        {
            var sourceFile = new FileInfo(sourceFilePath);

            if (sourceFile.DirectoryName == null)
                return Path.GetFileNameWithoutExtension(sourceFile.Name) + ".zip";

            return Path.Combine(sourceFile.DirectoryName, Path.GetFileNameWithoutExtension(sourceFile.Name) + ".zip");
        }

        /// <summary>
        /// Unzip gzipFilePath into the working directory defined when this class was instantiated
        /// Existing files will be overwritten
        /// </summary>
        /// <param name="gzipFilePath">.gz file to unzip</param>
        /// <returns>True if success, false if an error</returns>
        public bool GUnzipFile(string gzipFilePath)
        {
            return GUnzipFile(gzipFilePath, mWorkDir);
        }

        /// <summary>
        /// Unzip gzipFilePath into the specified target directory
        /// Existing files will be overwritten
        /// </summary>
        /// <param name="gzipFilePath">.gz file to unzip</param>
        /// <param name="targetDirectory">Directory to place the unzipped files</param>
        /// <returns>True if success, false if an error</returns>
        public bool GUnzipFile(string gzipFilePath, string targetDirectory)
        {
            return GUnzipFile(gzipFilePath, targetDirectory, ExtractExistingFileBehavior.OverwriteSilently);
        }

        /// <summary>
        /// Unzip gzipFilePath into the specified target directory
        /// </summary>
        /// <param name="gzipFilePath">.gz file to unzip</param>
        /// <param name="targetDirectory">Directory to place the unzipped files</param>
        /// <param name="overwriteBehavior">Defines what to do when existing files could be overwritten</param>
        /// <returns>True if success, false if an error</returns>
        public bool GUnzipFile(string gzipFilePath, string targetDirectory, ExtractExistingFileBehavior overwriteBehavior)
        {
            MostRecentZipFilePath = gzipFilePath;
            MostRecentUnzippedFiles.Clear();

            try
            {
                var fileToGUnzip = new FileInfo(gzipFilePath);

                if (!fileToGUnzip.Exists)
                {
                    LogError("GZip file not found: " + fileToGUnzip.FullName);
                    return false;
                }

                if (!string.Equals(fileToGUnzip.Extension, ".gz", StringComparison.OrdinalIgnoreCase))
                {
                    LogError("Not a GZipped file; must have extension .gz: " + fileToGUnzip.FullName);
                    return false;
                }

                if (DebugLevel >= 3)
                {
                    OnStatusEvent("Unzipping file: " + fileToGUnzip.FullName);
                }

                var startTime = DateTime.UtcNow;

                // Get original file extension, for example "doc" from report.doc.gz
                var curFile = fileToGUnzip.Name;
                var decompressedFilePath = Path.Combine(targetDirectory, curFile.Remove(curFile.Length - fileToGUnzip.Extension.Length));

                var decompressedFile = new FileInfo(decompressedFilePath);

                if (decompressedFile.Exists)
                {
                    if (overwriteBehavior == ExtractExistingFileBehavior.DoNotOverwrite)
                    {
                        Message = "Decompressed file already exists; will not overwrite: " + decompressedFile.FullName;
                        OnStatusEvent(Message);
                        return true;
                    }

                    if (overwriteBehavior == ExtractExistingFileBehavior.Throw)
                    {
                        throw new Exception("Decompressed file already exists: " + decompressedFile.FullName);
                    }
                }
                else
                {
                    // Make sure the target directory exists
                    if (decompressedFile.Directory?.Exists == false)
                        decompressedFile.Directory.Create();
                }

                var actualDecompressedFilePath = FileTools.GZipDecompressWithMetadata(fileToGUnzip, decompressedFile.DirectoryName);
                var actualDecompressedFile = new FileInfo(actualDecompressedFilePath);

                if (!string.Equals(decompressedFile.FullName, actualDecompressedFile.FullName))
                {
                    OnWarningEvent("GZipDecompressWithMetadata created a different file than expected; {0} vs. expected {1}", actualDecompressedFile.FullName, decompressedFile.FullName);
                }

                MostRecentUnzippedFiles.Add(new KeyValuePair<string, string>(actualDecompressedFile.Name, actualDecompressedFile.FullName));

                var endTime = DateTime.UtcNow;

                if (DebugLevel >= 2)
                {
                    ReportZipStats(fileToGUnzip, startTime, endTime, false, "GZipStream");
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
        /// <param name="deleteSourceAfterZip">If true, will delete the source file after zipping it</param>
        /// <returns>True if success, false if an error</returns>
        public bool GZipFile(string sourceFilePath, bool deleteSourceAfterZip)
        {
            var sourceFile = new FileInfo(sourceFilePath);
            return GZipFile(sourceFilePath, sourceFile.DirectoryName, deleteSourceAfterZip);
        }

        /// <summary>
        /// Stores sourceFilePath in a GZip file with the same name, but with extension .gz appended to the name (e.g. Dataset.mzid.gz)
        /// </summary>
        /// <param name="sourceFilePath">Full path to the file to be zipped</param>
        /// <param name="targetDirectoryPath">Target directory to create the .gz file</param>
        /// <param name="deleteSourceAfterZip">If true, will delete the source file after zipping it</param>
        /// <returns>True if success, false if an error</returns>
        public bool GZipFile(string sourceFilePath, string targetDirectoryPath, bool deleteSourceAfterZip)
        {
            var sourceFile = new FileInfo(sourceFilePath);

            var gzipFilePath = Path.Combine(targetDirectoryPath, sourceFile.Name + ".gz");

            Message = string.Empty;
            MostRecentZipFilePath = gzipFilePath;

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

            var success = GZipUsingGZipStream(sourceFile, gzipFilePath);

            if (!success)
            {
                return false;
            }

            if (deleteSourceAfterZip)
            {
                DeleteFile(sourceFile);
            }

            return true;
        }

        /// <summary>
        /// Compress the file using GZipStream (as implemented in PRISM.dll)
        /// </summary>
        /// <remarks>The .gz file created by PRISM.dll will include header information (filename and timestamp of the original file)</remarks>
        /// <param name="fileToGZip">File to compress</param>
        /// <param name="gzipFilePath"></param>
        private bool GZipUsingGZipStream(FileInfo fileToGZip, string gzipFilePath)
        {
            try
            {
                if (DebugLevel >= 3)
                {
                    OnStatusEvent("Creating .gz file using GZipStream: " + gzipFilePath);
                }

                var startTime = DateTime.UtcNow;

                var gzipFile = new FileInfo(gzipFilePath);

                FileTools.GZipCompressWithMetadata(fileToGZip, gzipFile.DirectoryName, gzipFile.Name);

                var endTime = DateTime.UtcNow;

                if (DebugLevel >= 2)
                {
                    ReportZipStats(fileToGZip, startTime, endTime, true, "GZipStream");
                }

                // Update the file modification time of the .gz file to use the modification time of the original file
                var gzippedFile = new FileInfo(gzipFilePath);

                if (!gzippedFile.Exists)
                {
                    LogError("GZipStream did not create a .gz file: " + gzipFilePath);
                    return false;
                }

                gzippedFile.LastWriteTimeUtc = fileToGZip.LastWriteTimeUtc;
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
        /// <remarks>If DebugLevel is 2 or larger, also raises event StatusEvent</remarks>
        /// <param name="zippedFiles">List of files that were added to the zip file</param>
        /// <param name="startTime">Start time</param>
        /// <param name="endTime">End time</param>
        /// <param name="zipProgramName">Zip program name</param>
        private void ReportZipStats(
            IReadOnlyList<FileInfo> zippedFiles,
            DateTime startTime,
            DateTime endTime,
            string zipProgramName)
        {
            long totalSizeBytes = 0;

            foreach (var item in zippedFiles)
            {
                if (!item.Exists)
                    continue;

                totalSizeBytes += item.Length;
            }

            var descriptionOfZippedItems = zippedFiles.Count == 1
                ? "file " + zippedFiles[0]
                : string.Format("{0} files", zippedFiles.Count);

            ReportZipStats(descriptionOfZippedItems, totalSizeBytes, startTime, endTime, true, zipProgramName);
        }

        /// <summary>
        /// Update Message with stats on the most recent zip file created
        /// </summary>
        /// <remarks>If DebugLevel is 2 or larger, also raises event StatusEvent</remarks>
        /// <param name="fileOrDirectoryZippedOrUnzipped">File or directory that was zipped</param>
        /// <param name="startTime">Start time</param>
        /// <param name="endTime">End time</param>
        /// <param name="fileWasZipped">True if creating a zip file, false if extracting files from an existing zip file</param>
        /// <param name="zipProgramName">Zip program name</param>
        /// <param name="includedAllSubdirectories">
        /// When fileOrDirectoryZippedOrUnzipped is a directory, set this to true if subdirectories were added to the .zip file
        /// </param>
        private void ReportZipStats(
            FileSystemInfo fileOrDirectoryZippedOrUnzipped,
            DateTime startTime,
            DateTime endTime,
            bool fileWasZipped,
            string zipProgramName,
            bool includedAllSubdirectories = false)
        {
            long totalSizeBytes;
            string descriptionOfZippedItems;

            switch (fileOrDirectoryZippedOrUnzipped)
            {
                case FileInfo processedFile:
                    totalSizeBytes = processedFile.Length;
                    descriptionOfZippedItems = "file " + processedFile.Name;
                    break;

                case DirectoryInfo processedDirectory:
                    var searchOption = includedAllSubdirectories ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly;

                    totalSizeBytes = processedDirectory.GetFiles("*", searchOption).Sum(item => item.Length);

                    descriptionOfZippedItems = "directory " + processedDirectory.Name;
                    break;

                default:
                    totalSizeBytes = 0;
                    descriptionOfZippedItems = "unknown item";
                    break;
            }

            ReportZipStats(descriptionOfZippedItems, totalSizeBytes, startTime, endTime, fileWasZipped, zipProgramName);
        }

        /// <summary>
        /// Update Message with stats on the most recent zip file created
        /// </summary>
        /// <remarks>If DebugLevel is 2 or larger, also raises event StatusEvent</remarks>
        /// <param name="descriptionOfZippedItems">Description of the zipped items</param>
        /// <param name="totalSizeBytes">Total size, in bytes</param>
        /// <param name="startTime">Start time</param>
        /// <param name="endTime">End time</param>
        /// <param name="fileWasZipped">True if creating a zip file, false if extracting files from an existing zip file</param>
        /// <param name="zipProgramName">Zip program name</param>
        private void ReportZipStats(
            string descriptionOfZippedItems,
            long totalSizeBytes,
            DateTime startTime,
            DateTime endTime,
            bool fileWasZipped,
            string zipProgramName)
        {
            double unzipSpeedMBPerSec;

            zipProgramName ??= "??";

            var unzipTimeSeconds = endTime.Subtract(startTime).TotalSeconds;

            if (unzipTimeSeconds > 0)
            {
                unzipSpeedMBPerSec = Global.BytesToMB(totalSizeBytes) / unzipTimeSeconds;
            }
            else
            {
                unzipSpeedMBPerSec = 0;
            }

            var zipAction = fileWasZipped ? "Zipped " : "Unzipped ";

            Message = zipAction + descriptionOfZippedItems + " using " + zipProgramName + "; " +
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
        /// <returns>True if success, false if an error</returns>
        public bool UnzipFile(string zipFilePath)
        {
            return UnzipFile(zipFilePath, mWorkDir);
        }

        /// <summary>
        /// Unzip zipFilePath into the specified target directory
        /// Existing files will be overwritten
        /// </summary>
        /// <param name="zipFilePath">File to unzip</param>
        /// <param name="targetDirectory">Directory to place the unzipped files</param>
        /// <returns>True if success, false if an error</returns>
        public bool UnzipFile(string zipFilePath, string targetDirectory)
        {
            return UnzipFile(zipFilePath, targetDirectory, string.Empty, ExtractExistingFileBehavior.OverwriteSilently);
        }

        /// <summary>
        /// Unzip zipFilePath into the specified target directory, applying the specified file filter
        /// Existing files will be overwritten
        /// </summary>
        /// <param name="zipFilePath">File to unzip</param>
        /// <param name="targetDirectory">Directory to place the unzipped files</param>
        /// <param name="fileFilter">Filter to apply when unzipping</param>
        /// <returns>True if success, false if an error</returns>
        public bool UnzipFile(string zipFilePath, string targetDirectory, string fileFilter)
        {
            return UnzipFile(zipFilePath, targetDirectory, fileFilter, ExtractExistingFileBehavior.OverwriteSilently);
        }

        /// <summary>
        /// Unzip zipFilePath into the specified target directory, applying the specified file filter
        /// </summary>
        /// <param name="zipFilePath">File to unzip</param>
        /// <param name="targetDirectory">Directory to place the unzipped files</param>
        /// <param name="fileFilter">Filter to apply when unzipping</param>
        /// <param name="overwriteBehavior">Defines what to do when existing files could be overwritten</param>
        /// <returns>True if success, false if an error</returns>
        public bool UnzipFile(string zipFilePath, string targetDirectory, string fileFilter, ExtractExistingFileBehavior overwriteBehavior)
        {
            Message = string.Empty;
            MostRecentZipFilePath = zipFilePath;
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

                var startTime = DateTime.UtcNow;

                using (var archive = System.IO.Compression.ZipFile.OpenRead(zipFilePath))
                {
                    var targetDirectoryInstance = new DirectoryInfo(targetDirectory);

                    if (!targetDirectoryInstance.Exists)
                    {
                        targetDirectoryInstance.Create();
                    }

                    if (string.IsNullOrWhiteSpace(fileFilter))
                    {
                        // When the overwrite behavior is "Throw", use method ExtractToDirectory() since it raises an exception if an existing file is found

                        if (overwriteBehavior == ExtractExistingFileBehavior.Throw)
                        {
                            archive.ExtractToDirectory(targetDirectory);

                            foreach (var item in archive.Entries)
                            {
                                // Note that item.FullName contains the relative path of the file, for example "Filename.txt" or "Subdirectory/Filename.txt"
                                var unzippedItem = new FileInfo(Path.Combine(targetDirectory, item.FullName.Replace('/', Path.DirectorySeparatorChar)));

                                MostRecentUnzippedFiles.Add(new KeyValuePair<string, string>(unzippedItem.Name, unzippedItem.FullName));
                            }
                        }
                        else
                        {
                            UnzipFiles(archive, targetDirectory, "*", overwriteBehavior);
                        }
                    }
                    else
                    {
                        UnzipFiles(archive, targetDirectory, fileFilter, overwriteBehavior);
                    }
                }

                var endTime = DateTime.UtcNow;

                if (DebugLevel >= 2)
                {
                    ReportZipStats(fileToUnzip, startTime, endTime, false, SYSTEM_IO_COMPRESSION_ZIP_NAME);
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error unzipping file " + zipFilePath, ex);
                return false;
            }
        }

        private void UnzipFiles(ZipArchive archive, string targetDirectory, string fileFilter, ExtractExistingFileBehavior overwriteBehavior)
        {
            var overwrite = (overwriteBehavior == ExtractExistingFileBehavior.OverwriteSilently);

            // Microsoft.Extensions.FileSystemGlobbing.Matcher
            Matcher matcher = new();
            matcher.AddInclude(fileFilter);

            foreach (var item in archive.Entries)
            {
                var result = matcher.Match(item.Name);

                if (!result.HasMatches)
                    continue;

                var targetFile = new FileInfo(Path.Combine(targetDirectory, item.FullName));

                // ReSharper disable once ConvertIfStatementToSwitchStatement
                if (targetFile.Exists && overwriteBehavior == ExtractExistingFileBehavior.Throw)
                {
                    throw new Exception(string.Format(
                        "Cannot unzip {0} since a file already exists at {1}", item.Name, targetFile.FullName));
                }

                if (targetFile.Exists && overwriteBehavior == ExtractExistingFileBehavior.DoNotOverwrite)
                {
                    OnWarningEvent("Skipping overwrite of existing file: {0}", targetFile.FullName);
                    continue;
                }

                // ReSharper disable once MergeIntoPattern
                if (targetFile.Directory != null && !targetFile.Directory.Exists)
                {
                    targetFile.Directory.Create();
                }

                item.ExtractToFile(targetFile.FullName, overwrite);

                // Note that item.FullName contains the relative path of the file, for example "Filename.txt" or "Subdirectory/Filename.txt"
                var unzippedItem = new FileInfo(Path.Combine(targetDirectory, item.FullName.Replace('/', Path.DirectorySeparatorChar)));

                MostRecentUnzippedFiles.Add(new KeyValuePair<string, string>(unzippedItem.Name, unzippedItem.FullName));
            }
        }

        /// <summary>
        /// Verifies that the zip file exists
        /// If the file size is less than 4 GB, also extracts each file to a memory stream (thus verifying the zip file is not corrupt)
        /// </summary>
        /// <param name="zipFilePath">Zip file to check</param>
        /// <returns>True if a valid zip file, otherwise false</returns>
        public bool VerifyZipFile(string zipFilePath)
        {
            return VerifyZipFile(zipFilePath, crcCheckThresholdGB: 4);
        }

        /// <summary>
        /// Verifies that the zip file exists
        /// If the file size is less than crcCheckThresholdGB, also extracts each file to a memory stream (thus verifying the zip file is not corrupt)
        /// </summary>
        /// <param name="zipFilePath">Zip file to check</param>
        /// <param name="crcCheckThresholdGB">Threshold (in GB) below which the zip file integrity should be checked</param>
        /// <returns>True if a valid zip file, otherwise false</returns>
        public bool VerifyZipFile(string zipFilePath, float crcCheckThresholdGB)
        {
            try
            {
                // Confirm that the zip file was created
                var zipFile = new FileInfo(zipFilePath);

                if (!zipFile.Exists)
                {
                    LogError("Zip file not found: " + zipFilePath);
                    return false;
                }

                // Open the zip file
                using var zipper = System.IO.Compression.ZipFile.OpenRead(zipFilePath);

                // For zip files less than 4 GB in size, perform a full unzip test to confirm that the file is not corrupted
                var crcCheckThresholdBytes = (long)(crcCheckThresholdGB * 1024 * 1024 * 1024);

                if (zipFile.Length > crcCheckThresholdBytes)
                {
                    // File is too big; do not verify it
                    return true;
                }

                // Unzip each zipped file to a byte buffer (no need to actually write to disk)
                foreach (var entry in zipper.Entries)
                {
                    var success = VerifyZipFileEntry(zipFilePath, entry);

                    if (!success)
                        return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error verifying zip file " + zipFilePath, ex);
                return false;
            }
        }

        private bool VerifyZipFileEntry(string zipFilePath, ZipArchiveEntry entry)
        {
            var buffer = new byte[8096];
            long totalBytesRead = 0;

            using var reader = entry.Open();

            int n;

            do
            {
                n = reader.Read(buffer, 0, buffer.Length);
                totalBytesRead += n;
            } while (n > 0);

            // Deprecated code that was used when verifying a zip file with System.IO.Compression.ZipFile
            // if (reader.Crc != entry.Crc)
            // {
            //     Message = string.Format("Zip entry " + entry.FileName + " failed the CRC Check in " + zipFilePath +
            //                             " (0x{0:X8} != 0x{1:X8})", reader.Crc, entry.Crc);
            //     OnErrorEvent(Message);
            //     return false;
            // }

            if (totalBytesRead == entry.Length)
            {
                return true;
            }

            Message = string.Format("Unexpected number of bytes for entry " + entry.FullName + " in " + zipFilePath +
                                    " ({0} != {1})", totalBytesRead, entry.Length);
            OnWarningEvent(Message);
            return false;
        }

        /// <summary>
        /// Stores sourceFilePath in a zip file with the same name, but extension .zip
        /// </summary>
        /// <param name="sourceFilePath">Full path to the file to be zipped</param>
        /// <param name="deleteSourceAfterZip">If true, will delete the source file after zipping it</param>
        /// <param name="verifyZipFile">When true, verify the newly created .zip file</param>
        /// <returns>True if success, false if an error</returns>
        public bool ZipFile(string sourceFilePath, bool deleteSourceAfterZip, bool verifyZipFile = false)
        {
            var zipFilePath = GetZipFilePathForFile(sourceFilePath);

            return ZipFile(sourceFilePath, deleteSourceAfterZip, zipFilePath, verifyZipFile);
        }

        /// <summary>
        /// Stores sourceFilePath in a zip file named zipFilePath
        /// </summary>
        /// <param name="sourceFilePath">Full path to the file to be zipped</param>
        /// <param name="deleteSourceAfterZip">If true, will delete the source file after zipping it</param>
        /// <param name="zipFilePath">Full path to the .zip file to be created; existing files will be overwritten</param>
        /// <param name="verifyZipFile">When true, verify the newly created .zip file</param>
        /// <returns>True if success, false if an error</returns>
        public bool ZipFile(string sourceFilePath, bool deleteSourceAfterZip, string zipFilePath, bool verifyZipFile = false)
        {
            var fileToZip = new FileInfo(sourceFilePath);

            Message = string.Empty;
            MostRecentZipFilePath = zipFilePath;

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
                LogError("Error deleting target .zip file prior to zipping file " + sourceFilePath + " using " + SYSTEM_IO_COMPRESSION_ZIP_NAME, ex);
                return false;
            }

            try
            {
                if (DebugLevel >= 3)
                {
                    OnStatusEvent("Creating .zip file: " + zipFilePath);
                }

                using var zipFile = System.IO.Compression.ZipFile.Open(zipFilePath, ZipArchiveMode.Create);

                var startTime = DateTime.UtcNow;

                zipFile.CreateEntryFromFile(fileToZip.FullName, fileToZip.Name);

                var endTime = DateTime.UtcNow;

                if (DebugLevel >= 2)
                {
                    ReportZipStats(fileToZip, startTime, endTime, true, SYSTEM_IO_COMPRESSION_ZIP_NAME);
                }
            }
            catch (Exception ex)
            {
                LogError("Error zipping file " + fileToZip.FullName, ex);
                return false;
            }

            // Verify that the zip file is not corrupt

            if (verifyZipFile && !VerifyZipFile(zipFilePath))
            {
                return false;
            }

            if (deleteSourceAfterZip)
            {
                DeleteFile(fileToZip);
            }

            return true;
        }

        // ReSharper disable once UnusedMember.Global

        /// <summary>
        /// Stores a list of files in a zip file named zipFilePath
        /// </summary>
        /// <remarks>If the files have a mix of parent directories, the original directory layout will be retained in the .zip file</remarks>
        /// <param name="filePaths">List of file paths to store in the zip file</param>
        /// <param name="zipFilePath">Full path to the .zip file to be created; existing files will be overwritten</param>
        /// <returns>True if success, false if an error</returns>
        public bool ZipFiles(IReadOnlyList<string> filePaths, string zipFilePath)
        {
            var filesToZip = filePaths.Select(item => new FileInfo(item)).ToList();

            return ZipFiles(filesToZip, zipFilePath);
        }

        /// <summary>
        /// Stores a list of files in a zip file named zipFilePath
        /// </summary>
        /// <remarks>If the files have a mix of parent directories, the original directory layout will be retained in the .zip file</remarks>
        /// <param name="filesToZip">List of file paths to store in the zip file</param>
        /// <param name="zipFilePath">Full path to the .zip file to be created; existing files will be overwritten</param>
        /// <param name="verifyZipFile">When true, verify the newly created .zip file</param>
        /// <returns>True if success, false if an error</returns>
        public bool ZipFiles(IReadOnlyList<FileInfo> filesToZip, string zipFilePath, bool verifyZipFile = false)
        {
            Message = string.Empty;
            MostRecentZipFilePath = zipFilePath;

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
                LogError("Error deleting target .zip file prior to zipping a list of files using " + SYSTEM_IO_COMPRESSION_ZIP_NAME, ex);
                return false;
            }

            try
            {
                if (DebugLevel >= 3)
                {
                    OnStatusEvent("Creating .zip file: " + zipFilePath);
                }

                var parentDirectories = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

                foreach (var item in filesToZip)
                {
                    if (item.Directory == null)
                        continue;

                    // Add the directory (if not already present)
                    parentDirectories.Add(item.Directory.FullName);
                }

                var relativePathByFile = new List<KeyValuePair<FileInfo, string>>();

                if (parentDirectories.Count <= 1)
                {
                    foreach (var item in filesToZip)
                    {
                        relativePathByFile.Add(new KeyValuePair<FileInfo, string>(item, item.Name));
                    }
                }
                else
                {
                    var commonParentDirectoryPath = string.Empty;

                    foreach (var item in parentDirectories)
                    {
                        if (string.IsNullOrEmpty(commonParentDirectoryPath))
                        {
                            commonParentDirectoryPath = item;
                            continue;
                        }

                        var commonStartString = GetCommonStartString(commonParentDirectoryPath, item);

                        if (commonStartString.Equals(commonParentDirectoryPath))
                            continue;

                        commonParentDirectoryPath = commonStartString;

                        if (string.IsNullOrWhiteSpace(commonStartString))
                            break;
                    }

                    if (string.IsNullOrWhiteSpace(commonParentDirectoryPath))
                    {
                        foreach (var item in filesToZip)
                        {
                            relativePathByFile.Add(new KeyValuePair<FileInfo, string>(item, item.Name));
                        }
                    }
                    else
                    {
                        var commonParentDirectory = new DirectoryInfo(commonParentDirectoryPath);

                        foreach (var sourceFile in filesToZip)
                        {
                            var relativePath = GetRelativeFilePath(sourceFile, commonParentDirectory);

                            relativePathByFile.Add(new KeyValuePair<FileInfo, string>(sourceFile, relativePath));
                        }
                    }
                }

                var startTime = DateTime.UtcNow;

                using (var zipFile = System.IO.Compression.ZipFile.Open(zipFilePath, ZipArchiveMode.Create))
                {
                    foreach (var sourceFile in relativePathByFile)
                    {
                        zipFile.CreateEntryFromFile(sourceFile.Key.FullName, sourceFile.Value);
                    }
                }

                var endTime = DateTime.UtcNow;

                if (DebugLevel >= 2)
                {
                    ReportZipStats(filesToZip, startTime, endTime, SYSTEM_IO_COMPRESSION_ZIP_NAME);
                }
            }
            catch (Exception ex)
            {
                LogError("Error zipping the list of files", ex);
                return false;
            }

            // ReSharper disable once ConvertIfStatementToReturnStatement
            if (!verifyZipFile)
                return true;

            // Verify that the zip file is not corrupt
            return VerifyZipFile(zipFilePath);
        }

        /// <summary>
        /// Zip all files in a directory
        /// </summary>
        /// <param name="sourceDirectoryPath"></param>
        /// <param name="zipFilePath"></param>
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
        public bool ZipDirectory(string sourceDirectoryPath, string zipFilePath, bool recurse)
        {
            return ZipDirectory(sourceDirectoryPath, zipFilePath, recurse, string.Empty);
        }

        /// <summary>
        /// Stores all files in a source directory into a zip file named zipFilePath
        /// </summary>
        /// <param name="sourceDirectoryPath">Full path to the directory to be zipped</param>
        /// <param name="zipFilePath">Full path to the .zip file to be created; existing files will be overwritten</param>
        /// <param name="recurse">If true, recurse through all subdirectories</param>
        /// <param name="fileFilter">Filter to apply when zipping</param>
        /// <param name="verifyZipFile">When true, verify the newly created .zip file</param>
        /// <returns>True if success, false if an error</returns>
        public bool ZipDirectory(string sourceDirectoryPath, string zipFilePath, bool recurse, string fileFilter, bool verifyZipFile = false)
        {
            var directoryToZip = new DirectoryInfo(sourceDirectoryPath);

            Message = string.Empty;
            MostRecentZipFilePath = zipFilePath;

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
                LogError("Error deleting target .zip file prior to zipping directory " + sourceDirectoryPath + " using " + SYSTEM_IO_COMPRESSION_ZIP_NAME, ex);
                return false;
            }

            try
            {
                if (DebugLevel >= 3)
                {
                    OnStatusEvent("Creating .zip file: " + zipFilePath);
                }

                var startTime = DateTime.UtcNow;

                if (string.IsNullOrWhiteSpace(fileFilter) && recurse)
                {
                    System.IO.Compression.ZipFile.CreateFromDirectory(directoryToZip.FullName, zipFilePath);
                }
                else
                {
                    using var zipFile = System.IO.Compression.ZipFile.Open(zipFilePath, ZipArchiveMode.Create);

                    if (string.IsNullOrWhiteSpace(fileFilter))
                    {
                        fileFilter = "*";
                    }

                    var searchOption = recurse ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly;

                    foreach (var sourceFile in directoryToZip.GetFiles(fileFilter, searchOption))
                    {
                        var relativeFilePath = GetRelativeFilePath(sourceFile, directoryToZip);

                        zipFile.CreateEntryFromFile(sourceFile.FullName, relativeFilePath);

                    }
                }
                var endTime = DateTime.UtcNow;

                if (DebugLevel >= 2)
                {
                    ReportZipStats(directoryToZip, startTime, endTime, true, SYSTEM_IO_COMPRESSION_ZIP_NAME, recurse);
                }
            }
            catch (Exception ex)
            {
                LogError("Error zipping directory " + directoryToZip.FullName, ex);
                return false;
            }

            // ReSharper disable once ConvertIfStatementToReturnStatement
            if (!verifyZipFile)
                return true;

            // Verify that the zip file is not corrupt
            return VerifyZipFile(zipFilePath);
        }
    }
}
