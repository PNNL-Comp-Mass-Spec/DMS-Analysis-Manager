using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using PRISM;

namespace AnalysisManagerBase.FileAndDirectoryTools
{
    /// <summary>
    /// This class has methods for zipping and unzipping subdirectories below a working directory
    /// </summary>
    /// <remarks>
    /// <para>
    /// When retrieving existing .zip files, after all files have been retrieved and unzipped,
    /// a metadata file is created listing all of the files in the working directory.
    /// </para>
    /// <para>
    /// Later, when instructed to re-zip the files, the directory contents are compared to the data in the metadata file
    /// If all files in the directory are unchanged, the directory will not be re-zipped, and an entry will be added to UnchangedDirectories
    /// </para>
    /// </remarks>
    public class SubdirectoryFileCompressor : EventNotifier
    {
        /// <summary>
        /// This file tracks information on all of the files in the working directory and its subdirectories
        /// It is a tab-delimited file with four columns:
        ///   DirectoryPath  FileName  FileSize  FileDate
        /// It is used by the ToolRunner to determine which subdirectories do not need to be re-zipped because the directory contents are unchanged
        /// </summary>
        public const string WORKING_DIRECTORY_METADATA_FILE = "__DMSWorkingDirectoryFileInfo__.txt";

        private int DebugLevel { get; }

        /// <summary>
        /// List of unchanged directories
        /// </summary>
        /// <remarks>
        /// Call ? to populate this list
        /// </remarks>
        public List<DirectoryInfo> UnchangedDirectories { get; }

        /// <summary>
        /// Working directory
        /// </summary>
        public DirectoryInfo WorkingDirectory { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="workingDirectory">Working directory</param>
        /// <param name="debugLevel">Debug Level for logging; 1=minimal logging; 5=detailed logging</param>
        public SubdirectoryFileCompressor(DirectoryInfo workingDirectory, int debugLevel)
        {
            UnchangedDirectories = new List<DirectoryInfo>();
            WorkingDirectory = workingDirectory;
            DebugLevel = debugLevel;
        }

        /// <summary>
        /// Create the metadata file in the working directory
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        public bool CreateWorkingDirectoryMetadataFile()
        {
            try
            {
                var metadataFile = new FileInfo(Path.Combine(WorkingDirectory.FullName, WORKING_DIRECTORY_METADATA_FILE));

                using var writer = new StreamWriter(new FileStream(metadataFile.FullName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite));

                foreach (var item in WorkingDirectory.GetFiles("*", SearchOption.AllDirectories))
                {
                    if (item.Directory == null)
                    {
                        OnErrorEvent("Unable to determine the parent directory of working directory file " + item.FullName);
                        return false;
                    }

                    writer.WriteLine("{0}\t{1}\t{2}\t{3:O}", item.Directory.FullName, item.Name, item.Length, item.LastWriteTimeUtc);
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception in CreateWorkingDirectoryMetadataFile", ex);
                return false;
            }
        }

        /// <summary>
        /// Examine a directory to see if it has any new or changed files
        /// </summary>
        /// <param name="workingDirectoryMetadata"></param>
        /// <param name="directoryToCheck"></param>
        /// <param name="isModified">True if modified, false if unmodified</param>
        /// <returns>True if success, false if an error</returns>
        private bool CheckDirectoryModified(
            WorkingDirectoryMetadata workingDirectoryMetadata,
            DirectoryInfo directoryToCheck,
            out bool isModified)
        {
            isModified = false;

            try
            {
                foreach (var item in directoryToCheck.GetFiles("*", SearchOption.AllDirectories))
                {
                    if (!workingDirectoryMetadata.TryGetFile(item, out var cachedMetadata))
                    {
                        // File is new; the directory is modified
                        isModified = true;
                        return true;
                    }

                    if (item.Length == cachedMetadata.Length && item.LastWriteTimeUtc == cachedMetadata.LastModifiedUTC)
                    {
                        // File is unchanged
                        continue;
                    }

                    // File is modified
                    isModified = true;
                    return true;
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in SubdirectoryFileCompressor->CheckDirectoryModified", ex);
                return false;
            }
        }

        /// <summary>
        /// Find unchanged directories by comparing to metadata in file WORKING_DIRECTORY_METADATA_FILE
        /// Results are stored in property UnchangedDirectories
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        public bool FindUnchangedDirectories()
        {
            UnchangedDirectories.Clear();

            try
            {
                // Load the directory files metadata file
                var metadataFile = new FileInfo(Path.Combine(WorkingDirectory.FullName, WORKING_DIRECTORY_METADATA_FILE));
                if (!metadataFile.Exists)
                {
                    OnWarningEvent("Working directory metadata file not found; the resourcer should have created file " + metadataFile.FullName);
                    return true;
                }

                var successReadMetadata = ReadWorkingDirectoryMetadataFile(metadataFile, out var workingDirectoryMetadata);

                if (!successReadMetadata)
                    return false;

                foreach (var subdirectory in WorkingDirectory.GetDirectories("*", SearchOption.AllDirectories))
                {
                    if (subdirectory.Parent == null)
                    {
                        OnErrorEvent("Unable to determine the parent directory of " + subdirectory.FullName);
                        return false;
                    }

                    var success = CheckDirectoryModified(workingDirectoryMetadata, subdirectory, out var isModified);
                    if (!success)
                        return false;

                    if (!isModified)
                    {
                        UnchangedDirectories.Add(subdirectory);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in SubdirectoryFileCompressor->FindUnchangedDirectories", ex);
                return false;
            }
        }

        private void OnDebugEvent(string debugMessage, int logFileDebugLevel = 0)
        {
            if (DebugLevel >= logFileDebugLevel)
                base.OnDebugEvent(debugMessage);
            else
                ConsoleMsgUtils.ShowDebug(debugMessage);
        }

        private bool ReadWorkingDirectoryMetadataFile(FileSystemInfo metadataFile, out WorkingDirectoryMetadata workingDirectoryMetadata)
        {
            workingDirectoryMetadata = new WorkingDirectoryMetadata();
            RegisterEvents(workingDirectoryMetadata);

            try
            {
                using var reader = new StreamReader(new FileStream(metadataFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();
                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    var lineParts = dataLine.Split('\t');
                    if (lineParts.Length < 4)
                    {
                        OnWarningEvent("Line in working directory metadata file does not have 4 columns; skipping " + dataLine);
                        continue;
                    }

                    var parentDirectoryPath = lineParts[0];
                    var metadata = new WorkingDirectoryMetadata.FileMetadata
                    {
                        Name = lineParts[1]
                    };

                    if (!long.TryParse(lineParts[2], out var fileSizeBytes))
                    {
                        OnWarningEvent(string.Format("File size is not an integer ({0}); skipping {1}", lineParts[2], dataLine));
                        continue;
                    }

                    if (!DateTime.TryParse(lineParts[3], out var lastModifiedUTC))
                    {
                        OnWarningEvent(string.Format("File modification time is not a date ({0}); skipping {1}", lineParts[3], dataLine));
                        continue;
                    }

                    metadata.Length = fileSizeBytes;
                    metadata.LastModifiedUTC = lastModifiedUTC;

                    workingDirectoryMetadata.AddWorkingDirectoryFile(parentDirectoryPath, metadata);
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in SubdirectoryFileCompressor->ReadWorkingDirectoryMetadataFile", ex);
                return false;
            }
        }

        /// <summary>
        /// Zip subdirectories below the working directory
        /// </summary>
        /// <param name="directoriesToSkip">List of directories to not zip</param>
        /// <param name="deleteFilesAfterZip">Set this to true to save disk space</param>
        /// <returns></returns>
        public bool ZipDirectories(IReadOnlyCollection<DirectoryInfo> directoriesToSkip, bool deleteFilesAfterZip = true)
        {
            try
            {
                var zipTools = new DotNetZipTools(DebugLevel, WorkingDirectory.FullName);
                RegisterEvents(zipTools);

                foreach (var subdirectory in WorkingDirectory.GetDirectories())
                {
                    var skipDirectory = directoriesToSkip.Any(item => item.FullName.Equals(subdirectory.FullName, StringComparison.OrdinalIgnoreCase));

                    if (skipDirectory)
                    {
                        OnDebugEvent("Not zipping directory since in directoriesToSkip: " + subdirectory.FullName, 2);
                        continue;
                    }

                    if (!subdirectory.Name.Equals("combined", StringComparison.OrdinalIgnoreCase))
                    {
                        var success = ZipDirectory(zipTools, subdirectory, deleteFilesAfterZip);

                        if (!success)
                        {
                            return false;
                        }
                        continue;
                    }

                    // Zip each subdirectory in the "combined" directory separately
                    // Leave the files in the "combined" directory unzipped
                    foreach (var item in subdirectory.GetDirectories())
                    {
                        var success = ZipDirectory(zipTools, item, deleteFilesAfterZip);

                        if (!success)
                        {
                            return false;
                        }
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in SubdirectoryFileCompressor->ZipDirectories", ex);
                return false;
            }
        }

        /// <summary>
        /// Zip a directory, plus optionally its subdirectories
        /// </summary>
        /// <param name="zipTools"></param>
        /// <param name="directoryToZip"></param>
        /// <param name="deleteFilesAfterZip">
        /// When true, delete the source files after the .zip file is created (useful for save disk space)
        /// </param>
        /// <returns>True if successful; false if an error</returns>
        private bool ZipDirectory(
            DotNetZipTools zipTools,
            DirectoryInfo directoryToZip,
            bool deleteFilesAfterZip = true)
        {
            try
            {
                if (directoryToZip.Parent == null)
                {
                    OnErrorEvent("Unable to determine the parent directory of directory; unable to zip " + directoryToZip.FullName);
                    return false;
                }

                var zipFilePath = Path.Combine(directoryToZip.Parent.FullName, directoryToZip.Name + ".zip");

                var success = zipTools.ZipDirectory(directoryToZip.FullName, zipFilePath, true);

                if (!success || !deleteFilesAfterZip)
                    return success;

                foreach (var fileToDelete in directoryToZip.GetFiles("*", SearchOption.AllDirectories))
                {
                    fileToDelete.Delete();
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in SubdirectoryFileCompressor->ZipDirectory", ex);
                return false;
            }
        }
    }
}
