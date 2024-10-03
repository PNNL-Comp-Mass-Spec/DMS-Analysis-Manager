using System;
using System.Collections.Generic;
using System.IO;
using PRISM;

namespace AnalysisManagerBase.FileAndDirectoryTools
{
    internal class WorkingDirectoryMetadata : EventNotifier
    {
        /// <summary>
        /// Tracks filename, size, and last modify date
        /// </summary>
        public struct FileMetadata
        {
            /// <summary>
            /// Filename
            /// </summary>
            public string Name;

            /// <summary>
            /// File size (bytes)
            /// </summary>
            public long Length;

            /// <summary>
            /// Last write time
            /// </summary>
            public DateTime LastModifiedUTC;

            /// <summary>
            /// Show the filename
            /// </summary>
            public readonly override string ToString()
            {
                return Name;
            }
        }

        /// <summary>
        /// Tracks files in the working directory and its subdirectories, by directory
        /// </summary>
        /// <remarks>
        /// Keys are the path to a directory
        /// Values are a dictionary of the files in that directory, with keys being filename and values instances of FileMetadata
        /// </remarks>
        public Dictionary<string, Dictionary<string, FileMetadata>> WorkingDirectoryFiles { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        public WorkingDirectoryMetadata()
        {
            WorkingDirectoryFiles = new Dictionary<string, Dictionary<string, FileMetadata>>();
        }

        /// <summary>
        /// Store information about a file in the working directory
        /// </summary>
        /// <param name="fileToAdd">File to add</param>
        public void AddWorkingDirectoryFile(FileInfo fileToAdd)
        {
            if (fileToAdd.Directory == null)
            {
                OnWarningEvent("Cannot add file to metadata dictionary since unable to determine the parent directory: " + fileToAdd.FullName);
                return;
            }

            var parentDirectoryPath = fileToAdd.Directory.FullName;

            var metadata = new FileMetadata
            {
                Name = fileToAdd.Name,
                Length = fileToAdd.Length,
                LastModifiedUTC = fileToAdd.LastWriteTimeUtc
            };

            AddWorkingDirectoryFile(parentDirectoryPath, metadata);
        }

        /// <summary>
        /// Store information about a file in the working directory
        /// </summary>
        /// <param name="parentDirectoryPath">Parent directory path</param>
        /// <param name="metadata">File metadata</param>
        public void AddWorkingDirectoryFile(string parentDirectoryPath, FileMetadata metadata)
        {
            if (WorkingDirectoryFiles.TryGetValue(parentDirectoryPath, out var subdirectoryFiles))
            {
                if (subdirectoryFiles.ContainsKey(metadata.Name))
                {
                    OnWarningEvent("Subdirectory already has metadata stored file; skipping " + Path.Combine(parentDirectoryPath, metadata.Name));
                    return;
                }

                subdirectoryFiles.Add(metadata.Name, metadata);
                return;
            }

            WorkingDirectoryFiles.Add(
                parentDirectoryPath,
                new Dictionary<string, FileMetadata> {
                    {metadata.Name, metadata}
                });
        }

        public bool TryGetFile(FileInfo targetFile, out FileMetadata fileMetadata)
        {
            if (targetFile.Directory == null)
            {
                OnWarningEvent("Cannot look for file in metadata dictionary since unable to determine the parent directory: " + targetFile.FullName);
                fileMetadata = new FileMetadata();
                return false;
            }

            if (WorkingDirectoryFiles.TryGetValue(targetFile.Directory.FullName, out var subdirectoryFiles) &&
                subdirectoryFiles.TryGetValue(targetFile.Name, out fileMetadata))
            {
                return true;
            }

            fileMetadata = new FileMetadata();
            return false;
        }
    }
}
