using System;
using System.Collections.Generic;
using System.IO;
using PRISM;
using PRISM.Logging;
using PRISMWin;

namespace AnalysisManagerBase.FileAndDirectoryTools
{
    /// <summary>
    /// Methods for determining free disk space, either on a local drive or a remote network share
    /// </summary>
    public class DirectorySpaceTools : EventNotifier
    {
        /// <summary>
        /// When true, log errors and warnings using the LogTools class
        /// Otherwise, use EventNotifier events
        /// </summary>
        public bool UseLogTools { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public DirectorySpaceTools(bool useLogTools = false)
        {
            UseLogTools = useLogTools;
        }

        /// <summary>
        /// Convert Bytes to Gigabytes
        /// </summary>
        /// <param name="bytes"></param>
        public static double BytesToGB(long bytes)
        {
            return bytes / 1024.0 / 1024.0 / 1024.0;
        }

        /// <summary>
        /// Convert Bytes to Megabytes
        /// </summary>
        /// <param name="bytes"></param>
        public static double BytesToMB(long bytes)
        {
            return bytes / 1024.0 / 1024.0;
        }

        /// <summary>
        /// Determine the free disk space on the drive with the given directory
        /// </summary>
        /// <param name="targetDirectory"></param>
        private double GetFreeDiskSpaceLinux(DirectoryInfo targetDirectory)
        {
            var driveInfo = GetLocalDriveInfo(targetDirectory);
            if (driveInfo == null)
                return 0;

            return BytesToMB(driveInfo.TotalFreeSpace);
        }

        /// <summary>
        /// Determine the free disk space on the drive with the given directory
        /// </summary>
        /// <remarks>Supports local drives on Windows and Linux; supports remote shares like \\Server\Share\ on Windows</remarks>
        /// <param name="targetDirectory"></param>
        /// <returns>Free space, in MB</returns>
        private double GetFreeDiskSpaceWindows(DirectoryInfo targetDirectory)
        {
            double freeSpaceMB;

            if (targetDirectory.Root.FullName.StartsWith(@"\\") || !targetDirectory.Root.FullName.Contains(":"))
            {
                // Directory path is a remote share; use GetDiskFreeSpaceEx in Kernel32.dll
                var targetFilePath = Path.Combine(targetDirectory.FullName, "DummyFile.txt");

                var success = DiskInfo.GetDiskFreeSpace(
                    targetFilePath, out var totalNumberOfFreeBytes, out var errorMessage, reportFreeSpaceAvailableToUser: false);

                if (success)
                {
                    freeSpaceMB = BytesToMB(totalNumberOfFreeBytes);
                }
                else
                {
                    if (UseLogTools)
                        LogTools.LogWarning(errorMessage);
                    else
                        OnWarningEvent(errorMessage);

                    freeSpaceMB = 0;
                }
            }
            else
            {
                // Directory is a local drive; can query with .NET
                var driveInfo = new DriveInfo(targetDirectory.Root.FullName);
                freeSpaceMB = BytesToMB(driveInfo.TotalFreeSpace);
            }

            return freeSpaceMB;
        }

        /// <summary>
        /// Get a DriveInfo instance for the drive with the given target directory (must be on the local host)
        /// Supports both Windows and Linux paths
        /// </summary>
        /// <param name="targetDirectory"></param>
        public DriveInfo GetLocalDriveInfo(DirectoryInfo targetDirectory)
        {
            var baseWarningMsg = "Unable to instantiate a DriveInfo object for " + targetDirectory.FullName;

            try
            {
                if (Path.DirectorySeparatorChar == '/' || targetDirectory.FullName.StartsWith("/"))
                {
                    // Linux system, with a path like /file1/temp/DMSOrgDBs/
                    // The root path that we need to send to DriveInfo is likely /file1
                    // If that doesn't work, try /

                    var candidateRootPaths = new List<string>();
                    var slashIndex = targetDirectory.FullName.IndexOf('/', 1);

                    if (slashIndex > 0)
                    {
                        candidateRootPaths.Add(targetDirectory.FullName.Substring(0, slashIndex));
                    }
                    candidateRootPaths.Add("/");

                    foreach (var candidatePath in candidateRootPaths)
                    {
                        try
                        {
                            return new DriveInfo(candidatePath);
                        }
                        catch (Exception ex)
                        {
                            ConsoleMsgUtils.ShowDebug("Unable to create a DriveInfo object for {0}: {1}", candidatePath, ex.Message);
                        }
                    }
                }
                else
                {
                    // Windows system, with a path like C:\DMS_Temp_Org
                    // Alternatively, a Windows share like \\proto-7\MSGFPlus_Index_Files

                    var driveLetter = targetDirectory.FullName.Substring(0, 2);
                    if (driveLetter.EndsWith(":"))
                    {
                        return new DriveInfo(driveLetter);
                    }
                }
            }
            catch (Exception ex)
            {
                var warningMessage = string.Format("{0}: {1}", baseWarningMsg, ex);

                if (UseLogTools)
                    LogTools.LogWarning(warningMessage);
                else
                    OnWarningEvent(warningMessage);
            }

            if (UseLogTools)
                LogTools.LogWarning(baseWarningMsg);
            else
                OnWarningEvent(baseWarningMsg);

            return null;
        }

        /// <summary>
        /// Check the free space on the drive with the given directory
        /// </summary>
        /// <remarks>Supports local drives on Windows and Linux; supports remote shares like \\Server\Share\ on Windows</remarks>
        /// <param name="directoryDescription"></param>
        /// <param name="directoryPath"></param>
        /// <param name="minFreeSpaceMB"></param>
        /// <param name="errorMessage">Output: error message</param>
        /// <param name="logToDatabase"></param>
        /// <returns>True if the drive has sufficient free space, otherwise false</returns>
        public bool ValidateFreeDiskSpace(
            string directoryDescription,
            string directoryPath,
            int minFreeSpaceMB,
            out string errorMessage,
            bool logToDatabase = false)
        {
            errorMessage = string.Empty;

            var targetDirectory = new DirectoryInfo(directoryPath);
            if (!targetDirectory.Exists)
            {
                // Example error message: Organism DB directory not found: G:\DMS_Temp_Org
                errorMessage = directoryDescription + " not found: " + directoryPath;

                if (UseLogTools)
                    LogTools.LogError(errorMessage, null, logToDatabase);
                else
                    OnErrorEvent(errorMessage);

                return false;
            }

            double freeSpaceMB;

            if (SystemInfo.IsLinux)
            {
                freeSpaceMB = GetFreeDiskSpaceLinux(targetDirectory);
            }
            else
            {
                freeSpaceMB = GetFreeDiskSpaceWindows(targetDirectory);
            }

            if (freeSpaceMB < minFreeSpaceMB)
            {
                // Example error message: Organism DB directory drive has less than 6858 MB free: 5794 MB
                errorMessage = $"{directoryDescription} drive has less than {minFreeSpaceMB} MB free: {(int)freeSpaceMB} MB";
                Console.WriteLine(errorMessage);
                LogTools.LogError(errorMessage);
                return false;
            }

            return true;
        }
    }
}
