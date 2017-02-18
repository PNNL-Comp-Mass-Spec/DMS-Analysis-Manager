
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Ionic.Zip;
using PRISM;

namespace AnalysisManagerBase
{
    public class clsIonicZipTools : clsEventNotifier
    {

        public const string IONIC_ZIP_NAME = "IonicZip (DotNetZip)";
        private int m_DebugLevel;

        private readonly string m_WorkDir;

        private string m_MostRecentZipFilePath = string.Empty;

        /// <summary>
        /// This variable tracks the files most recently unzipped
        /// Keys in the KeyValuePairs are filenames while values are relative paths (in case the .zip file has folders)
        /// </summary>
        private readonly List<KeyValuePair<string, string>> m_MostRecentUnzippedFiles = new List<KeyValuePair<string, string>>();

        private string m_Message = string.Empty;

        #region "Properties"

        public int DebugLevel
        {
            get { return m_DebugLevel; }
            set { m_DebugLevel = value; }
        }

        public string Message => m_Message;

        public string MostRecentZipFilePath => m_MostRecentZipFilePath;

        /// <summary>
        /// Returns the files most recently unzipped
        /// Keys in the KeyValuePairs are filenames while values are relative paths (in case the .zip file has folders)
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public List<KeyValuePair<string, string>> MostRecentUnzippedFiles => m_MostRecentUnzippedFiles;

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="debugLevel"></param>
        /// <param name="workDir"></param>
        public clsIonicZipTools(int debugLevel, string workDir)
        {
            m_DebugLevel = debugLevel;
            m_WorkDir = workDir;
        }

        [Obsolete("Unused")]
        private void DeleteFolder(DirectoryInfo diFolder)
        {

            try
            {
                if (m_DebugLevel >= 3)
                {
                    OnStatusEvent("Deleting folder: " + diFolder.FullName);
                }

                diFolder.Refresh();

                if (diFolder.Exists)
                {
                    // Now delete the source file
                    diFolder.Delete(true);
                }

                // Wait 100 msec
                Thread.Sleep(100);

            }
            catch (Exception ex)
            {
                // Log this as an error, but don't treat this as fatal
                LogError("Error deleting " + diFolder.FullName, ex);
            }

        }

        private void DeleteFile(FileInfo fiFile)
        {

            try
            {
                if (m_DebugLevel >= 3)
                {
                    OnStatusEvent("Deleting source file: " + fiFile.FullName);
                }

                fiFile.Refresh();

                if (fiFile.Exists)
                {
                    // Now delete the source file
                    fiFile.Delete();
                }


                // Wait 250 msec
                Thread.Sleep(250);

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
        /// Unzip GZipFilePath into the working directory defined when this class was instantiated
        /// Existing files will be overwritten
        /// </summary>
        /// <param name="GZipFilePath">.gz file to unzip</param>
        /// <returns>True if success; false if an error</returns>
        public bool GUnzipFile(string GZipFilePath)
        {
            return GUnzipFile(GZipFilePath, m_WorkDir);
        }

        /// <summary>
        /// Unzip GZipFilePath into the specified target directory
        /// Existing files will be overwritten
        /// </summary>
        /// <param name="GZipFilePath">.gz file to unzip</param>
        /// <param name="TargetDirectory">Folder to place the unzipped files</param>
        /// <returns>True if success; false if an error</returns>
        public bool GUnzipFile(string GZipFilePath, string TargetDirectory)
        {
            return GUnzipFile(GZipFilePath, TargetDirectory, Ionic.Zip.ExtractExistingFileAction.OverwriteSilently);
        }


        /// <summary>
        /// Unzip GZipFilePath into the specified target directory, applying the specified file filter
        /// </summary>
        /// <param name="GZipFilePath">.gz file to unzip</param>
        /// <param name="TargetDirectory">Folder to place the unzipped files</param>
        /// <param name="eOverwriteBehavior">Defines what to do when existing files could be ovewritten</param>
        /// <returns>True if success; false if an error</returns>
        public bool GUnzipFile(string GZipFilePath, string TargetDirectory, Ionic.Zip.ExtractExistingFileAction eOverwriteBehavior)
        {

            m_MostRecentZipFilePath = string.Copy(GZipFilePath);
            m_MostRecentUnzippedFiles.Clear();

            try
            {
                var fiFile = new FileInfo(GZipFilePath);

                if (!fiFile.Exists)
                {
                    LogError("GZip file not found: " + fiFile.FullName);
                    return false;
                }

                if (fiFile.Extension.ToLower() != ".gz")
                {
                    LogError("Not a GZipped file; must have extension .gz: " + fiFile.FullName);
                    return false;
                }

                if (m_DebugLevel >= 3)
                {
                    OnStatusEvent("Unzipping file: " + fiFile.FullName);
                }

                var dtStartTime = DateTime.UtcNow;

                // Get original file extension, for example "doc" from report.doc.gz
                var curFile = fiFile.Name;
                var decompressedFilePath = Path.Combine(TargetDirectory, curFile.Remove(curFile.Length - fiFile.Extension.Length));

                var fiDecompressedFile = new FileInfo(decompressedFilePath);

                if (fiDecompressedFile.Exists)
                {
                    if (eOverwriteBehavior == Ionic.Zip.ExtractExistingFileAction.DoNotOverwrite)
                    {
                        m_Message = "Decompressed file already exists; will not overwrite: " + fiDecompressedFile.FullName;
                        OnStatusEvent(m_Message);
                        return true;
                    }

                    if (eOverwriteBehavior == Ionic.Zip.ExtractExistingFileAction.Throw)
                    {
                        throw new Exception("Decompressed file already exists: " + fiDecompressedFile.FullName);
                    }
                }
                else
                {
                    // Make sure the target directory exists
                    if (fiDecompressedFile.Directory != null && !fiDecompressedFile.Directory.Exists)
                        fiDecompressedFile.Directory.Create();
                }

                using (var inFile = fiFile.OpenRead())
                {

                    // Create the decompressed file.
                    using (var outFile = File.Create(decompressedFilePath))
                    {
                        using (var decompress = new Ionic.Zlib.GZipStream(inFile, Ionic.Zlib.CompressionMode.Decompress))
                        {

                            // Copy the decompression stream into the output file.
                            decompress.CopyTo(outFile);
                            m_MostRecentUnzippedFiles.Add(new KeyValuePair<string, string>(fiDecompressedFile.Name, fiDecompressedFile.FullName));
                        }
                    }
                }

                var dtEndTime = DateTime.UtcNow;

                if (m_DebugLevel >= 2)
                {
                    ReportZipStats(fiFile, dtStartTime, dtEndTime, false);
                }

                // Update the file modification time of the decompressed file
                fiDecompressedFile.Refresh();
                if (fiDecompressedFile.LastWriteTimeUtc > fiFile.LastWriteTimeUtc)
                {
                    fiDecompressedFile.LastWriteTimeUtc = fiFile.LastWriteTimeUtc;
                }

                // Call the garbage collector to assure the handle to the .gz file is released
                PRISM.clsProgRunner.GarbageCollectNow();

            }
            catch (Exception ex)
            {
                LogError("Error unzipping .gz file " + GZipFilePath, ex);
                return false;
            }

            return true;

        }

        /// <summary>
        /// Stores SourceFilePath in a zip file with the same name, but with extension .gz appended to the name (e.g. Dataset.mzid.gz)
        /// </summary>
        /// <param name="SourceFilePath">Full path to the file to be zipped</param>
        /// <param name="DeleteSourceAfterZip">If True, then will delete the source file after zipping it</param>
        /// <returns>True if success; false if an error</returns>
        public bool GZipFile(string SourceFilePath, bool DeleteSourceAfterZip)
        {
            var fiFile = new FileInfo(SourceFilePath);
            return GZipFile(SourceFilePath, fiFile.DirectoryName, DeleteSourceAfterZip);
        }

        /// <summary>
        /// Stores SourceFilePath in a zip file with the same name, but with extension .gz appended to the name (e.g. Dataset.mzid.gz)
        /// </summary>
        /// <param name="SourceFilePath">Full path to the file to be zipped</param>
        /// <param name="TargetFolderPath">Target directory to create the .gz file</param>
        /// <param name="DeleteSourceAfterZip">If True, then will delete the source file after zipping it</param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks>Preferably uses the external gzip.exe software, since that software properly stores the original filename and date in the .gz file</remarks>
        public bool GZipFile(string SourceFilePath, string TargetFolderPath, bool DeleteSourceAfterZip)
        {

            var fiFile = new FileInfo(SourceFilePath);

            var GZipFilePath = Path.Combine(TargetFolderPath, fiFile.Name + ".gz");

            m_Message = string.Empty;
            m_MostRecentZipFilePath = string.Copy(GZipFilePath);

            try
            {

                if (File.Exists(GZipFilePath))
                {
                    if (m_DebugLevel >= 3)
                    {
                        OnStatusEvent("Deleting target .gz file: " + GZipFilePath);
                    }

                    File.Delete(GZipFilePath);
                    Thread.Sleep(250);

                }
            }
            catch (Exception ex)
            {
                LogError("Error deleting target .gz file prior to zipping " + SourceFilePath, ex);
                return false;
            }

            // Look for gzip.exe
            var fiGZip = new FileInfo(Path.Combine(clsGlobal.GetAppFolderPath(), "gzip.exe"));
            bool success;

            if (fiGZip.Exists)
            {
                success = GZipUsingExe(fiFile, GZipFilePath, fiGZip);
            }
            else
            {
                success = GZipUsingIonicZip(fiFile, GZipFilePath);
            }

            if (!success)
            {
                return false;
            }

            // Call the garbage collector to assure the handle to the .gz file is released
            PRISM.clsProgRunner.GarbageCollectNow();

            if (DeleteSourceAfterZip)
            {
                DeleteFile(fiFile);
            }

            return true;

        }

        /// <summary>
        /// Compress a file using the external GZip.exe software
        /// </summary>
        /// <param name="fiFile">File to compress</param>
        /// <param name="GZipFilePath">Full path to the .gz file to be created</param>
        /// <param name="fiGZip">GZip.exe fileinfo object</param>
        /// <returns></returns>
        /// <remarks>The .gz file will initially be created in the same folder as the original file.  If GZipFilePath points to a different folder, then the file will be moved to that new location</remarks>
        private bool GZipUsingExe(FileInfo fiFile, string GZipFilePath, FileInfo fiGZip)
        {

            try
            {
                if (m_DebugLevel >= 3)
                {
                    OnStatusEvent("Creating .gz file using " + fiGZip.Name + ": " + GZipFilePath);
                }

                var dtStartTime = DateTime.UtcNow;

                var strArgs = "-f -k " + clsGlobal.PossiblyQuotePath(fiFile.FullName);

                if (m_DebugLevel >= 3)
                {
                    OnStatusEvent(fiGZip.FullName + " " + strArgs);
                }

                var progRunner = new clsRunDosProgram(clsGlobal.GetAppFolderPath())
                {
                    CacheStandardOutput = false,
                    CreateNoWindow = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = false,
                    DebugLevel = 1,
                    MonitorInterval = 250
                };
                RegisterEvents(progRunner);

                var success = progRunner.RunProgram(fiGZip.FullName, strArgs, "GZip", false);

                if (!success)
                {
                    LogError("GZip.exe reported an error code");
                    return false;
                }

                Thread.Sleep(100);

                var dtEndTime = DateTime.UtcNow;

                // Confirm that the .gz file was created

                var fiCompressedFile = new FileInfo(fiFile.FullName + ".gz");
                if (!fiCompressedFile.Exists)
                {
                    LogError("GZip.exe did not create a .gz file: " + fiCompressedFile.FullName);
                    return false;
                }

                if (m_DebugLevel >= 2)
                {
                    ReportZipStats(fiFile, dtStartTime, dtEndTime, true);
                }

                var fiCompressedFileFinal = new FileInfo(GZipFilePath);


                if (!clsGlobal.IsMatch(fiCompressedFile.FullName, fiCompressedFileFinal.FullName))
                {
                    if (fiCompressedFileFinal.Exists)
                    {
                        fiCompressedFileFinal.Delete();
                    }
                    else
                    {
                        if (fiCompressedFileFinal.Directory != null && !fiCompressedFileFinal.Directory.Exists)
                            fiCompressedFileFinal.Directory.Create();
                    }

                    fiCompressedFile.MoveTo(fiCompressedFileFinal.FullName);
                }

            }
            catch (Exception ex)
            {
                LogError("Error gzipping file " + fiFile.FullName + " using gzip.exe", ex);
                return false;
            }

            return true;

        }

        /// <summary>
        /// Compress the file using IonicZip
        /// </summary>
        /// <param name="fiFile"></param>
        /// <param name="GZipFilePath"></param>
        /// <returns></returns>
        /// <remarks>IonicZip creates a valid .gz file, but it does not include the header information (filename and timestamp of the original file)</remarks>
        private bool GZipUsingIonicZip(FileInfo fiFile, string GZipFilePath)
        {
            try
            {
                if (m_DebugLevel >= 3)
                {
                    OnStatusEvent("Creating .gz file using IonicZip: " + GZipFilePath);
                }

                var dtStartTime = DateTime.UtcNow;

                using (Stream inFile = fiFile.OpenRead())
                {
                    using (var outFile = File.Create(GZipFilePath))
                    {
                        using (var gzippedStream = new Ionic.Zlib.GZipStream(outFile, Ionic.Zlib.CompressionMode.Compress))
                        {

                            inFile.CopyTo(gzippedStream);

                        }
                    }
                }

                var dtEndTime = DateTime.UtcNow;

                if (m_DebugLevel >= 2)
                {
                    ReportZipStats(fiFile, dtStartTime, dtEndTime, true);
                }

                // Update the file modification time of the .gz file to use the modification time of the original file
                var fiGZippedFile = new FileInfo(GZipFilePath);

                if (!fiGZippedFile.Exists)
                {
                    LogError("IonicZip did not create a .gz file: " + GZipFilePath);
                    return false;
                }

                fiGZippedFile.LastWriteTimeUtc = fiFile.LastWriteTimeUtc;

            }
            catch (Exception ex)
            {
                LogError("Error gzipping file " + fiFile.FullName, ex);
                return false;
            }

            return true;

        }

        private void LogError(string errorMessage)
        {
            m_Message = errorMessage;
            OnErrorEvent(errorMessage);
        }

        private void LogError(string errorMessage, Exception ex)
        {
            m_Message = errorMessage;
            OnErrorEvent(errorMessage, ex);
        }

        private void ReportZipStats(FileSystemInfo fiFileSystemInfo, DateTime dtStartTime, DateTime dtEndTime, bool FileWasZipped)
        {
            ReportZipStats(fiFileSystemInfo, dtStartTime, dtEndTime, FileWasZipped, IONIC_ZIP_NAME);

        }

        public void ReportZipStats(FileSystemInfo fiFileSystemInfo, DateTime dtStartTime, DateTime dtEndTime, bool FileWasZipped, string ZipProgramName)
        {

            long lngTotalSizeBytes = 0;
            double dblUnzipSpeedMBPerSec;

            if (ZipProgramName == null)
                ZipProgramName = "??";

            var dblUnzipTimeSeconds = dtEndTime.Subtract(dtStartTime).TotalSeconds;

            if (fiFileSystemInfo is FileInfo)
            {
                lngTotalSizeBytes = ((FileInfo)fiFileSystemInfo).Length;

            }
            else if (fiFileSystemInfo is DirectoryInfo)
            {
                var diFolderInfo = (DirectoryInfo)fiFileSystemInfo;

                lngTotalSizeBytes = 0;
                foreach (var fiEntry in diFolderInfo.GetFiles("*", SearchOption.AllDirectories))
                {
                    lngTotalSizeBytes += fiEntry.Length;
                }
            }

            if (dblUnzipTimeSeconds > 0)
            {
                dblUnzipSpeedMBPerSec = clsGlobal.BytesToMB(lngTotalSizeBytes) / dblUnzipTimeSeconds;
            }
            else
            {
                dblUnzipSpeedMBPerSec = 0;
            }

            string zipAction;
            if (FileWasZipped)
            {
                zipAction = "Zipped ";
            }
            else
            {
                zipAction = "Unzipped ";
            }

            m_Message = zipAction + fiFileSystemInfo.Name + " using " + ZipProgramName + "; " +
                "elapsed time = " + dblUnzipTimeSeconds.ToString("0.0") + " seconds; " +
                "rate = " + dblUnzipSpeedMBPerSec.ToString("0.0") + " MB/sec";

            if (m_DebugLevel >= 2)
            {
                OnStatusEvent(m_Message);
            }

        }

        /// <summary>
        /// Unzip ZipFilePath into the working directory defined when this class was instantiated
        /// Existing files will be overwritten
        /// </summary>
        /// <param name="ZipFilePath">File to unzip</param>
        /// <returns>True if success; false if an error</returns>
        public bool UnzipFile(string ZipFilePath)
        {
            return UnzipFile(ZipFilePath, m_WorkDir);
        }

        /// <summary>
        /// Unzip ZipFilePath into the specified target directory
        /// Existing files will be overwritten
        /// </summary>
        /// <param name="ZipFilePath">File to unzip</param>
        /// <param name="TargetDirectory">Folder to place the unzipped files</param>
        /// <returns>True if success; false if an error</returns>
        public bool UnzipFile(string ZipFilePath, string TargetDirectory)
        {
            return UnzipFile(ZipFilePath, TargetDirectory, string.Empty, Ionic.Zip.ExtractExistingFileAction.OverwriteSilently);
        }

        /// <summary>
        /// Unzip ZipFilePath into the specified target directory, applying the specified file filter
        /// Existing files will be overwritten
        /// </summary>
        /// <param name="ZipFilePath">File to unzip</param>
        /// <param name="TargetDirectory">Folder to place the unzipped files</param>
        /// <param name="FileFilter">Filter to apply when unzipping</param>
        /// <returns>True if success; false if an error</returns>
        public bool UnzipFile(string ZipFilePath, string TargetDirectory, string FileFilter)
        {

            return UnzipFile(ZipFilePath, TargetDirectory, FileFilter, Ionic.Zip.ExtractExistingFileAction.OverwriteSilently);
        }


        /// <summary>
        /// Unzip ZipFilePath into the specified target directory, applying the specified file filter
        /// </summary>
        /// <param name="ZipFilePath">File to unzip</param>
        /// <param name="TargetDirectory">Folder to place the unzipped files</param>
        /// <param name="FileFilter">Filter to apply when unzipping</param>
        /// <param name="eOverwriteBehavior">Defines what to do when existing files could be ovewritten</param>
        /// <returns>True if success; false if an error</returns>
        public bool UnzipFile(string ZipFilePath, string TargetDirectory, string FileFilter, Ionic.Zip.ExtractExistingFileAction eOverwriteBehavior)
        {

            m_Message = string.Empty;
            m_MostRecentZipFilePath = string.Copy(ZipFilePath);
            m_MostRecentUnzippedFiles.Clear();

            try
            {
                var fiFile = new FileInfo(ZipFilePath);

                if (!File.Exists(ZipFilePath))
                {
                    LogError("Zip file not found: " + fiFile.FullName);
                    return false;
                }

                if (m_DebugLevel >= 3)
                {
                    OnStatusEvent("Unzipping file: " + fiFile.FullName);
                }

                using (var zipper = new Ionic.Zip.ZipFile(ZipFilePath))
                {

                    var dtStartTime = DateTime.UtcNow;

                    if (string.IsNullOrEmpty(FileFilter))
                    {
                        zipper.ExtractAll(TargetDirectory, eOverwriteBehavior);

                        foreach (var objItem in zipper.Entries)
                        {
                            if (!objItem.IsDirectory)
                            {
                                // Note that objItem.FileName contains the relative path of the file, for example "Filename.txt" or "Subfolder/Filename.txt"
                                var fiUnzippedItem = new FileInfo(Path.Combine(TargetDirectory, objItem.FileName.Replace('/', Path.DirectorySeparatorChar)));
                                m_MostRecentUnzippedFiles.Add(new KeyValuePair<string, string>(fiUnzippedItem.Name, fiUnzippedItem.FullName));
                            }
                        }
                    }
                    else
                    {
                        var objEntries = zipper.SelectEntries(FileFilter);

                        foreach (var objItem in objEntries)
                        {
                            objItem.Extract(TargetDirectory, eOverwriteBehavior);
                            if (!objItem.IsDirectory)
                            {
                                // Note that objItem.FileName contains the relative path of the file, for example "Filename.txt" or "Subfolder/Filename.txt"
                                var fiUnzippedItem = new FileInfo(Path.Combine(TargetDirectory, objItem.FileName.Replace('/', Path.DirectorySeparatorChar)));
                                m_MostRecentUnzippedFiles.Add(new KeyValuePair<string, string>(fiUnzippedItem.Name, fiUnzippedItem.FullName));
                            }
                        }
                    }

                    var dtEndTime = DateTime.UtcNow;

                    if (m_DebugLevel >= 2)
                    {
                        ReportZipStats(fiFile, dtStartTime, dtEndTime, false);
                    }

                }

            }
            catch (Exception ex)
            {
                LogError("Error unzipping file " + ZipFilePath, ex);
                return false;
            }

            return true;

        }

        /// <summary>
        /// Verifies that the zip file exists and is not corrupt
        /// If the file size is less than 4 GB, then also performs a full CRC check of the data
        /// </summary>
        /// <param name="zipFilePath">Zip file to check</param>
        /// <returns>True if a valid zip file, otherwise false</returns>
        public bool VerifyZipFile(string zipFilePath)
        {
            return VerifyZipFile(zipFilePath, crcCheckThresholdGB: 4);
        }

        /// <summary>
        /// Verifies that the zip file exists.  
        /// If the file size is less than crcCheckThresholdGB, then also performs a full CRC check of the data
        /// </summary>
        /// <param name="zipFilePath">Zip file to check</param>
        /// <param name="crcCheckThresholdGB">Threshold (in GB) below which a full CRC check should be performed</param>
        /// <returns>True if a valid zip file, otherwise false</returns>
        public bool VerifyZipFile(string zipFilePath, float crcCheckThresholdGB)
        {

            try
            {
                // Wait 150 msec
                Thread.Sleep(150);

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
                    if (string.IsNullOrEmpty(m_Message))
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

                using (var zipper = new Ionic.Zip.ZipFile(zipFilePath))
                {

                    var objEntries = zipper.SelectEntries("*");

                    foreach (var entry in objEntries)
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
                } while ((n > 0));

                if (srReader.Crc != entry.Crc)
                {
                    m_Message = string.Format("Zip entry " + entry.FileName + " failed the CRC Check in " + zipFilePath +
                                              " (0x{0:X8} != 0x{1:X8})", srReader.Crc, entry.Crc);
                    OnWarningEvent(m_Message);
                    return false;
                }

                if ((totalBytesRead != entry.UncompressedSize))
                {
                    m_Message = string.Format("Unexpected number of bytes for entry " + entry.FileName + " in " + zipFilePath +
                                              " ({0} != {1})", totalBytesRead, entry.UncompressedSize);
                    OnWarningEvent(m_Message);
                    return false;
                }

            }

            return true;
        }

        /// <summary>
        /// Stores SourceFilePath in a zip file with the same name, but extension .zip
        /// </summary>
        /// <param name="sourceFilePath">Full path to the file to be zipped</param>
        /// <param name="deleteSourceAfterZip">If True, then will delete the source file after zipping it</param>
        /// <returns>True if success; false if an error</returns>
        public bool ZipFile(string sourceFilePath, bool deleteSourceAfterZip)
        {

            var zipFilePath = GetZipFilePathForFile(sourceFilePath);

            return ZipFile(sourceFilePath, deleteSourceAfterZip, zipFilePath);

        }

        /// <summary>
        /// Stores SourceFilePath in a zip file named ZipFilePath
        /// </summary>
        /// <param name="SourceFilePath">Full path to the file to be zipped</param>
        /// <param name="DeleteSourceAfterZip">If True, then will delete the source file after zipping it</param>
        /// <param name="ZipFilePath">Full path to the .zip file to be created.  Existing files will be overwritten</param>
        /// <returns>True if success; false if an error</returns>
        public bool ZipFile(string SourceFilePath, bool DeleteSourceAfterZip, string ZipFilePath)
        {

            var fiFile = new FileInfo(SourceFilePath);

            m_Message = string.Empty;
            m_MostRecentZipFilePath = string.Copy(ZipFilePath);

            try
            {

                if (File.Exists(ZipFilePath))
                {
                    if (m_DebugLevel >= 3)
                    {
                        OnStatusEvent("Deleting target .zip file: " + ZipFilePath);
                    }

                    File.Delete(ZipFilePath);
                    Thread.Sleep(250);

                }
            }
            catch (Exception ex)
            {
                LogError("Error deleting target .zip file prior to zipping file " + SourceFilePath + " using IonicZip", ex);
                return false;
            }

            try
            {
                if (m_DebugLevel >= 3)
                {
                    OnStatusEvent("Creating .zip file: " + ZipFilePath);
                }

                using (var zipper = new Ionic.Zip.ZipFile(ZipFilePath))
                {
                    zipper.UseZip64WhenSaving = Ionic.Zip.Zip64Option.AsNecessary;

                    var dtStartTime = DateTime.UtcNow;
                    zipper.AddItem(fiFile.FullName, string.Empty);
                    zipper.Save();
                    var dtEndTime = DateTime.UtcNow;

                    if (m_DebugLevel >= 2)
                    {
                        ReportZipStats(fiFile, dtStartTime, dtEndTime, true);
                    }

                }

            }
            catch (Exception ex)
            {
                LogError("Error zipping file " + fiFile.FullName, ex);
                return false;
            }

            // Verify that the zip file is not corrupt
            // Files less than 4 GB get a full CRC check
            // Large files get a quick check
            if (!VerifyZipFile(ZipFilePath))
            {
                return false;
            }

            if (DeleteSourceAfterZip)
            {
                DeleteFile(fiFile);
            }

            return true;

        }

        public bool ZipDirectory(string SourceDirectoryPath, string ZipFilePath)
        {

            return ZipDirectory(SourceDirectoryPath, ZipFilePath, true, string.Empty);

        }

        public bool ZipDirectory(string SourceDirectoryPath, string ZipFilePath, bool Recurse)
        {

            return ZipDirectory(SourceDirectoryPath, ZipFilePath, Recurse, string.Empty);

        }

        /// <summary>
        /// Stores all files in a source directory into a zip file named ZipFilePath
        /// </summary>
        /// <param name="SourceDirectoryPath">Full path to the directory to be zipped</param>    
        /// <param name="ZipFilePath">Full path to the .zip file to be created.  Existing files will be overwritten</param>
        /// <param name="Recurse">If True, then recurse through all subfolders</param>
        /// <param name="FileFilter">Filter to apply when zipping</param>
        /// <returns>True if success; false if an error</returns>
        public bool ZipDirectory(string SourceDirectoryPath, string ZipFilePath, bool Recurse, string FileFilter)
        {

            var diDirectory = new DirectoryInfo(SourceDirectoryPath);

            m_Message = string.Empty;
            m_MostRecentZipFilePath = string.Copy(ZipFilePath);

            try
            {
                if (File.Exists(ZipFilePath))
                {
                    if (m_DebugLevel >= 3)
                    {
                        OnStatusEvent("Deleting target .zip file: " + ZipFilePath);
                    }

                    File.Delete(ZipFilePath);
                    Thread.Sleep(250);
                }
            }
            catch (Exception ex)
            {
                LogError("Error deleting target .zip file prior to zipping folder " + SourceDirectoryPath + " using IonicZip", ex);
                return false;
            }

            try
            {
                if (m_DebugLevel >= 3)
                {
                    OnStatusEvent("Creating .zip file: " + ZipFilePath);
                }

                using (var zipper = new Ionic.Zip.ZipFile(ZipFilePath))
                {
                    zipper.UseZip64WhenSaving = Ionic.Zip.Zip64Option.AsNecessary;

                    var dtStartTime = DateTime.UtcNow;

                    if (string.IsNullOrEmpty(FileFilter) && Recurse)
                    {
                        zipper.AddDirectory(diDirectory.FullName);
                    }
                    else
                    {
                        if (string.IsNullOrEmpty(FileFilter))
                        {
                            FileFilter = "*";
                        }

                        zipper.AddSelectedFiles(FileFilter, diDirectory.FullName, string.Empty, Recurse);
                    }

                    zipper.Save();

                    var dtEndTime = DateTime.UtcNow;

                    if (m_DebugLevel >= 2)
                    {
                        ReportZipStats(diDirectory, dtStartTime, dtEndTime, true);
                    }

                }

            }
            catch (Exception ex)
            {
                LogError("Error zipping directory " + diDirectory.FullName, ex);
                return false;
            }

            // Verify that the zip file is not corrupt
            // Files less than 4 GB get a full CRC check
            // Large files get a quick check
            if (!VerifyZipFile(ZipFilePath))
            {
                return false;
            }

            return true;

        }
        
    }

}