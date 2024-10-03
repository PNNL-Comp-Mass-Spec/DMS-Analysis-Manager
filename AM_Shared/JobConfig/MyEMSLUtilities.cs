using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using AnalysisManagerBase.FileAndDirectoryTools;
using MyEMSLReader;
using PRISM;

namespace AnalysisManagerBase.JobConfig
{
    /// <summary>
    /// MyEMSL Utilities
    /// </summary>
    public class MyEMSLUtilities : EventNotifier
    {
        // Ignore Spelling: downloader, emsl, myemsl, svc-dms, yyyy-MM-dd hh:mm tt, dest

        /// <summary>
        /// MyEMSL path flag
        /// </summary>
        public const string MYEMSL_PATH_FLAG = @"\\MyEMSL";

        private const string DATETIME_FORMAT_NO_SECONDS = "yyyy-MM-dd hh:mm tt";

        private readonly ZipFileTools mZipTools;

        private readonly DatasetListInfo mMyEMSLDatasetListInfo;

        private List<DatasetDirectoryOrFileInfo> mRecentlyFoundMyEMSLFiles;

        private DateTime mLastMyEMSLProgressWriteTime = DateTime.UtcNow;

        private DateTime mLastDisableNotify = DateTime.MinValue;

        private bool mMyEMSLAutoDisabled;
        private DateTime mMyEMSLReEnableTime = DateTime.MinValue;

        private int mMyEMSLConnectionErrorCount;
        private int mMyEMSLDisableCount;

        private readonly MyEMSLFileIDComparer mFileIDComparer;

        /// <summary>
        /// File downloaded event
        /// </summary>
        public event EventHandler<FileDownloadedEventArgs> FileDownloaded;

        /// <summary>
        /// The most recently downloaded files; keys are the full paths to the downloaded file, values are extended file info
        /// </summary>
        public Dictionary<string, ArchivedFileInfo> DownloadedFiles => mMyEMSLDatasetListInfo.DownloadedFiles;

        /// <summary>
        /// MyEMSL IDs of files queued to be downloaded
        /// </summary>
        public Dictionary<long, DownloadQueue.FileDownloadInfo> FilesToDownload => mMyEMSLDatasetListInfo.FilesToDownload;

        /// <summary>
        /// All files found in MyEMSL via calls to FindFiles
        /// </summary>
        public List<DatasetDirectoryOrFileInfo> AllFoundMyEMSLFiles { get; }

        /// <summary>
        /// Returns the files most recently unzipped
        /// Keys in the KeyValuePairs are filenames while values are relative paths (in case the .zip file has directories)
        /// </summary>
        public List<KeyValuePair<string, string>> MostRecentUnzippedFiles { get; }

        /// <summary>
        /// Files most recently found via a call to FindFiles
        /// </summary>
        public List<DatasetDirectoryOrFileInfo> RecentlyFoundMyEMSLFiles => mRecentlyFoundMyEMSLFiles;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="debugLevel">Debug level for logging; 1=minimal logging; 5=detailed logging</param>
        /// <param name="workingDir">Working directory path</param>
        /// <param name="traceMode">Set to true to show additional debug messages</param>
        public MyEMSLUtilities(int debugLevel, string workingDir, bool traceMode = false)
        {
            mMyEMSLDatasetListInfo = new DatasetListInfo
            {
                ReportMetadataURLs = traceMode || debugLevel >= 2,
                ThrowErrors = true,
                TraceMode = traceMode
            };

            RegisterEvents(mMyEMSLDatasetListInfo);

            // Use a custom progress update handler
            mMyEMSLDatasetListInfo.ProgressUpdate -= OnProgressUpdate;
            mMyEMSLDatasetListInfo.ProgressUpdate += MyEMSLDatasetListInfo_ProgressEvent;

            mMyEMSLDatasetListInfo.FileDownloadedEvent += MyEMSLDatasetListInfo_FileDownloadedEvent;

            // Watch for error message "Unable to connect to the remote server"
            mMyEMSLDatasetListInfo.MyEMSLOffline += MyEMSLDatasetListInfo_MyEMSLOffline;

            AllFoundMyEMSLFiles = new List<DatasetDirectoryOrFileInfo>();
            mRecentlyFoundMyEMSLFiles = new List<DatasetDirectoryOrFileInfo>();

            mZipTools = new ZipFileTools(debugLevel, workingDir);
            RegisterEvents(mZipTools);

            mFileIDComparer = new MyEMSLFileIDComparer();

            MostRecentUnzippedFiles = new List<KeyValuePair<string, string>>();

            mMyEMSLAutoDisabled = false;
        }

        // ReSharper disable once CommentTypo

        /// <summary>
        /// Append a file to a directory path that ends with @MyEMSLID_12345
        /// </summary>
        /// <param name="myEmslDirectoryPath">Directory path to which fileName will be appended</param>
        /// <param name="fileName">Filename to append</param>
        public static string AddFileToMyEMSLDirectoryPath(string myEmslDirectoryPath, string fileName)
        {
            var myEMSLFileID = DatasetInfoBase.ExtractMyEMSLFileID(myEmslDirectoryPath, out var directoryPathClean);

            var filePath = Path.Combine(directoryPathClean, fileName);

            if (myEMSLFileID == 0)
            {
                return filePath;
            }

            return DatasetInfoBase.AppendMyEMSLFileID(filePath, myEMSLFileID);
        }

        /// <summary>
        /// Make sure that the MyEMSL DatasetListInfo class knows to search for the given dataset
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        public void AddDataset(string datasetName)
        {
            if (!mMyEMSLDatasetListInfo.ContainsDataset(datasetName))
            {
                mMyEMSLDatasetListInfo.AddDataset(datasetName);
            }
        }

        /// <summary>
        /// Queue a file to be downloaded
        /// </summary>
        /// <param name="fileInfo">Archive File Info</param>
        public void AddFileToDownloadQueue(ArchivedFileInfo fileInfo)
        {
            mMyEMSLDatasetListInfo.AddFileToDownloadQueue(fileInfo);
        }

        // ReSharper disable once CommentTypo

        /// <summary>
        /// Queue a file to be downloaded
        /// </summary>
        /// <param name="encodedFilePath">File path that includes @MyEMSLID_12345</param>
        /// <param name="unzipRequired">True if the file should be unzipped</param>
        public bool AddFileToDownloadQueue(string encodedFilePath, bool unzipRequired = false)
        {
            var myEMSLFileID = DatasetInfoBase.ExtractMyEMSLFileID(encodedFilePath);

            if (myEMSLFileID <= 0)
            {
                OnErrorEvent("MyEMSL File ID not found in path: " + encodedFilePath);
                return false;
            }

            if (!GetCachedArchivedFileInfo(myEMSLFileID, out var matchingFileInfo))
            {
                // File not found in mRecentlyFoundMyEMSLFiles
                // Instead check mAllFoundMyEMSLFiles

                var fileInfoQuery = (from item in AllFoundMyEMSLFiles where item.FileID == myEMSLFileID select item.FileInfo).ToList();

                if (fileInfoQuery.Count == 0)
                {
                    OnErrorEvent("Cached ArchiveFileInfo does not contain MyEMSL File ID " + myEMSLFileID);
                    return false;
                }

                matchingFileInfo = fileInfoQuery.First();
            }

            AddDataset(matchingFileInfo.Dataset);
            mMyEMSLDatasetListInfo.AddFileToDownloadQueue(matchingFileInfo, unzipRequired);
            return true;
        }

        /// <summary>
        /// Verify that svc-dms.pfx exists either in the same directory as Pacifica.core.dll, at C:\DMS_Programs\client_certs, or at C:\client_certs\
        /// </summary>
        /// <param name="errorMessage">Output: error message, indicating the paths that were checked</param>
        /// <returns>True if the file is found, otherwise false</returns>
        public bool CertificateFileExists(out string errorMessage)
        {
            return mMyEMSLDatasetListInfo.CertificateFileExists(out errorMessage);
        }

        /// <summary>
        /// Clear the list of MyEMSL files found via calls to FindFiles
        /// </summary>
        public void ClearAllFoundFiles()
        {
            AllFoundMyEMSLFiles.Clear();
        }

        /// <summary>
        /// Clear the queue of files to download
        /// </summary>
        public void ClearDownloadQueue()
        {
            mMyEMSLDatasetListInfo.FilesToDownload.Clear();
        }

        /// <summary>
        /// Look for the given file (optionally in a given subdirectory) for the given dataset
        /// </summary>
        /// <remarks>subdirectoryName can contain a partial path, for example 2013_09_10_DPB_Unwashed_Media_25um.d\2013_09_10_In_1sec_1MW.m</remarks>
        /// <param name="fileName">File name to find; can contain a wildcard, e.g. *.zip</param>
        /// <param name="subdirectoryName">Directory in which the file must reside; can contain a wildcard, e.g. SIC*</param>
        /// <param name="datasetName">Dataset name filter</param>
        /// <param name="recurse">True to search all directories; false to only search the root directory (or only subdirectoryName)</param>
        /// <returns>List of matching files</returns>
        public List<DatasetDirectoryOrFileInfo> FindFiles(string fileName, string subdirectoryName, string datasetName, bool recurse)
        {
            // Make sure the dataset name is being tracked by mMyEMSLDatasetListInfo
            AddDataset(datasetName);

            if (mMyEMSLAutoDisabled)
            {
                if (DateTime.UtcNow > mMyEMSLReEnableTime)
                {
                    mMyEMSLAutoDisabled = false;
                    OnStatusEvent("Re-enabling MyEMSL querying");
                }
                else
                {
                    if (DateTime.UtcNow.Subtract(mLastDisableNotify).TotalSeconds > 5)
                    {
                        mLastDisableNotify = DateTime.UtcNow;
                        OnDebugEvent("MyEMSL querying is currently disabled until " +
                                     mMyEMSLReEnableTime.ToLocalTime().ToString(DATETIME_FORMAT_NO_SECONDS));
                    }

                    if (mRecentlyFoundMyEMSLFiles == null)
                        mRecentlyFoundMyEMSLFiles = new List<DatasetDirectoryOrFileInfo>();
                    else
                        mRecentlyFoundMyEMSLFiles.Clear();

                    return mRecentlyFoundMyEMSLFiles;
                }
            }

            var fileIDList = string.Empty;

            mRecentlyFoundMyEMSLFiles = mMyEMSLDatasetListInfo.FindFiles(fileName, subdirectoryName, datasetName, fileIDList, recurse);

            if (!mMyEMSLAutoDisabled)
            {
                mMyEMSLConnectionErrorCount = 0;
                mMyEMSLDisableCount = 0;
            }

            var filesToAdd = mRecentlyFoundMyEMSLFiles.Except(AllFoundMyEMSLFiles, mFileIDComparer);

            AllFoundMyEMSLFiles.AddRange(filesToAdd);

            return mRecentlyFoundMyEMSLFiles;
        }

        private bool GetCachedArchivedFileInfo(long myEMSLFileID, out ArchivedFileInfo matchingFileInfo)
        {
            matchingFileInfo = null;

            var fileInfoQuery = (from item in mRecentlyFoundMyEMSLFiles where item.FileID == myEMSLFileID select item.FileInfo).ToList();

            if (fileInfoQuery.Count == 0)
            {
                return false;
            }

            matchingFileInfo = fileInfoQuery.First();
            return true;
        }

        /// <summary>
        /// Retrieve queued files from MyEMSL
        /// </summary>
        /// <remarks>Returns true if the download queue is empty</remarks>
        /// <param name="downloadDirectoryPath">Target directory path (ignored for files defined in destinationFilePathOverride)</param>
        /// <param name="directoryLayout">Directory Layout (ignored for files defined in destinationFilePathOverride)</param>
        /// <returns>True if success, false if an error</returns>
        public bool ProcessMyEMSLDownloadQueue(string downloadDirectoryPath, Downloader.DownloadLayout directoryLayout)
        {
            if (mMyEMSLDatasetListInfo.FilesToDownload.Count == 0)
            {
                // Nothing to download; that's OK
                return true;
            }

            MostRecentUnzippedFiles.Clear();

            var success = mMyEMSLDatasetListInfo.ProcessDownloadQueue(downloadDirectoryPath, directoryLayout);

            if (success)
                return true;

            if (mMyEMSLDatasetListInfo.ErrorMessages.Count > 0)
            {
                OnErrorEvent("Error in ProcessMyEMSLDownloadQueue: " + mMyEMSLDatasetListInfo.ErrorMessages.First());
            }
            else
            {
                OnErrorEvent("Unknown error in ProcessMyEMSLDownloadQueue");
            }

            return false;
        }

        private void MyEMSLDatasetListInfo_MyEMSLOffline(string message)
        {
            mMyEMSLConnectionErrorCount++;

            if (mMyEMSLConnectionErrorCount < 3)
                return;

            // Disable contacting MyEMSL for the next 15 minutes (or longer if mMyEMSLDisableCount is > 1)
            mMyEMSLAutoDisabled = true;

            mMyEMSLDisableCount++;
            mMyEMSLReEnableTime = DateTime.UtcNow.AddMinutes(15 * mMyEMSLDisableCount);
            OnWarningEvent("Disabling MyEMSL until " + mMyEMSLReEnableTime.ToLocalTime().ToString(DATETIME_FORMAT_NO_SECONDS));
        }

        private void MyEMSLDatasetListInfo_ProgressEvent(string progressMessage, float percentComplete)
        {
            if (DateTime.UtcNow.Subtract(mLastMyEMSLProgressWriteTime).TotalMinutes > 0.2)
            {
                mLastMyEMSLProgressWriteTime = DateTime.UtcNow;
                OnProgressUpdate("MyEMSL downloader: " + percentComplete + "% complete", percentComplete);
            }
        }

        private void MyEMSLDatasetListInfo_FileDownloadedEvent(object sender, FileDownloadedEventArgs e)
        {
            if (e.UnzipRequired)
            {
                var fileToUnzip = new FileInfo(Path.Combine(e.DownloadDirectoryPath, e.ArchivedFile.Filename));

                if (!fileToUnzip.Exists)
                    return;

                if (string.Equals(fileToUnzip.Extension, ".zip", StringComparison.OrdinalIgnoreCase))
                {
                    // Decompress the .zip file
                    OnStatusEvent("Unzipping file " + fileToUnzip.Name);
                    mZipTools.UnzipFile(fileToUnzip.FullName, e.DownloadDirectoryPath);
                    MostRecentUnzippedFiles.AddRange(mZipTools.MostRecentUnzippedFiles);
                }
                else if (string.Equals(fileToUnzip.Extension, ".gz", StringComparison.OrdinalIgnoreCase))
                {
                    // Decompress the .gz file
                    OnStatusEvent("Unzipping file " + fileToUnzip.Name);
                    mZipTools.GUnzipFile(fileToUnzip.FullName, e.DownloadDirectoryPath);
                    MostRecentUnzippedFiles.AddRange(mZipTools.MostRecentUnzippedFiles);
                }
            }

            FileDownloaded?.Invoke(sender, e);
        }

        /// <summary>
        /// Determines whether two DatasetDirectoryOrFileInfo instances refer to the same file in MyEMSL
        /// </summary>
        /// <remarks>Compares the value of FileID in the two instances</remarks>
        private class MyEMSLFileIDComparer : IEqualityComparer<DatasetDirectoryOrFileInfo>
        {
            private bool ItemsAreEqual(DatasetDirectoryOrFileInfo x, DatasetDirectoryOrFileInfo y)
            {
                return x.FileID == y.FileID;
            }

            bool IEqualityComparer<DatasetDirectoryOrFileInfo>.Equals(DatasetDirectoryOrFileInfo x, DatasetDirectoryOrFileInfo y)
            {
                return ItemsAreEqual(x, y);
            }

            private int GetHashCodeForItem(DatasetDirectoryOrFileInfo obj)
            {
                return obj.FileID.GetHashCode();
            }
            int IEqualityComparer<DatasetDirectoryOrFileInfo>.GetHashCode(DatasetDirectoryOrFileInfo obj)
            {
                return GetHashCodeForItem(obj);
            }
        }
    }
}
