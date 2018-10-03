
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using MyEMSLReader;
using PRISM;

namespace AnalysisManagerBase
{
    /// <summary>
    /// MyEMSL Utilities
    /// </summary>
    public class clsMyEMSLUtilities : EventNotifier
    {

        /// <summary>
        /// MyEMSL path flag
        /// </summary>
        public const string MYEMSL_PATH_FLAG = @"\\MyEMSL";

        private const string DATETIME_FORMAT_NO_SECONDS = "yyyy-MM-dd hh:mm tt";

        private readonly clsDotNetZipTools mDotNetZipTools;

        private readonly DatasetListInfo mMyEMSLDatasetListInfo;

        private readonly List<DatasetFolderOrFileInfo> mAllFoundMyEMSLFiles;

        private List<DatasetFolderOrFileInfo> mRecentlyFoundMyEMSLFiles;

        private DateTime mLastMyEMSLProgressWriteTime = DateTime.UtcNow;

        private DateTime mLastDisableNotify = DateTime.MinValue;

        private bool mMyEMSLAutoDisabled;
        private DateTime mMyEMSLReEnableTime = DateTime.MinValue;

        private int mMyEMSLConnectionErrorCount;
        private int mMyEMSLDisableCount;

        private readonly clsMyEMSLFileIDComparer mFileIDComparer;

        private readonly List<KeyValuePair<string, string>> mMostRecentUnzippedFiles;

        #region "Events"

        /// <summary>
        /// File downloaded event
        /// </summary>
        public event FileDownloadedEventHandler FileDownloaded;

        #endregion

        #region "Properties"

        /// <summary>
        /// The most recently downloaded files; keys are the full paths to the downloaded file, values are extended file info
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public Dictionary<string, ArchivedFileInfo> DownloadedFiles => mMyEMSLDatasetListInfo.DownloadedFiles;

        /// <summary>
        /// MyEMSL IDs of files queued to be downloaded
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public Dictionary<long, DownloadQueue.udtFileToDownload> FilesToDownload => mMyEMSLDatasetListInfo.FilesToDownload;

        /// <summary>
        /// All files found in MyEMSL via calls to FindFiles
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public List<DatasetFolderOrFileInfo> AllFoundMyEMSLFiles => mAllFoundMyEMSLFiles;

        /// <summary>
        /// Returns the files most recently unzipped
        /// Keys in the KeyValuePairs are filenames while values are relative paths (in case the .zip file has directories)
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public List<KeyValuePair<string, string>> MostRecentUnzippedFiles => mMostRecentUnzippedFiles;

        /// <summary>
        /// Files most recently found via a call to FindFiles
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public List<DatasetFolderOrFileInfo> RecentlyFoundMyEMSLFiles => mRecentlyFoundMyEMSLFiles;

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="debugLevel">Debug level (higher number means more messages)</param>
        /// <param name="workingDir">Working directory path</param>
        /// <param name="traceMode">Set to true to show additional debug messages</param>
        /// <remarks></remarks>
        public clsMyEMSLUtilities(int debugLevel, string workingDir, bool traceMode = false)
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

            mAllFoundMyEMSLFiles = new List<DatasetFolderOrFileInfo>();
            mRecentlyFoundMyEMSLFiles = new List<DatasetFolderOrFileInfo>();

            mDotNetZipTools = new clsDotNetZipTools(debugLevel, workingDir);
            RegisterEvents(mDotNetZipTools);

            mFileIDComparer = new clsMyEMSLFileIDComparer();

            mMostRecentUnzippedFiles = new List<KeyValuePair<string, string>>();

            mMyEMSLAutoDisabled = false;
        }

        /// <summary>
        /// Append a file to a folder path that ends with @MyEMSLID_12345
        /// </summary>
        /// <param name="myEmslFolderPath">Folder path to which fileName will be appended</param>
        /// <param name="fileName">Filename to append</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static string AddFileToMyEMSLFolderPath(string myEmslFolderPath, string fileName)
        {

            var myEMSLFileID = DatasetInfoBase.ExtractMyEMSLFileID(myEmslFolderPath, out var folderPathClean);

            var filePath = Path.Combine(folderPathClean, fileName);

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
        /// <remarks></remarks>
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
        /// <remarks></remarks>
        public void AddFileToDownloadQueue(ArchivedFileInfo fileInfo)
        {
            mMyEMSLDatasetListInfo.AddFileToDownloadQueue(fileInfo);
        }

        /// <summary>
        /// Queue a file to be downloaded
        /// </summary>
        /// <param name="encodedFilePath">File path that includes @MyEMSLID_12345</param>
        /// <param name="unzipRequired">True if the file should be unzipped</param>
        /// <remarks></remarks>
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

                var fileInfoQuery = (from item in mAllFoundMyEMSLFiles where item.FileID == myEMSLFileID select item.FileInfo).ToList();

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
        /// Verify that svc-dms.pfx exists either in the same folder as Pacifica.core.dll or at C:\client_certs\
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
        /// <remarks></remarks>
        public void ClearAllFoundFiles()
        {
            mAllFoundMyEMSLFiles.Clear();
        }

        /// <summary>
        /// Clear the queue of files to download
        /// </summary>
        /// <remarks></remarks>
        public void ClearDownloadQueue()
        {
            mMyEMSLDatasetListInfo.FilesToDownload.Clear();
        }

        /// <summary>
        /// Look for the given file (optionally in a given subdirectory) for the given dataset
        /// </summary>
        /// <param name="fileName">File name to find; can contain a wildcard, e.g. *.zip</param>
        /// <param name="subDirectoryName">Directory in which the file must reside; can contain a wildcard, e.g. SIC*</param>
        /// <param name="datasetName">Dataset name filter</param>
        /// <param name="recurse">True to search all directories; false to only search the root folder (or only subDirectoryName)</param>
        /// <returns>List of matching files</returns>
        /// <remarks>subDirectoryName can contain a partial path, for example 2013_09_10_DPB_Unwashed_Media_25um.d\2013_09_10_In_1sec_1MW.m</remarks>
        public List<DatasetFolderOrFileInfo> FindFiles(string fileName, string subDirectoryName, string datasetName, bool recurse)
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
                        mRecentlyFoundMyEMSLFiles = new List<DatasetFolderOrFileInfo>();
                    else
                        mRecentlyFoundMyEMSLFiles.Clear();

                    return mRecentlyFoundMyEMSLFiles;
                }
            }

            mRecentlyFoundMyEMSLFiles = mMyEMSLDatasetListInfo.FindFiles(fileName, subDirectoryName, datasetName, recurse);

            if (!mMyEMSLAutoDisabled)
            {
                mMyEMSLConnectionErrorCount = 0;
                mMyEMSLDisableCount = 0;
            }

            var filesToAdd = mRecentlyFoundMyEMSLFiles.Except(mAllFoundMyEMSLFiles, mFileIDComparer);

            mAllFoundMyEMSLFiles.AddRange(filesToAdd);

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
        /// <param name="downloadFolderPath">Target folder path (ignored for files defined in dctDestFilePathOverride)</param>
        /// <param name="folderLayout">Folder Layout (ignored for files defined in dctDestFilePathOverride)</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>Returns True if the download queue is empty</remarks>
        public bool ProcessMyEMSLDownloadQueue(string downloadFolderPath, Downloader.DownloadFolderLayout folderLayout)
        {

            if (mMyEMSLDatasetListInfo.FilesToDownload.Count == 0)
            {
                // Nothing to download; that's OK
                return true;
            }

            mMostRecentUnzippedFiles.Clear();

            var success = mMyEMSLDatasetListInfo.ProcessDownloadQueue(downloadFolderPath, folderLayout);
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

        #region "MyEMSL Event Handlers"


        private void MyEMSLDatasetListInfo_MyEMSLOffline(string message)
        {
            mMyEMSLConnectionErrorCount += 1;

            if (mMyEMSLConnectionErrorCount < 3)
                return;

            // Disable contacting MyEMSL for the next 15 minutes (or longer if mMyEMSLDisableCount is > 1)
            mMyEMSLAutoDisabled = true;

            mMyEMSLDisableCount += 1;
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
                var fiFileToUnzip = new FileInfo(Path.Combine(e.DownloadFolderPath, e.ArchivedFile.Filename));

                if (!fiFileToUnzip.Exists)
                    return;

                if (fiFileToUnzip.Extension.ToLower() == ".zip")
                {
                    // Decompress the .zip file
                    OnStatusEvent("Unzipping file " + fiFileToUnzip.Name);
                    mDotNetZipTools.UnzipFile(fiFileToUnzip.FullName, e.DownloadFolderPath);
                    mMostRecentUnzippedFiles.AddRange(mDotNetZipTools.MostRecentUnzippedFiles);
                }
                else if (fiFileToUnzip.Extension.ToLower() == ".gz")
                {
                    // Decompress the .gz file
                    OnStatusEvent("Unzipping file " + fiFileToUnzip.Name);
                    mDotNetZipTools.GUnzipFile(fiFileToUnzip.FullName, e.DownloadFolderPath);
                    mMostRecentUnzippedFiles.AddRange(mDotNetZipTools.MostRecentUnzippedFiles);
                }

            }

            FileDownloaded?.Invoke(sender, e);
        }
        #endregion

        /// <summary>
        /// Determines whether two DatasetFolderOrFileInfo instances refer to the same file in MyEMSL
        /// </summary>
        /// <remarks>Compares the value of FileID in the two instances</remarks>
        private class clsMyEMSLFileIDComparer : IEqualityComparer<DatasetFolderOrFileInfo>
        {
            private bool ItemsAreEqual(DatasetFolderOrFileInfo x, DatasetFolderOrFileInfo y)
            {
                return x.FileID == y.FileID;
            }

            bool IEqualityComparer<DatasetFolderOrFileInfo>.Equals(DatasetFolderOrFileInfo x, DatasetFolderOrFileInfo y)
            {
                return ItemsAreEqual(x, y);
            }

            private int GetHashCodeForItem(DatasetFolderOrFileInfo obj)
            {
                return obj.FileID.GetHashCode();
            }
            int IEqualityComparer<DatasetFolderOrFileInfo>.GetHashCode(DatasetFolderOrFileInfo obj)
            {
                return GetHashCodeForItem(obj);
            }
        }

    }

}