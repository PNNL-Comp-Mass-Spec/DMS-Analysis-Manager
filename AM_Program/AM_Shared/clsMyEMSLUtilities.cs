
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using MyEMSLReader;
using PRISM;

namespace AnalysisManagerBase
{
    public class clsMyEMSLUtilities : clsEventNotifier
    {

        public const string MYEMSL_PATH_FLAG = @"\\MyEMSL";

        private readonly clsIonicZipTools m_IonicZipTools;
        private readonly DatasetListInfo m_MyEMSLDatasetListInfo;

        private readonly List<DatasetFolderOrFileInfo> m_AllFoundMyEMSLFiles;

        private List<DatasetFolderOrFileInfo> m_RecentlyFoundMyEMSLFiles;

        private DateTime m_LastMyEMSLProgressWriteTime = DateTime.UtcNow;

        private readonly clsMyEMSLFileIDComparer mFileIDComparer;

        private readonly List<KeyValuePair<string, string>> m_MostRecentUnzippedFiles;

        #region "Events"
        public event FileDownloadedEventHandler FileDownloaded;

        #endregion

        #region "Properties"

        /// <summary>
        /// The most recently downloaded files; keys are the full paths to the downloaded file, values are extended file info
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public Dictionary<string, ArchivedFileInfo> DownloadedFiles => m_MyEMSLDatasetListInfo.DownloadedFiles;

        /// <summary>
        /// MyEMSL IDs of files queued to be downloaded
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public Dictionary<long, DownloadQueue.udtFileToDownload> FilesToDownload => m_MyEMSLDatasetListInfo.FilesToDownload;

        /// <summary>
        /// All files found in MyEMSL via calls to FindFiles
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public List<DatasetFolderOrFileInfo> AllFoundMyEMSLFiles => m_AllFoundMyEMSLFiles;

        /// <summary>
        /// Returns the files most recently unzipped
        /// Keys in the KeyValuePairs are filenames while values are relative paths (in case the .zip file has folders)
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public List<KeyValuePair<string, string>> MostRecentUnzippedFiles => m_MostRecentUnzippedFiles;

        /// <summary>
        /// Files most recently found via a call to FindFiles
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public List<DatasetFolderOrFileInfo> RecentlyFoundMyEMSLFiles => m_RecentlyFoundMyEMSLFiles;

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="debugLevel">Debug level (higher number means more messages)</param>
        /// <param name="workingDir">Working directory path</param>
        /// <remarks></remarks>
        public clsMyEMSLUtilities(int debugLevel, string workingDir)
        {
            m_MyEMSLDatasetListInfo = new DatasetListInfo();
            m_MyEMSLDatasetListInfo.DebugEvent += OnDebugEvent;
            m_MyEMSLDatasetListInfo.StatusEvent += OnStatusEvent;
            m_MyEMSLDatasetListInfo.ErrorEvent += OnErrorEvent;
            m_MyEMSLDatasetListInfo.WarningEvent += OnWarningEvent;
            m_MyEMSLDatasetListInfo.ProgressUpdate += m_MyEMSLDatasetListInfo_ProgressEvent;

            m_MyEMSLDatasetListInfo.FileDownloadedEvent += m_MyEMSLDatasetListInfo_FileDownloadedEvent;

            m_AllFoundMyEMSLFiles = new List<DatasetFolderOrFileInfo>();
            m_RecentlyFoundMyEMSLFiles = new List<DatasetFolderOrFileInfo>();

            m_IonicZipTools = new clsIonicZipTools(debugLevel, workingDir);
            RegisterEvents(m_IonicZipTools);

            mFileIDComparer = new clsMyEMSLFileIDComparer();

            m_MostRecentUnzippedFiles = new List<KeyValuePair<string, string>>();
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
            if ((!m_MyEMSLDatasetListInfo.ContainsDataset(datasetName)))
            {
                m_MyEMSLDatasetListInfo.AddDataset(datasetName);
            }
        }

        /// <summary>
        /// Queue a file to be downloaded
        /// </summary>
        /// <param name="fileInfo">Archive File Info</param>
        /// <remarks></remarks>
        public void AddFileToDownloadQueue(ArchivedFileInfo fileInfo)
        {
            m_MyEMSLDatasetListInfo.AddFileToDownloadQueue(fileInfo);
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

            if (myEMSLFileID > 0)
            {

                if (!GetCachedArchivedFileInfo(myEMSLFileID, out var matchingFileInfo))
                {
                    // File not found in m_RecentlyFoundMyEMSLFiles
                    // Instead check m_AllFoundMyEMSLFiles

                    var fileInfoQuery = (from item in m_AllFoundMyEMSLFiles where item.FileID == myEMSLFileID select item.FileInfo).ToList();

                    if (fileInfoQuery.Count == 0)
                    {
                        OnErrorEvent("Cached ArchiveFileInfo does not contain MyEMSL File ID " + myEMSLFileID);
                        return false;
                    }

                    matchingFileInfo = fileInfoQuery.First();
                }

                AddDataset(matchingFileInfo.Dataset);
                m_MyEMSLDatasetListInfo.AddFileToDownloadQueue(matchingFileInfo, unzipRequired);
                return true;

            }

            OnErrorEvent("MyEMSL File ID not found in path: " + encodedFilePath);
            return false;
        }

        /// <summary>
        /// Clear the list of MyEMSL files found via calls to FindFiles
        /// </summary>
        /// <remarks></remarks>
        public void ClearAllFoundfiles()
        {
            m_AllFoundMyEMSLFiles.Clear();
        }

        /// <summary>
        /// Clear the queue of files to download
        /// </summary>
        /// <remarks></remarks>
        public void ClearDownloadQueue()
        {
            m_MyEMSLDatasetListInfo.FilesToDownload.Clear();
        }

        /// <summary>
        /// Look for the given file (optionally in a given subfolder) for the given dataset
        /// </summary>
        /// <param name="fileName">File name to find; can contain a wildcard, e.g. *.zip</param>
        /// <param name="subFolderName">Subfolder in which the file must reside; can contain a wildcard, e.g. SIC*</param>
        /// <param name="datasetName">Dataset name filter</param>
        /// <param name="recurse">True to search all subfolders; false to only search the root folder (or only subFolderName)</param>
        /// <returns>List of matching files</returns>
        /// <remarks>subFolderName can contain a partial path, for example 2013_09_10_DPB_Unwashed_Media_25um.d\2013_09_10_In_1sec_1MW.m</remarks>
        public List<DatasetFolderOrFileInfo> FindFiles(string fileName, string subFolderName, string datasetName, bool recurse)
        {

            // Make sure the dataset name is being tracked by m_MyEMSLDatasetListInfo
            AddDataset(datasetName);

            m_RecentlyFoundMyEMSLFiles = m_MyEMSLDatasetListInfo.FindFiles(fileName, subFolderName, datasetName, recurse);

            var filesToAdd = m_RecentlyFoundMyEMSLFiles.Except(m_AllFoundMyEMSLFiles, mFileIDComparer);

            m_AllFoundMyEMSLFiles.AddRange(filesToAdd);

            return m_RecentlyFoundMyEMSLFiles;

        }

        private bool GetCachedArchivedFileInfo(long myEMSLFileID, out ArchivedFileInfo matchingFileInfo)
        {

            matchingFileInfo = null;

            var fileInfoQuery = (from item in m_RecentlyFoundMyEMSLFiles where item.FileID == myEMSLFileID select item.FileInfo).ToList();

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

            if (m_MyEMSLDatasetListInfo.FilesToDownload.Count == 0)
            {
                // Nothing to download; that's OK
                return true;
            }

            m_MostRecentUnzippedFiles.Clear();

            var success = m_MyEMSLDatasetListInfo.ProcessDownloadQueue(downloadFolderPath, folderLayout);
            if (success)
                return true;

            if (m_MyEMSLDatasetListInfo.ErrorMessages.Count > 0)
            {
                OnErrorEvent("Error in ProcessMyEMSLDownloadQueue: " + m_MyEMSLDatasetListInfo.ErrorMessages.First());
            }
            else
            {
                OnErrorEvent("Unknown error in ProcessMyEMSLDownloadQueue");
            }

            return false;

        }

        #region "MyEMSL Event Handlers"

        private void m_MyEMSLDatasetListInfo_ProgressEvent(string progressMessage, float percentComplete)
        {
            if (DateTime.UtcNow.Subtract(m_LastMyEMSLProgressWriteTime).TotalMinutes > 0.2)
            {
                m_LastMyEMSLProgressWriteTime = DateTime.UtcNow;
                OnProgressUpdate("MyEMSL downloader: " + percentComplete + "% complete", percentComplete);
            }
        }

        private void m_MyEMSLDatasetListInfo_FileDownloadedEvent(object sender, FileDownloadedEventArgs e)
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
                    m_IonicZipTools.UnzipFile(fiFileToUnzip.FullName, e.DownloadFolderPath);
                    m_MostRecentUnzippedFiles.AddRange(m_IonicZipTools.MostRecentUnzippedFiles);
                }
                else if (fiFileToUnzip.Extension.ToLower() == ".gz")
                {
                    // Decompress the .gz file
                    OnStatusEvent("Unzipping file " + fiFileToUnzip.Name);
                    m_IonicZipTools.GUnzipFile(fiFileToUnzip.FullName, e.DownloadFolderPath);
                    m_MostRecentUnzippedFiles.AddRange(m_IonicZipTools.MostRecentUnzippedFiles);
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