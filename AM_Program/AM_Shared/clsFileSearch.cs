using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using AnalysisManagerBase.Logging;
using MyEMSLReader;
using PHRPReader;
using PRISM;

namespace AnalysisManagerBase
{
    /// <summary>
    /// File search methods
    /// </summary>
    public class clsFileSearch : clsEventNotifier
    {
        #region "Constants"

        private const string MYEMSL_PATH_FLAG = clsMyEMSLUtilities.MYEMSL_PATH_FLAG;

        #endregion

        #region "Module variables"

        private readonly bool m_AuroraAvailable;

        private readonly int m_DebugLevel;

        private readonly string m_WorkingDir;

        private readonly IMgrParams m_mgrParams;

        private readonly IJobParams m_jobParams;

        private readonly clsFileCopyUtilities m_FileCopyUtilities;

        private readonly clsFolderSearch m_FolderSearch;

        private readonly clsMyEMSLUtilities m_MyEMSLUtilities;

        private readonly clsDotNetZipTools m_DotNetZipTools;

        #endregion

        #region "Properties"

        /// <summary>
        /// Dataset name
        /// </summary>
        public string DatasetName { get; set; }

        /// <summary>
        /// True if MyEMSL search is disabled
        /// </summary>
        public bool MyEMSLSearchDisabled { get; set; }

        #endregion

        #region "Events"

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="fileCopyUtilities"></param>
        /// <param name="folderSearch"></param>
        /// <param name="myEmslUtilities"></param>
        /// <param name="mgrParams"></param>
        /// <param name="jobParams"></param>
        /// <param name="datasetName"></param>
        /// <param name="debugLevel"></param>
        /// <param name="workingDir"></param>
        /// <param name="auroraAvailable"></param>
        public clsFileSearch(
            clsFileCopyUtilities fileCopyUtilities,
            clsFolderSearch folderSearch,
            clsMyEMSLUtilities myEmslUtilities,
            IMgrParams mgrParams,
            IJobParams jobParams,
            string datasetName,
            short debugLevel,
            string workingDir,
            bool auroraAvailable)
        {
            m_FileCopyUtilities = fileCopyUtilities;
            m_FolderSearch = folderSearch;
            m_MyEMSLUtilities = myEmslUtilities;
            m_mgrParams = mgrParams;
            m_jobParams = jobParams;
            DatasetName = datasetName;
            m_DebugLevel = debugLevel;
            m_WorkingDir = workingDir;
            m_AuroraAvailable = auroraAvailable;

            m_DotNetZipTools = new clsDotNetZipTools(debugLevel, workingDir);
            RegisterEvents(m_DotNetZipTools);
        }

        /// <summary>
        /// Copies the zipped s-folders to the working directory
        /// </summary>
        /// <param name="createStoragePathInfoOnly">
        /// When true, then does not actually copy the specified files,
        /// but instead creates a series of files named s*.zip_StoragePathInfo.txt,
        /// and each file's first line will be the full path to the source file
        /// </param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        private bool CopySFoldersToWorkDir(bool createStoragePathInfoOnly)
        {

            var DSFolderPath = m_FolderSearch.FindValidFolder(DatasetName, "s*.zip", RetrievingInstrumentDataFolder: true);

            // Verify dataset folder exists
            if (!Directory.Exists(DSFolderPath))
                return false;

            // Get a listing of the zip files to process
            var zipFiles = Directory.GetFiles(DSFolderPath, "s*.zip");
            if (zipFiles.GetLength(0) < 1)
            {
                // No zipped data files found
                return false;
            }

            // Copy each of the s*.zip files to the working directory

            foreach (var sourceZipFile in zipFiles)
            {
                if (m_DebugLevel > 3)
                {
                    OnDebugEvent("Copying file " + sourceZipFile + " to work directory");
                }
                var sourceFileName = Path.GetFileName(sourceZipFile);
                if (string.IsNullOrEmpty(sourceFileName))
                {
                    OnErrorEvent("Unable to determine the filename of zip file " + sourceZipFile);
                    return false;
                }

                var destFilePath = Path.Combine(m_WorkingDir, sourceFileName);

                if (createStoragePathInfoOnly)
                {
                    if (!m_FileCopyUtilities.CreateStoragePathInfoFile(sourceZipFile, destFilePath))
                    {
                        OnErrorEvent("Error creating storage path info file for " + sourceZipFile);
                        return false;
                    }
                }
                else
                {
                    if (!m_FileCopyUtilities.CopyFileWithRetry(sourceZipFile, destFilePath, false))
                    {
                        OnErrorEvent("Error copying file " + sourceZipFile);
                        return false;
                    }
                }
            }

            // If we got to here, everything worked
            return true;
        }

        /// <summary>
        /// Tries to delete the first file whose path is defined in filesToDelete
        /// If deletion succeeds, removes the file from the queue
        /// </summary>
        /// <param name="filesToDelete">Queue of files to delete (full file paths)</param>
        /// <param name="fileToQueueForDeletion">Optional: new file to add to the queue; blank to do nothing</param>
        /// <remarks></remarks>
        protected void DeleteQueuedFiles(Queue<string> filesToDelete, string fileToQueueForDeletion)
        {
            if (filesToDelete.Count > 0)
            {
                // Call the garbage collector, then try to delete the first queued file
                // Note, do not call WaitForPendingFinalizers since that could block this thread
                // Thus, do not use PRISM.clsProgRunner.GarbageCollectNow
                GC.Collect();

                try
                {
                    var fileToDelete = filesToDelete.Peek();

                    File.Delete(fileToDelete);

                    // If we get here, the delete succeeded, so we can dequeue the file
                    filesToDelete.Dequeue();

                }
                catch (Exception)
                {
                    // Exception deleting the file; ignore this error
                }

            }

            if (!string.IsNullOrEmpty(fileToQueueForDeletion))
            {
                filesToDelete.Enqueue(fileToQueueForDeletion);
            }

        }

        private bool FileExistsInWorkDir(string fileName)
        {
            var fileInfo = new FileInfo(Path.Combine(m_WorkingDir, fileName));
            return fileInfo.Exists;
        }

        /// <summary>
        /// Retrieves specified file from storage server, xfer folder, or archive and unzips if necessary
        /// </summary>
        /// <param name="fileName">Name of file to be retrieved</param>
        /// <param name="unzip">TRUE if retrieved file should be unzipped after retrieval</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks>Logs an error if the file is not found</remarks>
        public bool FindAndRetrieveMiscFiles(string fileName, bool unzip)
        {
            return FindAndRetrieveMiscFiles(fileName, unzip, searchArchivedDatasetFolder: true);
        }

        /// <summary>
        /// Retrieves specified file from storage server, xfer folder, or archive and unzips if necessary
        /// </summary>
        /// <param name="fileName">Name of file to be retrieved</param>
        /// <param name="unzip">TRUE if retrieved file should be unzipped after retrieval</param>
        /// <param name="searchArchivedDatasetFolder">TRUE if the EMSL archive (Aurora) should also be searched</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks>Logs an error if the file is not found</remarks>
        public bool FindAndRetrieveMiscFiles(string fileName, bool unzip, bool searchArchivedDatasetFolder)
        {
            return FindAndRetrieveMiscFiles(fileName, unzip, searchArchivedDatasetFolder, out _);
        }

        /// <summary>
        /// Retrieves specified file from storage server, xfer folder, or archive and unzips if necessary
        /// </summary>
        /// <param name="fileName">Name of file to be retrieved</param>
        /// <param name="unzip">TRUE if retrieved file should be unzipped after retrieval</param>
        /// <param name="searchArchivedDatasetFolder">TRUE if the EMSL archive (Aurora) should also be searched</param>
        /// <param name="logFileNotFound">True if an error should be logged when a file is not found</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        public bool FindAndRetrieveMiscFiles(string fileName, bool unzip, bool searchArchivedDatasetFolder, bool logFileNotFound)
        {
            return FindAndRetrieveMiscFiles(fileName, unzip, searchArchivedDatasetFolder, out _, logFileNotFound);
        }

        /// <summary>
        /// Retrieves specified file from storage server, xfer folder, or archive and unzips if necessary
        /// </summary>
        /// <param name="fileName">Name of file to be retrieved</param>
        /// <param name="unzip">TRUE if retrieved file should be unzipped after retrieval</param>
        /// <param name="searchArchivedDatasetFolder">TRUE if the EMSL archive (Aurora) should also be searched</param>
        /// <param name="sourceFolderPath">Output parameter: the folder from which the file was copied</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks>Logs an error if the file is not found</remarks>
        public bool FindAndRetrieveMiscFiles(string fileName, bool unzip, bool searchArchivedDatasetFolder, out string sourceFolderPath)
        {

            return FindAndRetrieveMiscFiles(fileName, unzip, searchArchivedDatasetFolder, out sourceFolderPath, logFileNotFound: true);
        }

        /// <summary>
        /// Retrieves specified file from storage server, xfer folder, or archive and unzips if necessary
        /// </summary>
        /// <param name="fileName">Name of file to be retrieved</param>
        /// <param name="unzip">TRUE if retrieved file should be unzipped after retrieval</param>
        /// <param name="searchArchivedDatasetFolder">TRUE if the EMSL archive (Aurora) should also be searched</param>
        /// <param name="sourceFolderPath">Output parameter: the folder from which the file was copied</param>
        /// <param name="logFileNotFound">True if an error should be logged when a file is not found</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        private bool FindAndRetrieveMiscFiles(
            string fileName, bool unzip, bool searchArchivedDatasetFolder,
            out string sourceFolderPath, bool logFileNotFound)
        {

            const bool CreateStoragePathInfoFile = false;

            // Look for the file in the various folders
            // A message will be logged if the file is not found
            sourceFolderPath = FindDataFile(fileName, searchArchivedDatasetFolder, logFileNotFound);

            // Exit if file was not found
            if (string.IsNullOrEmpty(sourceFolderPath))
            {
                // No folder found containing the specified file
                sourceFolderPath = string.Empty;
                return false;
            }

            if (sourceFolderPath.StartsWith(MYEMSL_PATH_FLAG))
            {
                return m_MyEMSLUtilities.AddFileToDownloadQueue(sourceFolderPath);
            }

            // Copy the file
            if (!m_FileCopyUtilities.CopyFileToWorkDir(fileName, sourceFolderPath, m_WorkingDir, BaseLogger.LogLevels.ERROR, CreateStoragePathInfoFile))
            {
                return false;
            }

            // Check whether unzipping was requested
            if (!unzip)
                return true;

            OnStatusEvent("Unzipping file " + fileName);
            if (UnzipFileStart(Path.Combine(m_WorkingDir, fileName), m_WorkingDir, "FindAndRetrieveMiscFiles", false))
            {
                if (m_DebugLevel >= 1)
                {
                    OnStatusEvent("Unzipped file " + fileName);
                }
            }

            return true;

        }

        /// <summary>
        /// Search for the specified PHRP file and copy it to the work directory
        /// If the filename contains _msgfplus and the file is not found, auto looks for the _msgfdb version of the file
        /// </summary>
        /// <param name="fileToGet">File to find; if the file is found with an alternative name, this variable is updated with the new name</param>
        /// <param name="synopsisFileName">Synopsis file name, if known</param>
        /// <param name="addToResultFileSkipList">If true, add the filename to the list of files to skip copying to the result folder</param>
        /// <param name="logFileNotFound">True if an error should be logged when a file is not found</param>
        /// <returns>True if success, false if not found</returns>
        /// <remarks>Used by the IDPicker and MSGF plugins</remarks>
        public bool FindAndRetrievePHRPDataFile(
            ref string fileToGet,
            string synopsisFileName,
            bool addToResultFileSkipList = true,
            bool logFileNotFound = true)
        {

            var success = FindAndRetrieveMiscFiles(
                fileToGet, unzip: false, searchArchivedDatasetFolder: true, logFileNotFound: logFileNotFound);

            if (!success && fileToGet.ToLower().Contains("msgfplus"))
            {
                if (string.IsNullOrEmpty(synopsisFileName))
                    synopsisFileName = "Dataset_msgfdb.txt";
                var alternativeName = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(fileToGet, synopsisFileName);

                if (!string.Equals(alternativeName, fileToGet))
                {
                    success = FindAndRetrieveMiscFiles(
                        alternativeName, unzip: false, searchArchivedDatasetFolder: true, logFileNotFound: logFileNotFound);

                    if (success)
                    {
                        fileToGet = alternativeName;
                    }
                }
            }

            if (!success)
            {
                return false;
            }

            if (addToResultFileSkipList)
            {
                m_jobParams.AddResultFileToSkip(fileToGet);
            }

            return true;

        }

        /// <summary>
        /// Finds the _DTA.txt file for this dataset
        /// </summary>
        /// <returns>The path to the _dta.zip file (or _dta.txt file)</returns>
        /// <remarks></remarks>
        public string FindCDTAFile(out string errorMessage)
        {

            errorMessage = string.Empty;

            // Retrieve zipped DTA file
            var sourceFileName = DatasetName + "_dta.zip";
            var sourceFolderPath = FindDataFile(sourceFileName);

            if (!string.IsNullOrEmpty(sourceFolderPath))
            {
                if (sourceFolderPath.StartsWith(MYEMSL_PATH_FLAG))
                {
                    // add the _dta.zip file name to the folder path found by FindDataFile
                    return clsMyEMSLUtilities.AddFileToMyEMSLFolderPath(sourceFolderPath, sourceFileName);
                }

                // Return the path to the _dta.zip file
                return Path.Combine(sourceFolderPath, sourceFileName);
            }

            // Couldn't find a folder with the _dta.zip file; how about the _dta.txt file?

            sourceFileName = DatasetName + "_dta.txt";
            sourceFolderPath = FindDataFile(sourceFileName);

            if (string.IsNullOrEmpty(sourceFolderPath))
            {
                // No folder found containing the zipped DTA files; return False
                // (the FindDataFile procedure should have already logged an error)
                errorMessage = "Could not find " + sourceFileName + " using FindDataFile";
                return string.Empty;
            }

            OnWarningEvent("Warning: could not find the _dta.zip file, but was able to find " + sourceFileName + " in folder " + sourceFolderPath);

            if (sourceFolderPath.StartsWith(MYEMSL_PATH_FLAG))
            {
                return sourceFolderPath;
            }

            // Return the path to the _dta.txt file
            return Path.Combine(sourceFolderPath, sourceFileName);
        }

        /// <summary>
        /// Finds the server or archive folder where specified file is located
        /// </summary>
        /// <param name="fileToFind">Name of the file to search for</param>
        /// <returns>Path to the directory containing the file if the file was found; empty string if not found found</returns>
        /// <remarks>If the file is found in MyEMSL, the directory path returned will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
        public string FindDataFile(string fileToFind)
        {
            return FindDataFile(fileToFind, searchArchivedDatasetFolder: true);
        }

        /// <summary>
        /// Finds the server or archive folder where specified file is located
        /// </summary>
        /// <param name="fileToFind">Name of the file to search for</param>
        /// <param name="searchArchivedDatasetFolder">
        /// TRUE if the EMSL archive (Aurora) or MyEMSL should also be searched
        /// (m_AuroraAvailable and MyEMSLSearchDisabled take precedence)
        /// </param>
        /// <param name="logFileNotFound">True if an error should be logged when a file is not found</param>
        /// <returns>Path to the directory containing the file if the file was found; empty string if not found found</returns>
        /// <remarks>If the file is found in MyEMSL, the directory path returned will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
        public string FindDataFile(string fileToFind, bool searchArchivedDatasetFolder, bool logFileNotFound = true)
        {

            try
            {
                // Fill collection with possible folder locations
                // The order of searching is:
                //  a. Check the "inputFolderName" and then each of the Shared Results Folders in the Transfer folder
                //  b. Check the "inputFolderName" and then each of the Shared Results Folders in the Dataset folder
                //  c. Check the "inputFolderName" and then each of the Shared Results Folders in MyEMSL for this dataset
                //  d. Check the "inputFolderName" and then each of the Shared Results Folders in the Archived dataset folder
                //
                // Note that "SharedResultsFolders" will typically only contain one folder path,
                //  but can contain a comma-separated list of folders

                var datasetFolderName = m_jobParams.GetParam("DatasetFolderName");
                var inputFolderName = m_jobParams.GetParam("inputFolderName");

                var sharedResultFolderNames = GetSharedResultFolderList().ToList();

                var parentFolderPaths = new List<string> {
                    m_jobParams.GetParam("transferFolderPath"),
                    m_jobParams.GetParam("DatasetStoragePath")};

                if (searchArchivedDatasetFolder)
                {
                    if (!MyEMSLSearchDisabled)
                    {
                        parentFolderPaths.Add(MYEMSL_PATH_FLAG);
                    }
                    if (m_AuroraAvailable)
                    {
                        parentFolderPaths.Add(m_jobParams.GetParam("DatasetArchivePath"));
                    }
                }

                var foldersToSearch = new List<string>();

                foreach (var parentFolderPath in parentFolderPaths)
                {
                    if (!string.IsNullOrEmpty(parentFolderPath))
                    {
                        if (!string.IsNullOrEmpty(inputFolderName))
                        {
                            // Parent Folder \ Dataset Folder \ Input folder
                            foldersToSearch.Add(FindDataFileAddFolder(parentFolderPath, datasetFolderName, inputFolderName));
                        }

                        foreach (var sharedFolderName in sharedResultFolderNames)
                        {
                            // Parent Folder \ Dataset Folder \  Shared results folder
                            foldersToSearch.Add(FindDataFileAddFolder(parentFolderPath, datasetFolderName, sharedFolderName));
                        }

                        // Parent Folder \ Dataset Folder
                        foldersToSearch.Add(FindDataFileAddFolder(parentFolderPath, datasetFolderName, string.Empty));
                    }

                }

                var matchingDirectory = string.Empty;
                var matchFound = false;

                // Now search for FileToFind in each folder in FoldersToSearch
                foreach (var folderPath in foldersToSearch)
                {
                    try
                    {
                        var diFolderToCheck = new DirectoryInfo(folderPath);

                        if (folderPath.StartsWith(MYEMSL_PATH_FLAG))
                        {
                            var matchingMyEMSLFiles = m_MyEMSLUtilities.FindFiles(fileToFind, diFolderToCheck.Name, DatasetName, recurse: false);

                            if (matchingMyEMSLFiles.Count > 0)
                            {
                                matchFound = true;

                                // Include the MyEMSL FileID in TempDir so that it is available for downloading
                                matchingDirectory = DatasetInfoBase.AppendMyEMSLFileID(folderPath, matchingMyEMSLFiles.First().FileID);
                                break;
                            }

                        }
                        else
                        {
                            if (diFolderToCheck.Exists)
                            {
                                if (File.Exists(Path.Combine(folderPath, fileToFind)))
                                {
                                    matchFound = true;
                                    matchingDirectory = folderPath;
                                    break;
                                }
                            }

                        }

                    }
                    catch (Exception ex)
                    {
                        // Exception checking TempDir; log an error, but continue checking the other folders in FoldersToSearch
                        OnErrorEvent("Exception in FindDataFile looking for: " + fileToFind + " in " + folderPath, ex);
                    }
                }

                if (matchFound)
                {
                    if (m_DebugLevel >= 2)
                    {
                        OnDebugEvent("Data file found: " + fileToFind);
                    }
                    return matchingDirectory;
                }

                // Data file not found
                // Log this as an error if SearchArchivedDatasetFolder=True
                // Log this as a warning if SearchArchivedDatasetFolder=False

                if (logFileNotFound)
                {
                    if (searchArchivedDatasetFolder || (!m_AuroraAvailable && MyEMSLSearchDisabled))
                    {
                        OnErrorEvent("Data file not found: " + fileToFind);
                    }
                    else
                    {
                        OnWarningEvent("Warning: Data file not found (did not check archive): " + fileToFind);
                    }
                }

                return string.Empty;

            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception in FindDataFile looking for: " + fileToFind, ex);
            }

            // We'll only get here if an exception occurs
            return string.Empty;

        }

        private string FindDataFileAddFolder(string parentFolderPath, string datasetFolderName, string inputFolderName)
        {
            var targetFolderPath = Path.Combine(parentFolderPath, datasetFolderName);
            if (!string.IsNullOrEmpty(inputFolderName))
            {
                targetFolderPath = Path.Combine(targetFolderPath, inputFolderName);
            }

            return targetFolderPath;

        }

        /// <summary>
        /// Looks for file fileName in folderPath or any of its subfolders
        /// The filename may contain a wildcard character, in which case the first match will be returned
        /// </summary>
        /// <param name="folderPath">Folder path to examine</param>
        /// <param name="fileName">File name to find</param>
        /// <returns>Full path to the file, if found; empty string if no match</returns>
        /// <remarks></remarks>
        public static string FindFileInDirectoryTree(string folderPath, string fileName)
        {
            return FindFileInDirectoryTree(folderPath, fileName, new SortedSet<string>());
        }

        /// <summary>
        /// Looks for file fileName in folderPath or any of its subfolders
        /// The filename may contain a wildcard character, in which case the first match will be returned
        /// </summary>
        /// <param name="folderPath">Folder path to examine</param>
        /// <param name="fileName">File name to find</param>
        /// <param name="lstFolderNamesToSkip">List of folder names that should not be examined</param>
        /// <returns>Full path to the file, if found; empty string if no match</returns>
        /// <remarks></remarks>
        public static string FindFileInDirectoryTree(string folderPath, string fileName, SortedSet<string> lstFolderNamesToSkip)
        {
            var diFolder = new DirectoryInfo(folderPath);

            if (diFolder.Exists)
            {
                // Examine the files for this folder
                foreach (var fiFile in diFolder.GetFiles(fileName))
                {
                    var filePathMatch = fiFile.FullName;
                    return filePathMatch;
                }

                // Match not found
                // Recursively call this function with the subdirectories in this folder

                foreach (var ioSubFolder in diFolder.GetDirectories())
                {
                    if (!lstFolderNamesToSkip.Contains(ioSubFolder.Name))
                    {
                        var filePathMatch = FindFileInDirectoryTree(ioSubFolder.FullName, fileName);
                        if (!string.IsNullOrEmpty(filePathMatch))
                        {
                            return filePathMatch;
                        }
                    }
                }
            }

            return string.Empty;

        }

        /// <summary>
        /// Looks for the newest .mzXML file for this dataset
        /// </summary>
        /// <param name="hashCheckFilePath">Output parameter: path to the hashcheck file if the .mzXML file was found in the MSXml cache</param>
        /// <returns>Full path to the file, if found; empty string if no match</returns>
        /// <remarks>Supports both gzipped mzXML files and unzipped ones (gzipping was enabled in September 2014)</remarks>
        public string FindMZXmlFile(out string hashCheckFilePath)
        {

            // First look in the MsXML cache folder
            var matchingFilePath = FindMsXmlFileInCache(clsAnalysisResources.MSXMLOutputTypeConstants.mzXML, out hashCheckFilePath);

            if (!string.IsNullOrEmpty(matchingFilePath))
            {
                return matchingFilePath;
            }

            // Not found in the cache; look in the dataset folder

            var datasetID = m_jobParams.GetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, "DatasetID");

            const string MSXmlFoldernameBase = "MSXML_Gen_1_";
            var mzXMLFilename = DatasetName + ".mzXML";

            const int MAX_ATTEMPTS = 1;

            // Initialize the values we'll look for
            // Note that these values are added to the list in the order of the preferred file to retrieve
            var valuesToCheck = new List<int>
            {
                //         Example folder name          CentroidMSXML  MSXMLGenerator   CentroidPeakCount    MSXMLOutputType
                215,    // MSXML_Gen_1_215_DatasetID,   True           MSConvert        -1                   mzXML
                149,    // MSXML_Gen_1_149_DatasetID,   True           MSConvert        1000                 mzXML
                148,    // MSXML_Gen_1_148_DatasetID,   True           MSConvert        500                  mzXML
                154,    // MSXML_Gen_1_154_DatasetID,   True           MSConvert        250                  mzXML
                132,    // MSXML_Gen_1_132_DatasetID,   True           MSConvert        150                  mzXML
                93,     // MSXML_Gen_1_93_DatasetID,    True           ReadW.exe        n/a                  mzXML
                126,    // MSXML_Gen_1_126_DatasetID,   True           ReadW.exe        n/a                  mzXML; ReAdW_Version=v2.1
                177,    // MSXML_Gen_1_177_DatasetID,   False          MSConvert.exe    n/a                  mzXML
                39      // MSXML_Gen_1_39_DatasetID,    False          ReadW.exe        n/a                  mzXML
            };

            hashCheckFilePath = string.Empty;

            foreach (var version in valuesToCheck)
            {
                var msXmlFoldername = MSXmlFoldernameBase + version + "_" + datasetID;

                // Look for the MSXmlFolder
                // If the folder cannot be found, m_FolderSearch.FindValidFolder will return the folder defined by "DatasetStoragePath"
                var ServerPath = m_FolderSearch.FindValidFolder(DatasetName, "", msXmlFoldername, MAX_ATTEMPTS, false, retrievingInstrumentDataFolder: false);

                if (string.IsNullOrEmpty(ServerPath))
                {
                    continue;
                }

                if (ServerPath.StartsWith(MYEMSL_PATH_FLAG))
                {
                    // File found in MyEMSL
                    // Determine the MyEMSL FileID by searching for the expected file in m_MyEMSLUtilities.RecentlyFoundMyEMSLFiles

                    long myEmslFileID = 0;

                    foreach (var udtArchivedFile in m_MyEMSLUtilities.RecentlyFoundMyEMSLFiles)
                    {
                        var fiArchivedFile = new FileInfo(udtArchivedFile.FileInfo.RelativePathWindows);
                        if (clsGlobal.IsMatch(fiArchivedFile.Name, mzXMLFilename))
                        {
                            myEmslFileID = udtArchivedFile.FileID;
                            break;
                        }
                    }

                    if (myEmslFileID > 0)
                    {
                        return Path.Combine(ServerPath, msXmlFoldername, DatasetInfoBase.AppendMyEMSLFileID(mzXMLFilename, myEmslFileID));
                    }

                }
                else
                {
                    // Due to quirks with how m_FolderSearch.FindValidFolder behaves, we need to confirm that the mzXML file actually exists
                    var diFolderInfo = new DirectoryInfo(ServerPath);

                    if (diFolderInfo.Exists)
                    {
                        // See if the ServerPath folder actually contains a subfolder named MSXmlFoldername
                        var diSubfolders = diFolderInfo.GetDirectories(msXmlFoldername);

                        if (diSubfolders.Length > 0)
                        {
                            // MSXmlFolder found; return the path to the file
                            return Path.Combine(diSubfolders[0].FullName, mzXMLFilename);

                        }

                    }

                }

            }

            // If we get here, no match was found
            return string.Empty;

        }

        /// <summary>
        /// Looks for the newest mzXML or mzML file for this dataset
        /// </summary>
        /// <param name="msXmlType">File type to find (mzXML or mzML)</param>
        /// <param name="hashCheckFilePath">Output parameter: path to the hashcheck file if the .mzXML file was found in the MSXml cache</param>
        /// <returns>Full path to the file if a match; empty string if no match</returns>
        /// <remarks>Supports gzipped .mzML files and supports both gzipped .mzXML files and unzipped ones (gzipping was enabled in September 2014)</remarks>
        public string FindMsXmlFileInCache(
            clsAnalysisResources.MSXMLOutputTypeConstants msXmlType,
            out string hashCheckFilePath)
        {

            var MsXMLFilename = string.Copy(DatasetName);
            hashCheckFilePath = string.Empty;

            switch (msXmlType)
            {
                case clsAnalysisResources.MSXMLOutputTypeConstants.mzXML:
                    MsXMLFilename += clsAnalysisResources.DOT_MZXML_EXTENSION + clsAnalysisResources.DOT_GZ_EXTENSION;
                    break;
                case clsAnalysisResources.MSXMLOutputTypeConstants.mzML:
                    // All MzML files should be gzipped
                    MsXMLFilename += clsAnalysisResources.DOT_MZML_EXTENSION + clsAnalysisResources.DOT_GZ_EXTENSION;
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(msXmlType), "Unsupported enum value for MSXMLOutputTypeConstants: " + msXmlType);
            }

            // Lookup the MSXML cache path (typically \\Proto-11\MSXML_Cache )
            var msXmlCacheFolderPath = m_mgrParams.GetParam("MSXMLCacheFolderPath", string.Empty);

            var diCacheFolder = new DirectoryInfo(msXmlCacheFolderPath);

            if (!diCacheFolder.Exists)
            {
                OnWarningEvent("Warning: Cache folder not found: " + msXmlCacheFolderPath);
                return string.Empty;
            }

            // Determine the YearQuarter code for this dataset
            var datasetStoragePath = m_jobParams.GetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, "DatasetStoragePath");
            if (string.IsNullOrEmpty(datasetStoragePath) && (m_AuroraAvailable || !MyEMSLSearchDisabled))
            {
                datasetStoragePath = m_jobParams.GetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, "DatasetArchivePath");
            }

            var yearQuarter = clsAnalysisResources.GetDatasetYearQuarter(datasetStoragePath);

            var lstMatchingFiles = new List<FileInfo>();

            if (string.IsNullOrEmpty(yearQuarter))
            {
                // Perform an exhaustive recursive search of the MSXML file cache
                var lstFilesToAppend = diCacheFolder.GetFiles(MsXMLFilename, SearchOption.AllDirectories);

                if (lstFilesToAppend.Length == 0 && msXmlType == clsAnalysisResources.MSXMLOutputTypeConstants.mzXML)
                {
                    // Older .mzXML files were not gzipped
                    lstFilesToAppend = diCacheFolder.GetFiles(DatasetName + clsAnalysisResources.DOT_MZXML_EXTENSION, SearchOption.AllDirectories);
                }

                var query = (from item in lstFilesToAppend orderby item.LastWriteTimeUtc descending select item).Take(1);

                lstMatchingFiles.AddRange(query);

            }
            else
            {
                // Look for the file in the top level subfolders of the MSXML file cache
                foreach (var diToolFolder in diCacheFolder.GetDirectories())
                {
                    var lstSubFolders = diToolFolder.GetDirectories(yearQuarter);

                    if (lstSubFolders.Length > 0)
                    {
                        var lstFilesToAppend = lstSubFolders.First().GetFiles(MsXMLFilename, SearchOption.TopDirectoryOnly);
                        if (lstFilesToAppend.Length == 0 && msXmlType == clsAnalysisResources.MSXMLOutputTypeConstants.mzXML)
                        {
                            // Older .mzXML files were not gzipped
                            lstFilesToAppend = lstSubFolders.First().GetFiles(DatasetName + clsAnalysisResources.DOT_MZXML_EXTENSION, SearchOption.TopDirectoryOnly);
                        }

                        var query = (from item in lstFilesToAppend orderby item.LastWriteTimeUtc descending select item).Take(1);
                        lstMatchingFiles.AddRange(query);

                    }

                }

            }

            if (lstMatchingFiles.Count == 0)
            {
                return string.Empty;
            }

            // One or more matches were found; select the newest one
            var sortQuery = (from item in lstMatchingFiles orderby item.LastWriteTimeUtc descending select item).Take(1);
            var matchedFilePath = sortQuery.First().FullName;

            // Confirm that the file has a .hashcheck file and that the information in the .hashcheck file matches the file
            hashCheckFilePath = matchedFilePath + clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX;

            if (clsGlobal.ValidateFileVsHashcheck(matchedFilePath, hashCheckFilePath, out var errorMessage))
            {
                return matchedFilePath;
            }

            OnWarningEvent("Warning: " + errorMessage);
            return string.Empty;
        }

        /// <summary>
        /// Split apart coordinates that look like "R00X438Y093" into R, X, and Y
        /// </summary>
        /// <param name="coord"></param>
        /// <param name="reRegExRXY"></param>
        /// <param name="reRegExRX"></param>
        /// <param name="R"></param>
        /// <param name="X"></param>
        /// <param name="Y"></param>
        /// <returns>True if success, false otherwise</returns>
        /// <remarks></remarks>
        private bool GetBrukerImagingFileCoords(string coord, Regex reRegExRXY, Regex reRegExRX, out int R, out int X, out int Y)
        {
            // Try to match names like R00X438Y093
            var reMatch = reRegExRXY.Match(coord);

            var success = false;

            if (reMatch.Success)
            {
                // Match succeeded; extract out the coordinates
                if (int.TryParse(reMatch.Groups["R"].Value, out R))
                    success = true;
                if (int.TryParse(reMatch.Groups["X"].Value, out X))
                    success = true;
                int.TryParse(reMatch.Groups["Y"].Value, out Y);

            }
            else
            {
                // Try to match names like R00X438
                reMatch = reRegExRX.Match(coord);

                if (reMatch.Success)
                {
                    if (int.TryParse(reMatch.Groups["R"].Value, out R))
                        success = true;
                    if (int.TryParse(reMatch.Groups["X"].Value, out X))
                        success = true;
                    Y = 0;
                }
                else
                {
                    R = 0;
                    X = 0;
                    Y = 0;
                }
            }

            return success;

        }

        /// <summary>
        /// Looks for job parameters BrukerMALDI_Imaging_startSectionX and BrukerMALDI_Imaging_endSectionX
        /// If defined, populates startSectionX and endSectionX with the Start and End X values to filter on
        /// </summary>
        /// <param name="jobParams"></param>
        /// <param name="startSectionX"></param>
        /// <param name="endSectionX"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool GetBrukerImagingSectionFilter(IJobParams jobParams, out int startSectionX, out int endSectionX)
        {
            var applySectionFilter = false;
            startSectionX = -1;
            endSectionX = int.MaxValue;

            var param = jobParams.GetParam("MALDI_Imaging_startSectionX");
            if (!string.IsNullOrEmpty(param))
            {
                if (int.TryParse(param, out startSectionX))
                {
                    applySectionFilter = true;
                }
            }

            param = jobParams.GetParam("MALDI_Imaging_endSectionX");
            if (!string.IsNullOrEmpty(param))
            {
                if (int.TryParse(param, out endSectionX))
                {
                    applySectionFilter = true;
                }
            }

            return applySectionFilter;

        }

        /// <summary>
        /// Examines job parameter SharedResultsFolders to construct a list of the shared result folders
        /// </summary>
        /// <returns>List of folder names</returns>
        private IEnumerable<string> GetSharedResultFolderList()
        {

            var sharedResultFolderNames = new List<string>();

            var sharedResultFolders = m_jobParams.GetParam("SharedResultsFolders");

            if (sharedResultFolders.Contains(","))
            {
                // Split on commas and populate sharedResultFolderNames
                foreach (var item in sharedResultFolders.Split(','))
                {
                    var itemTrimmed = item.Trim();
                    if (itemTrimmed.Length > 0)
                    {
                        sharedResultFolderNames.Add(itemTrimmed);
                    }
                }

                // Reverse the list so that the last item in sharedResultFolders is the first item in sharedResultFolderNames
                sharedResultFolderNames.Reverse();
            }
            else
            {
                // Just one item in sharedResultFolders
                sharedResultFolderNames.Add(sharedResultFolders);
            }

            return sharedResultFolderNames;

        }

        /// <summary>
        /// Unzip gzipFilePath into the working directory
        /// Existing files will be overwritten
        /// </summary>
        /// <param name="gzipFilePath">.gz file to unzip</param>
        /// <returns>True if success; false if an error</returns>
        public bool GUnzipFile(string gzipFilePath)
        {
            return m_DotNetZipTools.GUnzipFile(gzipFilePath);
        }

        private void NotifyInvalidParentDirectory(FileSystemInfo fiSourceFile)
        {
            OnErrorEvent("Unable to determine the parent directory of " + fiSourceFile.FullName);
        }

        /// <summary>
        /// Retrieve the dataset's cached .mzML file from the MsXML Cache
        /// </summary>
        /// <param name="unzip">True to unzip; otherwise, will remain as a .gzip file</param>
        /// <param name="errorMessage">Output parameter: Error message</param>
        /// <param name="fileMissingFromCache">Output parameter: will be True if the file was not found in the cache</param>
        /// <returns>True if success, false if an error or file not found</returns>
        /// <remarks>
        /// Uses the jobs InputFolderName parameter to dictate which subfolder to search at \\Proto-11\MSXML_Cache
        /// InputFolderName should be in the form MSXML_Gen_1_93_367204
        /// </remarks>
        public bool RetrieveCachedMzMLFile(bool unzip, out string errorMessage, out bool fileMissingFromCache)
        {
            return RetrieveCachedMSXMLFile(clsAnalysisResources.DOT_MZML_EXTENSION, unzip, out errorMessage, out fileMissingFromCache);
        }

        /// <summary>
        /// Retrieve the dataset's cached .PBF file from the MsXML Cache
        /// </summary>
        /// <param name="errorMessage">Output parameter: Error message</param>
        /// <param name="fileMissingFromCache">Output parameter: will be True if the file was not found in the cache</param>
        /// <returns>True if success, false if an error or file not found</returns>
        /// <remarks>
        /// Uses the jobs InputFolderName parameter to dictate which subfolder to search at \\Proto-11\MSXML_Cache
        /// InputFolderName should be in the form MSXML_Gen_1_93_367204
        /// </remarks>
        public bool RetrieveCachedPBFFile(out string errorMessage, out bool fileMissingFromCache)
        {
            const bool unzip = false;
            return RetrieveCachedMSXMLFile(clsAnalysisResources.DOT_PBF_EXTENSION, unzip, out errorMessage, out fileMissingFromCache);
        }

        /// <summary>
        /// Retrieve the dataset's cached .mzXML file from the MsXML Cache
        /// </summary>
        /// <param name="unzip">True to unzip; otherwise, will remain as a .gzip file</param>
        /// <param name="errorMessage">Output parameter: Error message</param>
        /// <param name="fileMissingFromCache">Output parameter: will be True if the file was not found in the cache</param>
        /// <returns>True if success, false if an error or file not found</returns>
        /// <remarks>
        /// Uses the jobs InputFolderName parameter to dictate which subfolder to search at \\Proto-11\MSXML_Cache
        /// InputFolderName should be in the form MSXML_Gen_1_105_367204
        /// </remarks>
        public bool RetrieveCachedMzXMLFile(bool unzip, out string errorMessage, out bool fileMissingFromCache)
        {
            return RetrieveCachedMSXMLFile(clsAnalysisResources.DOT_MZXML_EXTENSION, unzip, out errorMessage, out fileMissingFromCache);
        }

        /// <summary>
        /// Retrieve the dataset's cached .mzXML or .mzML file from the MsXML Cache (assumes the file is gzipped)
        /// </summary>
        /// <param name="resultFileExtension">File extension to retrieve (.mzXML or .mzML)</param>
        /// <param name="unzip">True to unzip; otherwise, will remain as a .gzip file</param>
        /// <param name="errorMessage">Output parameter: Error message</param>
        /// <param name="fileMissingFromCache">Output parameter: will be True if the file was not found in the cache</param>
        /// <returns>True if success, false if an error or file not found</returns>
        /// <remarks>
        /// Uses the job's InputFolderName parameter to dictate which subfolder to search at \\Proto-11\MSXML_Cache
        /// InputFolderName should be in the form MSXML_Gen_1_93_367204
        /// </remarks>
        public bool RetrieveCachedMSXMLFile(string resultFileExtension, bool unzip, out string errorMessage, out bool fileMissingFromCache)
        {

            var msXMLCacheFolderPath = m_mgrParams.GetParam("MSXMLCacheFolderPath", string.Empty);
            var diMSXmlCacheFolder = new DirectoryInfo(msXMLCacheFolderPath);

            errorMessage = string.Empty;
            fileMissingFromCache = false;

            if (string.IsNullOrEmpty(resultFileExtension))
            {
                errorMessage = "resultFileExtension is empty; should be .mzXML or .mzML";
                return false;
            }

            if (!diMSXmlCacheFolder.Exists)
            {
                errorMessage = "MSXmlCache folder not found: " + msXMLCacheFolderPath;
                return false;
            }

            var foldersToSearch = new List<string> {
                m_jobParams.GetJobParameter("InputFolderName", string.Empty)
            };

            if (foldersToSearch[0].Length == 0)
            {
                foldersToSearch.Clear();
            }

            foreach (var sharedResultFolder in GetSharedResultFolderList())
            {
                if (sharedResultFolder.Trim().Length == 0)
                    continue;

                if (!foldersToSearch.Contains(sharedResultFolder))
                {
                    foldersToSearch.Add(sharedResultFolder);
                }
            }

            if (foldersToSearch.Count == 0)
            {
                errorMessage = "Job parameters InputFolderName and SharedResultsFolders are empty; cannot retrieve the " + resultFileExtension + " file";
                return false;
            }

            var msXmlToolNameVersionFolders = new List<string>();

            foreach (var folderName in foldersToSearch)
            {
                try
                {
                    var msXmlToolNameVersionFolder = clsAnalysisResources.GetMSXmlToolNameVersionFolder(folderName);
                    msXmlToolNameVersionFolders.Add(msXmlToolNameVersionFolder);
                }
                catch (Exception)
                {
                    errorMessage = "InputFolderName is not in the expected form of ToolName_Version_DatasetID (" + folderName + "); " +
                        "will not try to find the " + resultFileExtension + " file in this folder";

                    OnDebugEvent(errorMessage);
                }
            }

            if (msXmlToolNameVersionFolders.Count == 0)
            {
                if (string.IsNullOrEmpty(errorMessage))
                {
                    errorMessage = "The input folder and shared results folder(s) were not in the expected form of ToolName_Version_DatasetID";
                }
                return false;
            }

            errorMessage = string.Empty;

            DirectoryInfo disourceFolder = null;

            foreach (var toolNameVersionFolder in msXmlToolNameVersionFolders)
            {
                var sourceFolder = clsAnalysisResources.GetMSXmlCacheFolderPath(diMSXmlCacheFolder.FullName, m_jobParams, toolNameVersionFolder, out errorMessage);
                if (!string.IsNullOrEmpty(errorMessage))
                {
                    continue;
                }

                disourceFolder = new DirectoryInfo(sourceFolder);
                if (disourceFolder.Exists)
                {
                    break;
                }

                if (string.IsNullOrEmpty(errorMessage))
                {
                    errorMessage = "Cache folder does not exist (" + sourceFolder;
                }
                else
                {
                    errorMessage += " or " + sourceFolder;
                }

            }

            if (disourceFolder == null || !disourceFolder.Exists)
            {
                errorMessage += ")";
                fileMissingFromCache = true;
                return false;
            }

            var sourceFilePath = Path.Combine(disourceFolder.FullName, DatasetName + resultFileExtension);
            var expectedFileDescription = resultFileExtension;
            if (resultFileExtension != clsAnalysisResources.DOT_PBF_EXTENSION)
            {
                sourceFilePath += clsAnalysisResources.DOT_GZ_EXTENSION;
                expectedFileDescription += clsAnalysisResources.DOT_GZ_EXTENSION;
            }

            var fiSourceFile = new FileInfo(sourceFilePath);
            if (!fiSourceFile.Exists)
            {
                errorMessage = "Cached " + expectedFileDescription + " file does not exist in " + disourceFolder.FullName + "; will re-generate it";
                fileMissingFromCache = true;
                return false;
            }

            // Match found; confirm that it has a .hashcheck file and that the information in the .hashcheck file matches the file

            var hashCheckFilePath = fiSourceFile.FullName + clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX;

            if (!clsGlobal.ValidateFileVsHashcheck(fiSourceFile.FullName, hashCheckFilePath, out errorMessage))
            {
                errorMessage = "Cached " + resultFileExtension + " file does not match the hashcheck file in " + disourceFolder.FullName + "; will re-generate it";
                fileMissingFromCache = true;
                return false;
            }

            if (fiSourceFile.Directory == null)
            {
                NotifyInvalidParentDirectory(fiSourceFile);
                return false;
            }

            if (!m_FileCopyUtilities.CopyFileToWorkDir(fiSourceFile.Name, fiSourceFile.Directory.FullName, m_WorkingDir, BaseLogger.LogLevels.ERROR))
            {
                errorMessage = "Error copying " + fiSourceFile.Name;
                return false;
            }

            if (fiSourceFile.Extension.ToLower() == clsAnalysisResources.DOT_GZ_EXTENSION)
            {
                // Do not skip all .gz files because we compress MSGF+ results using .gz and we want to keep those

                m_jobParams.AddResultFileToSkip(fiSourceFile.Name);
                m_jobParams.AddResultFileToSkip(fiSourceFile.Name.Substring(0, fiSourceFile.Name.Length - clsAnalysisResources.DOT_GZ_EXTENSION.Length));

                if (unzip)
                {
                    var localzippedFile = Path.Combine(m_WorkingDir, fiSourceFile.Name);

                    if (!GUnzipFile(localzippedFile))
                    {
                        errorMessage = m_DotNetZipTools.Message;
                        return false;
                    }
                }

            }

            return true;

        }

        /// <summary>
        /// Retrieves file PNNLOmicsElementData.xml from the program directory of the program specified by progLocName
        /// </summary>
        /// <param name="progLocName"></param>
        /// <returns></returns>
        /// <remarks>progLocName is tyipcally DeconToolsProgLoc, LipidToolsProgLoc, or TargetedWorkflowsProgLoc</remarks>
        public bool RetrievePNNLOmicsResourceFiles(string progLocName)
        {

            const string OMICS_ELEMENT_DATA_FILE = "PNNLOmicsElementData.xml";

            try
            {
                var progLoc = m_mgrParams.GetParam(progLocName);
                if (string.IsNullOrEmpty(progLocName))
                {
                    OnErrorEvent("Manager parameter " + progLocName + " is not defined; cannot retrieve file " + OMICS_ELEMENT_DATA_FILE);
                    return false;
                }

                var fiSourceFile = new FileInfo(Path.Combine(progLoc, OMICS_ELEMENT_DATA_FILE));

                if (!fiSourceFile.Exists)
                {
                    OnErrorEvent("PNNLOmics Element Data file not found at: " + fiSourceFile.FullName);
                    return false;
                }

                fiSourceFile.CopyTo(Path.Combine(m_WorkingDir, OMICS_ELEMENT_DATA_FILE));

            }
            catch (Exception ex)
            {
                OnErrorEvent("Error copying " + OMICS_ELEMENT_DATA_FILE, ex);
                return false;
            }

            return true;

        }

        /// <summary>
        /// Retrieves a dataset file for the analysis job in progress; uses the user-supplied extension to match the file
        /// </summary>
        /// <param name="fileExtension">File extension to match; must contain a period, for example ".raw"</param>
        /// <param name="createStoragePathInfoOnly">If true, create a storage path info file</param>
        /// <param name="maxAttempts">Maximum number of attempts</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        private bool RetrieveDatasetFile(string fileExtension, bool createStoragePathInfoOnly, int maxAttempts)
        {

            var DatasetFilePath = m_FolderSearch.FindDatasetFile(maxAttempts, fileExtension);
            if (string.IsNullOrEmpty(DatasetFilePath))
            {
                return false;
            }

            if (DatasetFilePath.StartsWith(MYEMSL_PATH_FLAG))
            {
                // Queue this file for download
                m_MyEMSLUtilities.AddFileToDownloadQueue(m_MyEMSLUtilities.RecentlyFoundMyEMSLFiles.First().FileInfo);
                return true;
            }

            var fiDatasetFile = new FileInfo(DatasetFilePath);
            if (!fiDatasetFile.Exists)
            {
                OnErrorEvent("Source dataset file not found: " + fiDatasetFile.FullName);
                return false;
            }

            if (m_DebugLevel >= 1)
            {
                OnDebugEvent("Retrieving file " + fiDatasetFile.FullName);
            }

            if (m_FileCopyUtilities.CopyFileToWorkDir(fiDatasetFile.Name, fiDatasetFile.DirectoryName, m_WorkingDir,
                BaseLogger.LogLevels.ERROR, createStoragePathInfoOnly))
            {
                return true;
            }

            return false;
        }

        /// <summary>
        /// Retrieves the _DTA.txt file (either zipped or unzipped).
        /// </summary>
        /// <returns>TRUE for success, FALSE for error</returns>
        /// <remarks>If the _dta.zip or _dta.txt file already exists in the working folder then will not re-copy it from the remote folder</remarks>
        public bool RetrieveDtaFiles()
        {

            var targetZipFilePath = Path.Combine(m_WorkingDir, DatasetName + "_dta.zip");
            var targetCDTAFilePath = Path.Combine(m_WorkingDir, DatasetName + "_dta.txt");

            if (!File.Exists(targetCDTAFilePath) && !File.Exists(targetZipFilePath))
            {

                // Find the CDTA file
                var sourceFilePath = FindCDTAFile(out var errorMessage);

                if (string.IsNullOrEmpty(sourceFilePath))
                {
                    OnErrorEvent(errorMessage);
                    return false;
                }

                if (sourceFilePath.StartsWith(MYEMSL_PATH_FLAG))
                {
                    m_MyEMSLUtilities.AddFileToDownloadQueue(sourceFilePath);

                    // ReSharper disable once RedundantNameQualifier
                    if (m_MyEMSLUtilities.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
                    {
                        if (m_DebugLevel >= 1)
                        {
                            OnDebugEvent("Downloaded " + m_MyEMSLUtilities.DownloadedFiles.First().Value.Filename + " from MyEMSL");
                        }
                    }
                    else
                    {
                        return false;
                    }

                }
                else
                {
                    var fiSourceFile = new FileInfo(sourceFilePath);

                    if (fiSourceFile.Directory == null)
                    {
                        NotifyInvalidParentDirectory(fiSourceFile);
                        return false;
                    }

                    // Copy the file locally
                    if (!m_FileCopyUtilities.CopyFileToWorkDir(fiSourceFile.Name, fiSourceFile.Directory.FullName, m_WorkingDir, BaseLogger.LogLevels.ERROR))
                    {
                        if (m_DebugLevel >= 2)
                        {
                            OnStatusEvent("CopyFileToWorkDir returned False for " + fiSourceFile.Name + " using folder " + fiSourceFile.Directory.FullName);
                        }
                        return false;
                    }

                    if (m_DebugLevel >= 1)
                    {
                        OnStatusEvent("Copied " + fiSourceFile.Name + " from folder " + fiSourceFile.FullName);
                    }
                }

            }

            if (File.Exists(targetCDTAFilePath))
                return true;

            if (!File.Exists(targetZipFilePath))
            {
                OnErrorEvent(Path.GetFileName(targetZipFilePath) + " not found in the working directory; cannot unzip in RetrieveDtaFiles");
                return false;
            }

            // Unzip concatenated DTA file
            OnStatusEvent("Unzipping concatenated DTA file");
            if (UnzipFileStart(targetZipFilePath, m_WorkingDir, "RetrieveDtaFiles", false))
            {
                if (m_DebugLevel >= 1)
                {
                    OnDebugEvent("Concatenated DTA file unzipped");
                }
            }

            // Delete the _DTA.zip file to free up some disk space
            Thread.Sleep(100);
            if (m_DebugLevel >= 3)
            {
                OnDebugEvent("Deleting the _DTA.zip file");
            }

            try
            {
                Thread.Sleep(125);
                clsProgRunner.GarbageCollectNow();

                File.Delete(targetZipFilePath);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error deleting the _DTA.zip file", ex);
            }

            return true;

        }

        /// <summary>
        /// This is just a generic function to copy files to the working directory
        /// </summary>
        /// <param name="fileName">Name of file to be copied</param>
        /// <param name="sourceFolderPath">Source folder that has the file</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        public bool RetrieveFile(string fileName, string sourceFolderPath)
        {

            // Copy the file
            if (!m_FileCopyUtilities.CopyFileToWorkDir(fileName, sourceFolderPath, m_WorkingDir, BaseLogger.LogLevels.ERROR))
            {
                return false;
            }

            return true;

        }

        /// <summary>
        /// This is just a generic function to copy files to the working directory
        /// </summary>
        /// <param name="fileName">Name of file to be copied</param>
        /// <param name="sourceFolderPath">Source folder that has the file</param>
        /// <param name="maxCopyAttempts">Maximum number of attempts to make when errors are encountered while copying the file</param>
        /// <param name="logMsgTypeIfNotFound">Type of message to log if the file is not found</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        public bool RetrieveFile(string fileName, string sourceFolderPath, int maxCopyAttempts,
            BaseLogger.LogLevels logMsgTypeIfNotFound = BaseLogger.LogLevels.ERROR)
        {

            // Copy the file
            if (maxCopyAttempts < 1)
                maxCopyAttempts = 1;
            if (!m_FileCopyUtilities.CopyFileToWorkDir(fileName, sourceFolderPath, m_WorkingDir,
                logMsgTypeIfNotFound, createStoragePathInfoOnly: false, maxCopyAttempts: maxCopyAttempts))
            {
                return false;
            }

            return true;

        }

        /// <summary>
        /// Retrieves an Agilent ion trap .mgf file or .cdf/,mgf pair for analysis job in progress
        /// </summary>
        /// <param name="getCdfAlso">TRUE if .cdf file is needed along with .mgf file; FALSE otherwise</param>
        /// <param name="createStoragePathInfoOnly"></param>
        /// <param name="maxAttempts"></param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        private bool RetrieveMgfFile(bool getCdfAlso, bool createStoragePathInfoOnly, int maxAttempts)
        {

            var mgfFilePath = m_FolderSearch.FindMGFFile(maxAttempts, assumeUnpurged: false);

            if (string.IsNullOrEmpty(mgfFilePath))
            {
                OnErrorEvent("Source mgf file not found using FindMGFFile");
                return false;
            }

            var fiMGFFile = new FileInfo(mgfFilePath);
            if (!fiMGFFile.Exists)
            {
                OnErrorEvent("Source mgf file not found: " + fiMGFFile.FullName);
                return false;
            }

            // Do the copy
            if (!m_FileCopyUtilities.CopyFileToWorkDirWithRename(DatasetName, fiMGFFile.Name, fiMGFFile.DirectoryName, m_WorkingDir,
                BaseLogger.LogLevels.ERROR, createStoragePathInfoOnly, maxCopyAttempts: 3))
                return false;

            // If we don't need to copy the .cdf file, we're done; othewise, find the .cdf file and copy it
            if (!getCdfAlso)
                return true;

            if (fiMGFFile.Directory == null)
            {
                NotifyInvalidParentDirectory(fiMGFFile);
                return false;
            }

            foreach (var fiCDFFile in fiMGFFile.Directory.GetFiles("*" + clsAnalysisResources.DOT_CDF_EXTENSION))
            {
                // Copy the .cdf file that was found
                if (m_FileCopyUtilities.CopyFileToWorkDirWithRename(DatasetName, fiCDFFile.Name, fiCDFFile.DirectoryName, m_WorkingDir,
                    BaseLogger.LogLevels.ERROR, createStoragePathInfoOnly, maxCopyAttempts: 3))
                {
                    return true;
                }

                OnErrorEvent("Error obtaining CDF file from " + fiCDFFile.FullName);
                return false;
            }

            // CDF file not found
            OnErrorEvent("CDF File not found");

            return false;

        }

        /// <summary>
        /// Looks for the newest mzXML file for this dataset
        /// First looks for the newest file in \\Proto-11\MSXML_Cache
        /// If not found, looks in the dataset folder, looking for subfolders
        /// MSXML_Gen_1_154_DatasetID, MSXML_Gen_1_93_DatasetID, or MSXML_Gen_1_39_DatasetID (plus some others)
        /// </summary>
        /// <param name="createStoragePathInfoOnly"></param>
        /// <param name="sourceFilePath">Output parameter: Returns the full path to the file that was retrieved</param>
        /// <returns>True if the file was found and retrieved, otherwise False</returns>
        /// <remarks>The retrieved file might be gzipped.  For MzML files, use RetrieveMzMLFile</remarks>
        public bool RetrieveMZXmlFile(bool createStoragePathInfoOnly, out string sourceFilePath)
        {

            sourceFilePath = FindMZXmlFile(out var hashCheckFilePath);

            if (string.IsNullOrEmpty(sourceFilePath))
            {
                return false;
            }

            return RetrieveMZXmlFileUsingSourceFile(createStoragePathInfoOnly, sourceFilePath, hashCheckFilePath);
        }

        /// <summary>
        /// Retrieves this dataset's mzXML or mzML file
        /// </summary>
        /// <param name="createStoragePathInfoOnly"></param>
        /// <param name="sourceFilePath">Full path to the file that should be retrieved</param>
        /// <param name="hashCheckFilePath"></param>
        /// <returns>True if success, false if not retrieved or a hash error</returns>
        /// <remarks></remarks>
        public bool RetrieveMZXmlFileUsingSourceFile(bool createStoragePathInfoOnly, string sourceFilePath, string hashCheckFilePath)
        {

            if (sourceFilePath.StartsWith(MYEMSL_PATH_FLAG))
            {
                return m_MyEMSLUtilities.AddFileToDownloadQueue(sourceFilePath);
            }

            var fiSourceFile = new FileInfo(sourceFilePath);

            if (fiSourceFile.Exists)
            {
                if (fiSourceFile.Directory == null)
                {
                    NotifyInvalidParentDirectory(fiSourceFile);
                    return false;
                }

                if (m_FileCopyUtilities.CopyFileToWorkDir(fiSourceFile.Name, fiSourceFile.Directory.FullName, m_WorkingDir,
                    BaseLogger.LogLevels.ERROR, createStoragePathInfoOnly))
                {
                    if (!string.IsNullOrEmpty(hashCheckFilePath) && File.Exists(hashCheckFilePath))
                    {
                        return RetrieveMzXMLFileVerifyHash(fiSourceFile, hashCheckFilePath, createStoragePathInfoOnly);
                    }

                    return true;
                }
            }

            if (m_DebugLevel >= 1)
            {
                OnStatusEvent("MzXML (or MzML) file not found; will need to generate it: " + fiSourceFile.Name);
            }

            return false;

        }

        /// <summary>
        /// Verify the hash value of a given .mzXML or .mzML file
        /// </summary>
        /// <param name="fiSourceFile"></param>
        /// <param name="hashCheckFilePath"></param>
        /// <param name="createStoragePathInfoOnly"></param>
        /// <returns>True if the hash of the file matches the expected hash, otherwise false</returns>
        /// <remarks>If createStoragePathInfoOnly is true and the source file matches the target file, the hash is not recomputed</remarks>
        private bool RetrieveMzXMLFileVerifyHash(FileSystemInfo fiSourceFile, string hashCheckFilePath, bool createStoragePathInfoOnly)
        {

            string targetFilePath;
            bool computeHash;

            if (createStoragePathInfoOnly)
            {
                targetFilePath = fiSourceFile.FullName;
                // Don't compute the hash, since we're accessing the file over the network
                computeHash = false;
            }
            else
            {
                targetFilePath = Path.Combine(m_WorkingDir, fiSourceFile.Name);
                computeHash = true;
            }

            if (clsGlobal.ValidateFileVsHashcheck(targetFilePath, hashCheckFilePath, out var errorMessage, checkDate: true, computeHash: computeHash))
            {
                return true;
            }

            OnErrorEvent("MzXML/MzML file validation error in RetrieveMzXMLFileVerifyHash: " + errorMessage);

            try
            {
                if (createStoragePathInfoOnly)
                {
                    // Delete the local StoragePathInfo file
                    var storagePathInfoFile = Path.Combine(m_WorkingDir, fiSourceFile.Name + clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX);
                    if (File.Exists(storagePathInfoFile))
                    {
                        File.Delete(storagePathInfoFile);
                    }
                }
                else
                {
                    // Delete the local file to force it to be re-generated
                    File.Delete(targetFilePath);
                }

            }
            catch (Exception)
            {
                // Ignore errors here
            }

            try
            {
                // Delete the remote mzXML or mzML file only if we computed the hash and we had a hash mismatch
                if (computeHash)
                {
                    fiSourceFile.Delete();
                }
            }
            catch (Exception)
            {
                // Ignore errors here
            }

            return false;

        }

        /// <summary>
        /// Retrieves zipped, concatenated OUT file, unzips, and splits into individual OUT files
        /// </summary>
        /// <param name="unConcatenate">TRUE to split concatenated file; FALSE to leave the file concatenated</param>
        /// <returns>TRUE for success, FALSE for error</returns>
        /// <remarks></remarks>
        public bool RetrieveOutFiles(bool unConcatenate)
        {

            // Retrieve zipped OUT file
            var zippedFileName = DatasetName + "_out.zip";
            var zippedFolderName = FindDataFile(zippedFileName);

            if (string.IsNullOrEmpty(zippedFolderName))
                return false;

            // No folder found containing the zipped OUT files
            // Copy the file
            if (!m_FileCopyUtilities.CopyFileToWorkDir(zippedFileName, zippedFolderName, m_WorkingDir, BaseLogger.LogLevels.ERROR))
            {
                return false;
            }

            // Unzip concatenated OUT file
            OnStatusEvent("Unzipping concatenated OUT file");
            if (UnzipFileStart(Path.Combine(m_WorkingDir, zippedFileName), m_WorkingDir, "RetrieveOutFiles", false))
            {
                if (m_DebugLevel >= 1)
                {
                    OnDebugEvent("Concatenated OUT file unzipped");
                }
            }

            // Unconcatenate OUT file if needed
            if (unConcatenate)
            {
                OnStatusEvent("Splitting concatenated OUT file");

                var fiSourceFile = new FileInfo(Path.Combine(m_WorkingDir, DatasetName + "_out.txt"));

                if (!fiSourceFile.Exists)
                {
                    OnErrorEvent("_OUT.txt file not found after unzipping");
                    return false;
                }

                if (fiSourceFile.Length == 0)
                {
                    OnErrorEvent("_OUT.txt file is empty (zero-bytes)");
                    return false;
                }

                var FileSplitter = new clsSplitCattedFiles();
                FileSplitter.SplitCattedOutsOnly(DatasetName, m_WorkingDir);

                if (m_DebugLevel >= 1)
                {
                    OnDebugEvent("Completed splitting concatenated OUT file");
                }
            }

            return true;

        }

        /// <summary>
        /// Looks for this dataset's ScanStats files (previously created by MASIC)
        /// Looks for the files in any SIC folder that exists for the dataset
        /// </summary>
        /// <param name="createStoragePathInfoOnly">If true, creates a storage path info file but doesn't actually copy the files</param>
        /// <returns>True if the file was found and retrieved, otherwise False</returns>
        /// <remarks></remarks>
        public bool RetrieveScanStatsFiles(bool createStoragePathInfoOnly)
        {

            return RetrieveScanAndSICStatsFiles(
                retrieveSICStatsFile: false,
                createStoragePathInfoOnly: createStoragePathInfoOnly,
                retrieveScanStatsFile: true,
                retrieveScanStatsExFile: true);

        }

        /// <summary>
        /// Looks for this dataset's ScanStats files (previously created by MASIC)
        /// Looks for the files in any SIC folder that exists for the dataset
        /// </summary>
        /// <param name="createStoragePathInfoOnly"></param>
        /// <param name="retrieveScanStatsFile">If True, retrieves the ScanStats.txt file</param>
        /// <param name="retrieveScanStatsExFile">If True, retrieves the ScanStatsEx.txt file</param>
        /// <returns>True if the file was found and retrieved, otherwise False</returns>
        /// <remarks></remarks>
        public bool RetrieveScanStatsFiles(bool createStoragePathInfoOnly, bool retrieveScanStatsFile, bool retrieveScanStatsExFile)
        {

            const bool retrieveSICStatsFile = false;
            return RetrieveScanAndSICStatsFiles(retrieveSICStatsFile,
                createStoragePathInfoOnly, retrieveScanStatsFile, retrieveScanStatsExFile);

        }

        /// <summary>
        /// Looks for this dataset's MASIC results files
        /// Looks for the files in any SIC folder that exists for the dataset
        /// </summary>
        /// <param name="retrieveSICStatsFile">If True, also copies the _SICStats.txt file in addition to the ScanStats files</param>
        /// <param name="createStoragePathInfoOnly">If true, creates a storage path info file but doesn't actually copy the files</param>
        /// <param name="retrieveScanStatsFile">If True, retrieves the ScanStats.txt file</param>
        /// <param name="retrieveScanStatsExFile">If True, retrieves the ScanStatsEx.txt file</param>
        /// <returns>True if the file was found and retrieved, otherwise False</returns>
        /// <remarks></remarks>
        public bool RetrieveScanAndSICStatsFiles(
            bool retrieveSICStatsFile,
            bool createStoragePathInfoOnly,
            bool retrieveScanStatsFile,
            bool retrieveScanStatsExFile)
        {

            var lstNonCriticalFileSuffixes = new List<string>();
            const bool RETRIEVE_REPORTERIONS_FILE = false;

            return RetrieveScanAndSICStatsFiles(retrieveSICStatsFile,
                createStoragePathInfoOnly, retrieveScanStatsFile, retrieveScanStatsExFile, RETRIEVE_REPORTERIONS_FILE, lstNonCriticalFileSuffixes);

        }

        /// <summary>
        /// Looks for this dataset's MASIC results files
        /// Looks for the files in any SIC folder that exists for the dataset
        /// </summary>
        /// <param name="retrieveSICStatsFile">If True, also copies the _SICStats.txt file in addition to the ScanStats files</param>
        /// <param name="createStoragePathInfoOnly">If true, creates a storage path info file but doesn't actually copy the files</param>
        /// <param name="retrieveScanStatsFile">If True, retrieves the ScanStats.txt file</param>
        /// <param name="retrieveScanStatsExFile">If True, retrieves the ScanStatsEx.txt file</param>
        /// <param name="retrieveReporterIonsFile">If True, retrieves the ReporterIons.txt file</param>
        /// <param name="lstNonCriticalFileSuffixes">Filename suffixes that can be missing.  For example, "ScanStatsEx.txt"</param>
        /// <returns>True if the file was found and retrieved, otherwise False</returns>
        /// <remarks></remarks>
        public bool RetrieveScanAndSICStatsFiles(
            bool retrieveSICStatsFile,
            bool createStoragePathInfoOnly,
            bool retrieveScanStatsFile,
            bool retrieveScanStatsExFile,
            bool retrieveReporterIonsFile,
            List<string> lstNonCriticalFileSuffixes)
        {
            long bestScanStatsFileTransactionID = 0;

            const int MAX_ATTEMPTS = 1;


            var requiredFileSuffixes = new List<string>();

            if (retrieveSICStatsFile) requiredFileSuffixes.Add(clsAnalysisResources.SIC_STATS_FILE_SUFFIX);
            if (retrieveScanStatsFile) requiredFileSuffixes.Add(clsAnalysisResources.SCAN_STATS_FILE_SUFFIX);
            if (retrieveScanStatsExFile) requiredFileSuffixes.Add(clsAnalysisResources.SCAN_STATS_EX_FILE_SUFFIX);
            if (retrieveReporterIonsFile) requiredFileSuffixes.Add(clsAnalysisResources.REPORTERIONS_FILE_SUFFIX);

            var matchCount = requiredFileSuffixes.Count(fileSuffix => FileExistsInWorkDir(DatasetName + fileSuffix));

            if (matchCount == requiredFileSuffixes.Count)
            {
                // All required MASIC files are already present in the working directory
                return true;
            }

            // Look for the MASIC Results folder
            // If the folder cannot be found, m_FolderSearch.FindValidFolder will return the folder defined by "DatasetStoragePath"
                var scanStatsFilename = DatasetName + clsAnalysisResources.SCAN_STATS_FILE_SUFFIX;
            var serverPath = m_FolderSearch.FindValidFolder(DatasetName, "", "SIC*", MAX_ATTEMPTS, logFolderNotFound: false, retrievingInstrumentDataFolder: false);

            if (string.IsNullOrEmpty(serverPath))
            {
                OnErrorEvent("Dataset folder path not found in RetrieveScanAndSICStatsFiles");
                return false;
            }

            if (serverPath.StartsWith(MYEMSL_PATH_FLAG))
            {
                // Find the newest _ScanStats.txt file in MyEMSL
                var bestSICFolderName = string.Empty;

                foreach (var myEmslFile in m_MyEMSLUtilities.RecentlyFoundMyEMSLFiles)
                {
                    if (myEmslFile.IsFolder)
                    {
                        continue;
                    }

                    if (clsGlobal.IsMatch(myEmslFile.FileInfo.Filename, scanStatsFilename) && myEmslFile.FileInfo.TransactionID > bestScanStatsFileTransactionID)
                    {
                        var fiScanStatsFile = new FileInfo(myEmslFile.FileInfo.RelativePathWindows);

                        if (fiScanStatsFile.Directory == null)
                        {
                            NotifyInvalidParentDirectory(fiScanStatsFile);
                            return false;
                        }
                        bestSICFolderName
                            = fiScanStatsFile.Directory.Name;
                        bestScanStatsFileTransactionID = myEmslFile.FileInfo.TransactionID;
                    }
                }

                if (bestScanStatsFileTransactionID == 0)
                {
                    OnErrorEvent("MASIC ScanStats file not found in the SIC results folder(s) in MyEMSL");
                    return false;
                }

                var bestSICFolderPath = Path.Combine(MYEMSL_PATH_FLAG, bestSICFolderName);
                return RetrieveScanAndSICStatsFiles(
                    bestSICFolderPath, retrieveSICStatsFile, createStoragePathInfoOnly,
                    retrieveScanStatsFile: retrieveScanStatsFile,
                    retrieveScanStatsExFile: retrieveScanStatsExFile,
                    retrieveReporterIonsFile: retrieveReporterIonsFile,
                    lstNonCriticalFileSuffixes: lstNonCriticalFileSuffixes);
            }
            else
            {
                var diFolderInfo = new DirectoryInfo(serverPath);

                if (!diFolderInfo.Exists)
                {
                    OnErrorEvent("Dataset folder with MASIC files not found: " + diFolderInfo.FullName);
                    return false;
                }

                // See if the ServerPath folder actually contains a subfolder that starts with "SIC"
                var diSubfolders = diFolderInfo.GetDirectories("SIC*");
                if (diSubfolders.Length == 0)
                {
                    OnErrorEvent("Dataset folder does not contain any MASIC results folders: " + diFolderInfo.FullName);
                    return false;
                }

                // MASIC Results Folder Found
                // If more than one folder, use the folder with the newest _ScanStats.txt file
                var dtNewestScanStatsFileDate = DateTime.MinValue;
                var newestScanStatsFilePath = string.Empty;

                foreach (var diSubFolder in diSubfolders)
                {
                    var fiScanStatsFile = new FileInfo(Path.Combine(diSubFolder.FullName, scanStatsFilename));
                    if (fiScanStatsFile.Exists)
                    {
                        if (string.IsNullOrEmpty(newestScanStatsFilePath) || fiScanStatsFile.LastWriteTimeUtc > dtNewestScanStatsFileDate)
                        {
                            newestScanStatsFilePath = fiScanStatsFile.FullName;
                            dtNewestScanStatsFileDate = fiScanStatsFile.LastWriteTimeUtc;
                        }
                    }
                }

                if (string.IsNullOrEmpty(newestScanStatsFilePath))
                {
                    OnErrorEvent("MASIC ScanStats file not found below " + diFolderInfo.FullName);
                    return false;
                }

                var fiSourceFile = new FileInfo(newestScanStatsFilePath);

                if (fiSourceFile.Directory == null)
                {
                    NotifyInvalidParentDirectory(fiSourceFile);
                    return false;
                }

                var bestSICFolderPath = fiSourceFile.Directory.FullName;

                return RetrieveScanAndSICStatsFiles(
                    bestSICFolderPath, retrieveSICStatsFile, createStoragePathInfoOnly,
                    retrieveScanStatsFile: retrieveScanStatsFile,
                    retrieveScanStatsExFile: retrieveScanStatsExFile,
                    retrieveReporterIonsFile: retrieveReporterIonsFile,
                    lstNonCriticalFileSuffixes: lstNonCriticalFileSuffixes);
            }

        }

        /// <summary>
        /// Retrieves the MASIC results for this dataset using the specified folder
        /// </summary>
        /// <param name="masicResultsFolderPath">Source folder to copy files from</param>
        /// <param name="retrieveSICStatsFile">If True, also copies the _SICStats.txt file in addition to the ScanStats files</param>
        /// <param name="createStoragePathInfoOnly">If true, creates a storage path info file but doesn't actually copy the files</param>
        /// <param name="retrieveScanStatsFile">If True, retrieves the ScanStats.txt file</param>
        /// <param name="retrieveScanStatsExFile">If True, retrieves the ScanStatsEx.txt file</param>
        /// <param name="retrieveReporterIonsFile">If True, retrieves the ReporterIons.txt file</param>
        /// <param name="lstNonCriticalFileSuffixes">Filename suffixes that can be missing.  For example, "ScanStatsEx.txt"</param>
        /// <returns>True if the file was found and retrieved, otherwise False</returns>
        public bool RetrieveScanAndSICStatsFiles(
            string masicResultsFolderPath,
            bool retrieveSICStatsFile,
            bool createStoragePathInfoOnly,
            bool retrieveScanStatsFile,
            bool retrieveScanStatsExFile,
            bool retrieveReporterIonsFile,
            List<string> lstNonCriticalFileSuffixes)
        {

            const int maxCopyAttempts = 2;

            // Copy the MASIC files from the MASIC results folder

            if (string.IsNullOrEmpty(masicResultsFolderPath))
            {
                OnErrorEvent("MASIC Results folder path not defined in RetrieveScanAndSICStatsFiles");
                return false;
            }

            if (masicResultsFolderPath.StartsWith(MYEMSL_PATH_FLAG))
            {
                var diSICFolder = new DirectoryInfo(masicResultsFolderPath);

                if (retrieveScanStatsFile)
                {
                    // Look for and copy the _ScanStats.txt file
                    if (!RetrieveSICFileMyEMSL(
                        DatasetName + clsAnalysisResources.SCAN_STATS_FILE_SUFFIX,
                        diSICFolder.Name, lstNonCriticalFileSuffixes))
                    {
                        return false;
                    }
                }

                if (retrieveScanStatsExFile)
                {
                    // Look for and copy the _ScanStatsEx.txt file
                    if (!RetrieveSICFileMyEMSL
                        (DatasetName + clsAnalysisResources.SCAN_STATS_EX_FILE_SUFFIX,
                        diSICFolder.Name, lstNonCriticalFileSuffixes))
                    {
                        return false;
                    }
                }

                if (retrieveSICStatsFile)
                {
                    // Look for and copy the _SICStats.txt file
                    if (!RetrieveSICFileMyEMSL(
                        DatasetName + clsAnalysisResources.SIC_STATS_FILE_SUFFIX,
                        diSICFolder.Name, lstNonCriticalFileSuffixes))
                    {
                        return false;
                    }
                }

                if (retrieveReporterIonsFile)
                {
                    // Look for and copy the _SICStats.txt file
                    if (!RetrieveSICFileMyEMSL(
                        DatasetName + "_ReporterIons.txt",
                        diSICFolder.Name, lstNonCriticalFileSuffixes))
                    {
                        return false;
                    }
                }

                // All files have been found
                // The calling process should download them using ProcessMyEMSLDownloadQueue()
                return true;

            }

            var diFolderInfo = new DirectoryInfo(masicResultsFolderPath);

            if (!diFolderInfo.Exists)
            {
                OnErrorEvent("MASIC Results folder not found: " + diFolderInfo.FullName);
                return false;
            }

            if (retrieveScanStatsFile)
            {
                // Look for and copy the _ScanStats.txt file
                if (!RetrieveSICFileUNC(
                    DatasetName + clsAnalysisResources.SCAN_STATS_FILE_SUFFIX,
                    masicResultsFolderPath,
                    createStoragePathInfoOnly, maxCopyAttempts, lstNonCriticalFileSuffixes))
                {
                    return false;
                }
            }

            if (retrieveScanStatsExFile)
            {
                // Look for and copy the _ScanStatsEx.txt file
                if (!RetrieveSICFileUNC(
                    DatasetName + clsAnalysisResources.SCAN_STATS_EX_FILE_SUFFIX,
                    masicResultsFolderPath,
                    createStoragePathInfoOnly, maxCopyAttempts, lstNonCriticalFileSuffixes))
                {
                    return false;
                }
            }

            if (retrieveSICStatsFile)
            {
                // Look for and copy the _SICStats.txt file
                if (!RetrieveSICFileUNC(
                    DatasetName + clsAnalysisResources.SIC_STATS_FILE_SUFFIX,
                    masicResultsFolderPath,
                    createStoragePathInfoOnly, maxCopyAttempts, lstNonCriticalFileSuffixes))
                {
                    return false;
                }
            }

            if (retrieveReporterIonsFile)
            {
                // Look for and copy the _SICStats.txt file
                if (!RetrieveSICFileUNC(
                    DatasetName + "_ReporterIons.txt",
                    masicResultsFolderPath,
                    createStoragePathInfoOnly, maxCopyAttempts, lstNonCriticalFileSuffixes))
                {
                    return false;
                }
            }

            // All files successfully copied
            return true;

        }

        private bool RetrieveSICFileMyEMSL(string fileToFind, string sicFolderName, IReadOnlyCollection<string> lstNonCriticalFileSuffixes)
        {

            var matchingMyEMSLFiles = m_MyEMSLUtilities.FindFiles(fileToFind, sicFolderName, DatasetName, recurse: false);

            if (matchingMyEMSLFiles.Count > 0)
            {
                if (m_DebugLevel >= 3)
                {
                    OnDebugEvent("Found MASIC results file in MyEMSL, " + Path.Combine(sicFolderName, fileToFind));
                }

                m_MyEMSLUtilities.AddFileToDownloadQueue(matchingMyEMSLFiles.First().FileInfo);

            }
            else
            {
                var ignoreFile = SafeToIgnore(fileToFind, lstNonCriticalFileSuffixes);

                if (!ignoreFile)
                {
                    OnErrorEvent(fileToFind + " not found in MyEMSL, subfolder " + sicFolderName);
                    return false;
                }
            }

            return true;

        }

        private bool RetrieveSICFileUNC(
            string fileToFind,
            string MASICResultsFolderPath,
            bool createStoragePathInfoOnly,
            int maxCopyAttempts,
            IReadOnlyCollection<string> lstNonCriticalFileSuffixes)
        {

            var fiSourceFile = new FileInfo(Path.Combine(MASICResultsFolderPath, fileToFind));

            if (m_DebugLevel >= 3)
            {
                OnDebugEvent("Copying MASIC results file: " + fiSourceFile.FullName);
            }

            var ignoreFile = SafeToIgnore(fiSourceFile.Name, lstNonCriticalFileSuffixes);

            BaseLogger.LogLevels logMsgTypeIfNotFound;
            if (ignoreFile)
            {
                logMsgTypeIfNotFound = BaseLogger.LogLevels.DEBUG;
            }
            else
            {
                logMsgTypeIfNotFound = BaseLogger.LogLevels.ERROR;
            }

            if (fiSourceFile.Directory == null)
            {
                NotifyInvalidParentDirectory(fiSourceFile);
                return false;
            }

            var success = m_FileCopyUtilities.CopyFileToWorkDir(
                fiSourceFile.Name, fiSourceFile.Directory.FullName, m_WorkingDir,
                logMsgTypeIfNotFound, createStoragePathInfoOnly, maxCopyAttempts);

            if (success)
                return true;

            if (ignoreFile)
            {
                if (m_DebugLevel >= 3)
                {
                    OnDebugEvent("  File not found; this is not a problem");
                }
            }
            else
            {
                OnErrorEvent(fileToFind + " not found at " + fiSourceFile.Directory.FullName);
                return false;
            }

            return true;

        }

        /// <summary>
        /// Retrieves the spectra file(s) based on raw data type and puts them in the working directory
        /// </summary>
        /// <param name="rawDataType">Type of data to copy</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        public bool RetrieveSpectra(string rawDataType)
        {
            const bool createStoragePathInfoOnly = false;
            return RetrieveSpectra(rawDataType, createStoragePathInfoOnly);
        }

        /// <summary>
        /// Retrieves the spectra file(s) based on raw data type and puts them in the working directory
        /// </summary>
        /// <param name="rawDataType">Type of data to copy</param>
        /// <param name="createStoragePathInfoOnly">
        /// When true, then does not actually copy the dataset file (or folder), and instead creates a file named Dataset.raw_StoragePathInfo.txt,
        /// and this file's first line will be the full path to the spectrum file (or spectrum folder)
        /// </param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        public bool RetrieveSpectra(string rawDataType, bool createStoragePathInfoOnly)
        {
            return RetrieveSpectra(rawDataType, createStoragePathInfoOnly, clsFolderSearch.DEFAULT_MAX_RETRY_COUNT);
        }

        /// <summary>
        /// Retrieves the spectra file(s) based on raw data type and puts them in the working directory
        /// </summary>
        /// <param name="rawDataType">Type of data to copy</param>
        /// <param name="createStoragePathInfoOnly">
        /// When true, does not actually copy the dataset file (or folder), and instead creates a file named Dataset.raw_StoragePathInfo.txt
        /// The first line in the StoragePathInfo file will be the full path to the spectrum file (or spectrum folder)
        /// </param>
        /// <param name="maxAttempts">Maximum number of attempts</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        public bool RetrieveSpectra(string rawDataType, bool createStoragePathInfoOnly, int maxAttempts)
        {

            var success = false;
            var StoragePath = m_jobParams.GetParam("DatasetStoragePath");

            OnStatusEvent("Retrieving spectra file(s)");

            var eRawDataType = clsAnalysisResources.GetRawDataType(rawDataType);
            switch (eRawDataType)
            {
                case clsAnalysisResources.eRawDataTypeConstants.AgilentDFolder:
                    // Agilent ion trap data
                    if (StoragePath.ToLower().Contains("Agilent_SL1".ToLower()) ||
                        StoragePath.ToLower().Contains("Agilent_XCT1".ToLower()))
                    {
                        // For Agilent Ion Trap datasets acquired on Agilent_SL1 or Agilent_XCT1 in 2005,
                        //  we would pre-process the data beforehand to create MGF files
                        // The following call can be used to retrieve the files
                        success = RetrieveMgfFile(getCdfAlso: true, createStoragePathInfoOnly: createStoragePathInfoOnly, maxAttempts: maxAttempts);
                    }
                    else
                    {
                        // DeconTools_V2 now supports reading the .D files directly
                        // Call RetrieveDotDFolder() to copy the folder and all subfolders
                        success = RetrieveDotDFolder(createStoragePathInfoOnly, skipBAFFiles: true);
                    }

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.AgilentQStarWiffFile:
                    // Agilent/QSTAR TOF data
                    success = RetrieveDatasetFile(clsAnalysisResources.DOT_WIFF_EXTENSION, createStoragePathInfoOnly, maxAttempts);

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.ZippedSFolders:
                    // FTICR data
                    success = RetrieveSFolders(createStoragePathInfoOnly, maxAttempts);

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile:
                    // Finnigan ion trap/LTQ-FT data
                    success = RetrieveDatasetFile(clsAnalysisResources.DOT_RAW_EXTENSION, createStoragePathInfoOnly, maxAttempts);

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.MicromassRawFolder:
                    // Micromass QTOF data
                    success = RetrieveDotRawFolder(createStoragePathInfoOnly);

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.UIMF:
                    // IMS UIMF data
                    success = RetrieveDatasetFile(clsAnalysisResources.DOT_UIMF_EXTENSION, createStoragePathInfoOnly, maxAttempts);

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.mzXML:
                    success = RetrieveDatasetFile(clsAnalysisResources.DOT_MZXML_EXTENSION, createStoragePathInfoOnly, maxAttempts);

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.mzML:
                    success = RetrieveDatasetFile(clsAnalysisResources.DOT_MZML_EXTENSION, createStoragePathInfoOnly, maxAttempts);

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.BrukerFTFolder:
                case clsAnalysisResources.eRawDataTypeConstants.BrukerTOFBaf:
                    // Call RetrieveDotDFolder() to copy the folder and all subfolders

                    // Both the MSXml step tool and DeconTools require the .Baf file
                    // We previously didn't need this file for DeconTools, but, now that DeconTools is using CompassXtract, we need the file
                    // In contrast, ICR-2LS only needs the ser or FID file, plus the apexAcquisition.method file in the .md folder

                    var skipBAFFiles = false;

                    var stepTool = m_jobParams.GetJobParameter("StepTool", "Unknown");

                    if (stepTool == "ICR2LS")
                    {
                        skipBAFFiles = true;
                    }

                    success = RetrieveDotDFolder(createStoragePathInfoOnly, skipBAFFiles);

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.BrukerMALDIImaging:
                    success = RetrieveBrukerMALDIImagingFolders(unzipOverNetwork: true);

                    break;
                default:
                    // rawDataType is not recognized or not supported by this function
                    if (eRawDataType == clsAnalysisResources.eRawDataTypeConstants.Unknown)
                    {

                        OnErrorEvent("Invalid data type specified: " + rawDataType);
                    }
                    else
                    {

                        OnErrorEvent("Data type " + rawDataType + " is not supported by the RetrieveSpectra function");
                    }
                    break;
            }

            // Return the result of the spectra retrieval
            return success;
        }

        ///
        /// <summary>
        /// Retrieves an Agilent or Bruker .D folder for the analysis job in progress
        /// </summary>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        private bool RetrieveDotDFolder(bool createStoragePathInfoOnly, bool skipBAFFiles)
        {
            var fileNamesToSkip = new List<string>();
            if (skipBAFFiles)
            {
                fileNamesToSkip.Add("analysis.baf");
            }

            return RetrieveDotXFolder(clsAnalysisResources.DOT_D_EXTENSION, createStoragePathInfoOnly, fileNamesToSkip);
        }

        /// <summary>
        /// Retrieves a Micromass .raw folder for the analysis job in progress
        /// </summary>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        private bool RetrieveDotRawFolder(bool createStoragePathInfoOnly)
        {
            return RetrieveDotXFolder(clsAnalysisResources.DOT_RAW_EXTENSION, createStoragePathInfoOnly, new List<string>());
        }

        /// <summary>
        /// Retrieves a folder with a name like Dataset.D or Dataset.Raw
        /// </summary>
        /// <param name="folderExtension">Extension on the folder; for example, ".D"</param>
        /// <param name="createStoragePathInfoOnly"></param>
        /// <param name="fileNamesToSkip"></param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        private bool RetrieveDotXFolder(
            string folderExtension,
            bool createStoragePathInfoOnly,
            List<string> fileNamesToSkip)
        {

            // Copies a data folder ending in folderExtension to the working directory

            // Find the instrument data folder (e.g. Dataset.D or Dataset.Raw) in the dataset folder
            var DSFolderPath = m_FolderSearch.FindDotXFolder(folderExtension, assumeUnpurged: false);

            if (string.IsNullOrEmpty(DSFolderPath))
            {
                return false;
            }

            if ((DSFolderPath.StartsWith(MYEMSL_PATH_FLAG)))
            {
                // Queue the MyEMSL files for download
                foreach (var udtArchiveFile in m_MyEMSLUtilities.RecentlyFoundMyEMSLFiles)
                {
                    m_MyEMSLUtilities.AddFileToDownloadQueue(udtArchiveFile.FileInfo);
                }
                return true;
            }

            // Do the copy
            try
            {
                var disourceFolder = new DirectoryInfo(DSFolderPath);
                if (!disourceFolder.Exists)
                {
                    OnErrorEvent("Source dataset folder not found: " + disourceFolder.FullName);
                    return false;
                }

                var destFolderPath = Path.Combine(m_WorkingDir, disourceFolder.Name);

                if (createStoragePathInfoOnly)
                {
                    m_FileCopyUtilities.CreateStoragePathInfoFile(disourceFolder.FullName, destFolderPath);
                }
                else
                {
                    // Copy the directory and all subdirectories
                    // Skip any files defined by fileNamesToSkip
                    if (m_DebugLevel >= 1)
                    {
                        OnStatusEvent("Retrieving folder " + disourceFolder.FullName);
                    }

                    m_FileCopyUtilities.CopyDirectory(disourceFolder.FullName, destFolderPath, fileNamesToSkip);

                }

            }
            catch (Exception ex)
            {
                OnErrorEvent("Error copying folder " + DSFolderPath, ex);
                return false;
            }

            // If we get here, all is fine
            return true;

        }

        /// <summary>
        /// Retrieves a data from a Bruker MALDI imaging dataset
        /// The data is stored as zip files with names like 0_R00X433.zip
        /// This data is unzipped into a subfolder in the Chameleon cached data folder
        /// </summary>
        /// <param name="unzipOverNetwork"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public bool RetrieveBrukerMALDIImagingFolders(bool unzipOverNetwork)
        {

            const string ZIPPED_BRUKER_IMAGING_SECTIONS_FILE_MASK = "*R*X*.zip";

            var ChameleonCachedDataFolder = m_mgrParams.GetParam("ChameleonCachedDataFolder");
            DirectoryInfo diCachedDataFolder;

            string unzipFolderPathBase;

            var filesToDelete = new Queue<string>();

            var zipFilePathRemote = string.Empty;

            try
            {
                if (string.IsNullOrEmpty(ChameleonCachedDataFolder))
                {

                    OnErrorEvent("Chameleon cached data folder not defined; unable to unzip MALDI imaging data");
                    return false;
                }

                // Delete any subfolders at ChameleonCachedDataFolder that do not have this dataset's name
                diCachedDataFolder = new DirectoryInfo(ChameleonCachedDataFolder);
                if (!diCachedDataFolder.Exists)
                {

                    OnErrorEvent("Chameleon cached data folder does not exist: " + diCachedDataFolder.FullName);
                    return false;
                }

                unzipFolderPathBase = Path.Combine(diCachedDataFolder.FullName, DatasetName);

                foreach (var diSubFolder in diCachedDataFolder.GetDirectories())
                {
                    if (!clsGlobal.IsMatch(diSubFolder.Name, DatasetName))
                    {
                        // Delete this directory
                        try
                        {
                            if (m_DebugLevel >= 2)
                            {

                                OnDebugEvent("Deleting old dataset subfolder from chameleon cached data folder: " + diSubFolder.FullName);
                            }

                            if (m_mgrParams.ManagerName.ToLower().Contains("monroe"))
                            {

                                OnDebugEvent(" Skipping delete since this is a development computer");
                            }
                            else
                            {
                                diSubFolder.Delete(true);
                            }

                        }
                        catch (Exception ex)
                        {

                            OnErrorEvent("Error deleting cached subfolder " + diSubFolder.FullName, ex);
                            return false;
                        }
                    }
                }

                // Delete any .mis files that do not start with this dataset's name
                foreach (var fiFile in diCachedDataFolder.GetFiles("*.mis"))
                {
                    if (!clsGlobal.IsMatch(Path.GetFileNameWithoutExtension(fiFile.Name), DatasetName))
                    {
                        fiFile.Delete();
                    }
                }

            }
            catch (Exception ex)
            {

                OnErrorEvent("Error cleaning out old data from the Chameleon cached data folder", ex);
                return false;
            }

            // See if any imaging section filters are defined
            var applySectionFilter = GetBrukerImagingSectionFilter(m_jobParams, out var startSectionX, out var endSectionX);

            // Look for the dataset folder; it must contain .Zip files with names like 0_R00X442.zip
            // If a matching folder isn't found, ServerPath will contain the folder path defined by Job Param "DatasetStoragePath"
            var serverPath = m_FolderSearch.FindValidFolder(DatasetName, ZIPPED_BRUKER_IMAGING_SECTIONS_FILE_MASK, RetrievingInstrumentDataFolder: true);

            try
            {
                // Look for the .mis file (ImagingSequence file)
                var imagingSeqFilePathFinal = Path.Combine(diCachedDataFolder.FullName, DatasetName + ".mis");

                if (!File.Exists(imagingSeqFilePathFinal))
                {
                    // Copy the .mis file (ImagingSequence file) over from the storage server
                    var MisFiles = Directory.GetFiles(serverPath, "*.mis");

                    if (MisFiles.Length == 0)
                    {
                        // No .mis files were found; unable to continue
                        OnErrorEvent("ImagingSequence (.mis) file not found in dataset folder; unable to process MALDI imaging data");
                        return false;
                    }

                    // We'll copy the first file in MisFiles[0]
                    // Log a warning if we will be renaming the file

                    if (!clsGlobal.IsMatch(Path.GetFileName(MisFiles[0]), imagingSeqFilePathFinal))
                    {

                        OnDebugEvent("Note: Renaming .mis file (ImagingSequence file) from " + Path.GetFileName(MisFiles[0]) +
                            " to " + Path.GetFileName(imagingSeqFilePathFinal));
                    }

                    if (!m_FileCopyUtilities.CopyFileWithRetry(MisFiles[0], imagingSeqFilePathFinal, true))
                    {
                        // Abort processing
                        OnErrorEvent("Error copying ImagingSequence (.mis) file; unable to process MALDI imaging data");
                        return false;
                    }
                }
            }
            catch (Exception ex)
            {

                OnErrorEvent("Error obtaining ImagingSequence (.mis) file", ex);
                return false;
            }

            try
            {
                // Unzip each of the *R*X*.zip files to the Chameleon cached data folder

                // However, consider limits defined by job params BrukerMALDI_Imaging_startSectionX and BrukerMALDI_Imaging_endSectionX
                // when processing the files

                var ZipFiles = Directory.GetFiles(serverPath, ZIPPED_BRUKER_IMAGING_SECTIONS_FILE_MASK);

                var reRegExRXY = new Regex(@"R(?<R>\d+)X(?<X>\d+)Y(?<Y>\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
                var reRegExRX = new Regex(@"R(?<R>\d+)X(?<X>\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                foreach (var zipFilePath in ZipFiles)
                {
                    zipFilePathRemote = zipFilePath;

                    bool unzipFile;
                    if (applySectionFilter)
                    {
                        unzipFile = false;

                        // Determine the R, X, and Y coordinates for this .Zip file

                        if (GetBrukerImagingFileCoords(zipFilePathRemote, reRegExRXY, reRegExRX, out _, out var coordX, out _))
                        {
                            // Compare to startSectionX and endSectionX
                            if (coordX >= startSectionX && coordX <= endSectionX)
                            {
                                unzipFile = true;
                            }
                        }
                    }
                    else
                    {
                        unzipFile = true;
                    }

                    if (!unzipFile)
                        continue;

                    // Open up the zip file over the network and get a listing of all of the files
                    // If they already exist in the cached data folder, there is no need to continue

                    // Set this to false for now
                    unzipFile = false;

                    var remoteZipFile = new Ionic.Zip.ZipFile(zipFilePathRemote);

                    foreach (var entry in remoteZipFile.Entries)
                    {

                        if (!entry.IsDirectory)
                        {
                            var pathToCheck = Path.Combine(unzipFolderPathBase, entry.FileName.Replace('/', '\\'));

                            if (!File.Exists(pathToCheck))
                            {
                                unzipFile = true;
                                break;
                            }
                        }
                    }

                    if (!unzipFile)
                        continue;


                    // Unzip the file to the Chameleon cached data folder
                    // If unzipOverNetwork=True, we want to copy the file locally first

                    string zipFilePathToExtract;
                    if (unzipOverNetwork)
                    {
                        zipFilePathToExtract = string.Copy(zipFilePathRemote);
                    }
                    else
                    {

                        try
                        {
                            // Copy the file to the work directory on the local computer
                            var sourceFileName = Path.GetFileName(zipFilePathRemote);
                            if (string.IsNullOrEmpty(sourceFileName))
                            {
                                OnErrorEvent("Unable to determine the filename of the remote zip file: " + zipFilePathRemote);
                                return false;
                            }

                            zipFilePathToExtract = Path.Combine(m_WorkingDir, sourceFileName);

                            if (m_DebugLevel >= 2)
                            {
                                OnDebugEvent("Copying " + zipFilePathRemote);
                            }

                            if (!m_FileCopyUtilities.CopyFileWithRetry(zipFilePathRemote, zipFilePathToExtract, true))
                            {
                                // Abort processing
                                OnErrorEvent("Error copying Zip file; unable to process MALDI imaging data");
                                return false;
                            }

                        }
                        catch (Exception ex)
                        {

                            OnErrorEvent("Error copying zipped instrument data, file " + zipFilePathRemote, ex);
                            return false;
                        }
                    }

                    // Now use DotNetZip (aka Ionic.Zip) to unzip zipFilePathLocal to the data cache folder
                    // Do not overwrite existing files (assume they're already valid)

                    try
                    {
                        using (var zipFile = new Ionic.Zip.ZipFile(zipFilePathToExtract))
                        {
                            if (m_DebugLevel >= 2)
                            {
                                OnDebugEvent("Unzipping " + zipFilePathToExtract);
                            }

                            zipFile.ExtractAll(unzipFolderPathBase, Ionic.Zip.ExtractExistingFileAction.DoNotOverwrite);
                        }

                    }
                    catch (Exception ex)
                    {

                        OnErrorEvent("Error extracting zipped instrument data, file " + zipFilePathToExtract, ex);
                        return false;
                    }

                    if (!unzipOverNetwork)
                    {
                        // Need to delete the zip file that we copied locally
                        // However, DotNet may have a file handle open so we use a queue to keep track of files that need to be deleted

                        DeleteQueuedFiles(filesToDelete, zipFilePathToExtract);
                    }
                }

                if (!unzipOverNetwork)
                {
                    var dtStartTime = DateTime.UtcNow;

                    while (filesToDelete.Count > 0)
                    {
                        // Try to process the files remaining in queue filesToDelete

                        DeleteQueuedFiles(filesToDelete, string.Empty);

                        if (filesToDelete.Count > 0)
                        {
                            if (DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds > 20)
                            {
                                // Stop trying to delete files; it's not worth continuing to try
                                OnWarningEvent("Unable to delete all of the files in queue filesToDelete; " +
                                    "Queue Length = " + filesToDelete.Count + "; " +
                                    "this warning can be safely ignored (function RetrieveBrukerMALDIImagingFolders)");
                                break;
                            }

                            Thread.Sleep(500);
                        }
                    }

                }

            }
            catch (Exception ex)
            {

                OnErrorEvent("Error extracting zipped instrument data from " + zipFilePathRemote, ex);
                return false;
            }

            // If we get here, all is fine
            return true;
        }

        /// <summary>
        /// Unzips dataset folders to working directory
        /// </summary>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        private bool RetrieveSFolders(bool createStoragePathInfoOnly, int maxAttempts)
        {

            try
            {
                // First Check for the existence of a 0.ser Folder
                // If 0.ser folder exists, either store the path to the 0.ser folder in a StoragePathInfo file, or copy the 0.ser folder to the working directory
                var DSFolderPath = m_FolderSearch.FindValidFolder(DatasetName, fileNameToFind: "", folderNameToFind:
                    clsAnalysisResources.BRUKER_ZERO_SER_FOLDER, maxRetryCount: maxAttempts, logFolderNotFound: true, retrievingInstrumentDataFolder: true);

                if (!string.IsNullOrEmpty(DSFolderPath))
                {
                    var disourceFolder = new DirectoryInfo(Path.Combine(DSFolderPath, clsAnalysisResources.BRUKER_ZERO_SER_FOLDER));

                    if (disourceFolder.Exists)
                    {
                        if (createStoragePathInfoOnly)
                        {
                            if (m_FileCopyUtilities.CreateStoragePathInfoFile(disourceFolder.FullName, m_WorkingDir + @"\"))
                            {
                                return true;
                            }
                            return false;
                        }

                        // Copy the 0.ser folder to the Work directory
                        // First create the 0.ser subfolder
                        var diTargetFolder = Directory.CreateDirectory(Path.Combine(m_WorkingDir, clsAnalysisResources.BRUKER_ZERO_SER_FOLDER));

                        // Now copy the files from the source 0.ser folder to the target folder
                        // Typically there will only be two files: ACQUS and ser
                        foreach (var fiFile in disourceFolder.GetFiles())
                        {
                            if (!m_FileCopyUtilities.CopyFileToWorkDir(fiFile.Name, disourceFolder.FullName, diTargetFolder.FullName))
                            {
                                // Error has alredy been logged
                                return false;
                            }
                        }

                        return true;
                    }

                }

                // If the 0.ser folder does not exist, unzip the zipped s-folders
                // Copy the zipped s-folders from archive to work directory
                if (!CopySFoldersToWorkDir(createStoragePathInfoOnly))
                {
                    // Error messages have already been logged, so just exit
                    return false;
                }

                if (createStoragePathInfoOnly)
                {
                    // Nothing was copied locally, so nothing to unzip
                    return true;
                }

                // Get a listing of the zip files to process
                var zipFiles = Directory.GetFiles(m_WorkingDir, "s*.zip");
                if (zipFiles.GetLength(0) < 1)
                {
                    OnErrorEvent("No zipped s-folders found in working directory");
                    return false;
                }

                // Create a dataset subdirectory under the working directory
                var datasetWorkFolder = Path.Combine(m_WorkingDir, DatasetName);
                Directory.CreateDirectory(datasetWorkFolder);

                // Set up the unzipper
                var dotNetZipTools = new clsDotNetZipTools(m_DebugLevel, datasetWorkFolder);
                RegisterEvents(dotNetZipTools);

                // Unzip each of the zip files to the working directory
                foreach (var zipFilePath in zipFiles)
                {
                    if (m_DebugLevel > 3)
                    {
                        OnDebugEvent("Unzipping file " + zipFilePath);
                    }

                    try
                    {
                        var sourceFileName = Path.GetFileName(zipFilePath);
                        var fileNameBase = Path.GetFileNameWithoutExtension(zipFilePath);

                        if (string.IsNullOrEmpty(sourceFileName) || string.IsNullOrEmpty(fileNameBase))
                        {
                            OnErrorEvent("Unable to determine the filename of the zip file: " + zipFilePath);
                            return false;
                        }

                        var targetFolderPath = Path.Combine(datasetWorkFolder, fileNameBase);
                        Directory.CreateDirectory(targetFolderPath);

                        var sourceFilePath = Path.Combine(m_WorkingDir, sourceFileName);

                        if (!dotNetZipTools.UnzipFile(sourceFilePath, targetFolderPath))
                        {
                            OnErrorEvent("Error unzipping file " + zipFilePath);
                            return false;
                        }
                    }
                    catch (Exception ex)
                    {
                        OnErrorEvent("Exception while unzipping s-folders", ex);
                        return false;
                    }
                }

                Thread.Sleep(125);
                clsProgRunner.GarbageCollectNow();

                // Delete all s*.zip files in working directory
                foreach (var zipFilePath in zipFiles)
                {
                    try
                    {
                        var targetFileName = Path.GetFileName(zipFilePath);

                        if (string.IsNullOrEmpty(targetFileName))
                        {
                            OnWarningEvent("Unable to determine the filename of the zip file: " + zipFilePath);
                            continue;
                        }

                        File.Delete(Path.Combine(m_WorkingDir, targetFileName));
                    }
                    catch (Exception ex)
                    {
                        OnErrorEvent("Exception deleting file " + zipFilePath, ex);
                        return false;
                    }
                }

            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in RetrieveSFolders", ex);
                return false;
            }

            // Got to here, so everything must have worked
            return true;

        }

        /// <summary>
        /// Returns True if the filename ends with any of the suffixes in lstNonCriticalFileSuffixes
        /// </summary>
        /// <param name="fileName"></param>
        /// <param name="lstNonCriticalFileSuffixes"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool SafeToIgnore(string fileName, IReadOnlyCollection<string> lstNonCriticalFileSuffixes)
        {

            if ((lstNonCriticalFileSuffixes != null))
            {
                foreach (var suffix in lstNonCriticalFileSuffixes)
                {
                    if (fileName.EndsWith(suffix, StringComparison.OrdinalIgnoreCase))
                    {
                        // It's OK that this file is missing
                        return true;
                    }
                }

            }

            return false;

        }

        /// <summary>
        /// Unzips all files in the specified Zip file
        /// </summary>
        /// <param name="zipFilePath">File to unzip</param>
        /// <param name="outFolderPath">Target directory for the extracted files</param>
        /// <param name="callingFunctionName">Calling function name (used for debugging purposes)</param>
        /// <param name="forceExternalZipProgramUse">If True, force use of PKZipC.exe</param>
        /// <returns>True if success, otherwise false</returns>
        /// <remarks></remarks>
        public bool UnzipFileStart(string zipFilePath, string outFolderPath, string callingFunctionName, bool forceExternalZipProgramUse)
        {
            var unzipperName = "??";

            try
            {
                if (string.IsNullOrEmpty(callingFunctionName))
                {
                    callingFunctionName = "??";
                }

                if (zipFilePath == null)
                {

                    OnErrorEvent(callingFunctionName + " called UnzipFileStart with an empty file path");
                    return false;
                }

                var fiFileInfo = new FileInfo(zipFilePath);

                if (!fiFileInfo.Exists)
                {
                    // File not found
                    OnErrorEvent("Error unzipping '" + zipFilePath + "': File not found");
                    OnStatusEvent("CallingFunction: " + callingFunctionName);
                    return false;
                }

                if (zipFilePath.EndsWith(clsAnalysisResources.DOT_GZ_EXTENSION, StringComparison.OrdinalIgnoreCase))
                {
                    // This is a gzipped file
                    // Use DotNetZip
                    unzipperName = clsDotNetZipTools.DOTNET_ZIP_NAME;
                    m_DotNetZipTools.DebugLevel = m_DebugLevel;
                    return m_DotNetZipTools.GUnzipFile(zipFilePath, outFolderPath);
                }

                // Use DotNetZip
                unzipperName = clsDotNetZipTools.DOTNET_ZIP_NAME;
                m_DotNetZipTools.DebugLevel = m_DebugLevel;
                var success = m_DotNetZipTools.UnzipFile(zipFilePath, outFolderPath);

                return success;

            }
            catch (Exception ex)
            {
                var errMsg = "Exception while unzipping '" + zipFilePath + "'";
                if (!string.IsNullOrEmpty(unzipperName))
                    errMsg += " using " + unzipperName;

                OnErrorEvent(errMsg, ex);
                OnStatusEvent("CallingFunction: " + callingFunctionName);

                return false;
            }
        }

        #endregion

    }
}
