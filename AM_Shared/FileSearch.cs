using MyEMSLReader;
using PHRPReader;
using PRISM;
using PRISM.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

// ReSharper disable UnusedMember.Global
namespace AnalysisManagerBase
{
    /// <summary>
    /// File search methods
    /// </summary>
    public class FileSearch : EventNotifier
    {
        // Ignore Spelling: Finalizers, msgfdb, Deconcatenate, ser, gzipping, Workflows, dta, mgf, CompassXtract

        #region "Constants"

        private const string MYEMSL_PATH_FLAG = MyEMSLUtilities.MYEMSL_PATH_FLAG;

        #endregion

        #region "Module variables"

        private readonly bool mAuroraAvailable;

        private readonly int mDebugLevel;

        private readonly string mWorkDir;

        private readonly IMgrParams mMgrParams;

        private readonly IJobParams mJobParams;

        private readonly FileCopyUtilities mFileCopyUtilities;

        private readonly DirectorySearch mDirectorySearch;

        private readonly MyEMSLUtilities mMyEMSLUtilities;

        private readonly DotNetZipTools mDotNetZipTools;

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
        /// <param name="directorySearch"></param>
        /// <param name="myEmslUtilities"></param>
        /// <param name="mgrParams"></param>
        /// <param name="jobParams"></param>
        /// <param name="datasetName"></param>
        /// <param name="debugLevel"></param>
        /// <param name="workingDir"></param>
        /// <param name="auroraAvailable"></param>
        public FileSearch(
            FileCopyUtilities fileCopyUtilities,
            DirectorySearch directorySearch,
            MyEMSLUtilities myEmslUtilities,
            IMgrParams mgrParams,
            IJobParams jobParams,
            string datasetName,
            short debugLevel,
            string workingDir,
            bool auroraAvailable)
        {
            mFileCopyUtilities = fileCopyUtilities;
            mDirectorySearch = directorySearch;
            mMyEMSLUtilities = myEmslUtilities;
            mMgrParams = mgrParams;
            mJobParams = jobParams;
            DatasetName = datasetName;
            mDebugLevel = debugLevel;
            mWorkDir = workingDir;
            mAuroraAvailable = auroraAvailable;

            mDotNetZipTools = new DotNetZipTools(debugLevel, workingDir);
            RegisterEvents(mDotNetZipTools);
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
        private bool CopySFoldersToWorkDir(bool createStoragePathInfoOnly)
        {
            var datasetDirectoryPath = mDirectorySearch.FindValidDirectory(DatasetName, "s*.zip",
                                                                           retrievingInstrumentDataDir: true);

            // Verify dataset directory exists
            if (!Directory.Exists(datasetDirectoryPath))
                return false;

            // Get a listing of the zip files to process
            var zipFiles = Directory.GetFiles(datasetDirectoryPath, "s*.zip");
            if (zipFiles.GetLength(0) < 1)
            {
                // No zipped data files found
                return false;
            }

            // Copy each of the s*.zip files to the working directory

            foreach (var sourceZipFile in zipFiles)
            {
                if (mDebugLevel > 3)
                {
                    OnDebugEvent("Copying file " + sourceZipFile + " to work directory");
                }
                var sourceFileName = Path.GetFileName(sourceZipFile);
                if (string.IsNullOrEmpty(sourceFileName))
                {
                    OnErrorEvent("Unable to determine the filename of zip file " + sourceZipFile);
                    return false;
                }

                var destinationFilePath = Path.Combine(mWorkDir, sourceFileName);

                if (createStoragePathInfoOnly)
                {
                    if (!mFileCopyUtilities.CreateStoragePathInfoFile(sourceZipFile, destinationFilePath))
                    {
                        OnErrorEvent("Error creating storage path info file for " + sourceZipFile);
                        return false;
                    }
                }
                else
                {
                    if (!mFileCopyUtilities.CopyFileWithRetry(sourceZipFile, destinationFilePath, false))
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
        protected void DeleteQueuedFiles(Queue<string> filesToDelete, string fileToQueueForDeletion)
        {
            if (filesToDelete.Count > 0)
            {
                // Call the garbage collector, then try to delete the first queued file
                // Note, do not call WaitForPendingFinalizers since that could block this thread
                // Thus, do not use PRISM.ProgRunner.GarbageCollectNow
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
            var fileInfo = new FileInfo(Path.Combine(mWorkDir, fileName));
            return fileInfo.Exists;
        }

        /// <summary>
        /// Retrieves specified file from storage server, xfer directory, or archive and unzips if necessary
        /// </summary>
        /// <param name="fileName">Name of file to be retrieved</param>
        /// <param name="unzip">TRUE if retrieved file should be unzipped after retrieval</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks>Logs an error if the file is not found</remarks>
        public bool FindAndRetrieveMiscFiles(string fileName, bool unzip)
        {
            return FindAndRetrieveMiscFiles(fileName, unzip, searchArchivedDatasetDir: true);
        }

        /// <summary>
        /// Retrieves specified file from storage server, xfer directory, or archive and unzips if necessary
        /// </summary>
        /// <param name="fileName">Name of file to be retrieved</param>
        /// <param name="unzip">TRUE if retrieved file should be unzipped after retrieval</param>
        /// <param name="searchArchivedDatasetDir">TRUE if the EMSL archive (Aurora) should also be searched</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks>Logs an error if the file is not found</remarks>
        public bool FindAndRetrieveMiscFiles(string fileName, bool unzip, bool searchArchivedDatasetDir)
        {
            return FindAndRetrieveMiscFiles(fileName, unzip, searchArchivedDatasetDir, out _);
        }

        /// <summary>
        /// Retrieves specified file from storage server, xfer directory, or archive and unzips if necessary
        /// </summary>
        /// <param name="fileName">Name of file to be retrieved</param>
        /// <param name="unzip">TRUE if retrieved file should be unzipped after retrieval</param>
        /// <param name="searchArchivedDatasetDir">TRUE if the EMSL archive (Aurora) should also be searched</param>
        /// <param name="logFileNotFound">True if an error should be logged when a file is not found</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        public bool FindAndRetrieveMiscFiles(string fileName, bool unzip, bool searchArchivedDatasetDir, bool logFileNotFound)
        {
            return FindAndRetrieveMiscFiles(fileName, unzip, searchArchivedDatasetDir, out _, logFileNotFound);
        }

        /// <summary>
        /// Retrieves specified file from storage server, xfer directory, or archive and unzips if necessary
        /// </summary>
        /// <param name="fileName">Name of file to be retrieved</param>
        /// <param name="unzip">TRUE if retrieved file should be unzipped after retrieval</param>
        /// <param name="searchArchivedDatasetDir">TRUE if the EMSL archive (Aurora) should also be searched</param>
        /// <param name="sourceDirPath">Output parameter: the directory from which the file was copied</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks>Logs an error if the file is not found</remarks>
        public bool FindAndRetrieveMiscFiles(string fileName, bool unzip, bool searchArchivedDatasetDir, out string sourceDirPath)
        {
            return FindAndRetrieveMiscFiles(fileName, unzip, searchArchivedDatasetDir, out sourceDirPath, logFileNotFound: true);
        }

        /// <summary>
        /// Retrieves specified file from storage server, xfer directory, or archive and unzips if necessary
        /// </summary>
        /// <param name="fileName">Name of file to be retrieved</param>
        /// <param name="unzip">TRUE if retrieved file should be unzipped after retrieval</param>
        /// <param name="searchArchivedDatasetDir">TRUE if the EMSL archive (Aurora) should also be searched</param>
        /// <param name="sourceDirPath">Output parameter: the directory from which the file was copied</param>
        /// <param name="logFileNotFound">True if an error should be logged when a file is not found</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        private bool FindAndRetrieveMiscFiles(
            string fileName, bool unzip, bool searchArchivedDatasetDir,
            out string sourceDirPath, bool logFileNotFound)
        {
            const bool CreateStoragePathInfoFile = false;

            // Look for the file in the various directories
            // A message will be logged if the file is not found
            sourceDirPath = FindDataFile(fileName, searchArchivedDatasetDir, logFileNotFound);

            // Exit if file was not found
            if (string.IsNullOrEmpty(sourceDirPath))
            {
                // No directory found containing the specified file
                sourceDirPath = string.Empty;
                return false;
            }

            if (sourceDirPath.StartsWith(MYEMSL_PATH_FLAG))
            {
                return mMyEMSLUtilities.AddFileToDownloadQueue(sourceDirPath);
            }

            // Copy the file
            if (!mFileCopyUtilities.CopyFileToWorkDir(fileName, sourceDirPath, mWorkDir, BaseLogger.LogLevels.ERROR, CreateStoragePathInfoFile))
            {
                return false;
            }

            // Check whether unzipping was requested
            if (!unzip)
                return true;

            OnStatusEvent("Unzipping file " + fileName);
            if (UnzipFileStart(Path.Combine(mWorkDir, fileName), mWorkDir, "FindAndRetrieveMiscFiles"))
            {
                if (mDebugLevel >= 3)
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
        /// <param name="addToResultFileSkipList">If true, add the filename to the list of files to skip copying to the result directory</param>
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
                fileToGet, unzip: false, searchArchivedDatasetDir: true, logFileNotFound: logFileNotFound);

            if (!success && fileToGet.IndexOf("msgfplus", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                if (string.IsNullOrEmpty(synopsisFileName))
                    synopsisFileName = "Dataset_msgfdb.txt";
                var alternativeName = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(fileToGet, synopsisFileName);

                if (!string.Equals(alternativeName, fileToGet))
                {
                    success = FindAndRetrieveMiscFiles(
                        alternativeName, unzip: false, searchArchivedDatasetDir: true, logFileNotFound: logFileNotFound);

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
                mJobParams.AddResultFileToSkip(fileToGet);
            }

            return true;
        }

        /// <summary>
        /// Finds the _DTA.txt file for this dataset
        /// </summary>
        /// <returns>The path to the _dta.zip file (or _dta.txt file)</returns>
        public string FindCDTAFile(out string errorMessage)
        {
            errorMessage = string.Empty;

            // Retrieve zipped DTA file
            var sourceFileName = DatasetName + AnalysisResources.CDTA_ZIPPED_EXTENSION;
            var sourceDirPath = FindDataFile(sourceFileName);

            if (!string.IsNullOrEmpty(sourceDirPath))
            {
                if (sourceDirPath.StartsWith(MYEMSL_PATH_FLAG))
                {
                    // add the _dta.zip file name to the directory path found by FindDataFile
                    return MyEMSLUtilities.AddFileToMyEMSLDirectoryPath(sourceDirPath, sourceFileName);
                }

                // Return the path to the _dta.zip file
                return Path.Combine(sourceDirPath, sourceFileName);
            }

            // Couldn't find a directory with the _dta.zip file; how about the _dta.txt file?

            sourceFileName = DatasetName + AnalysisResources.CDTA_EXTENSION;
            sourceDirPath = FindDataFile(sourceFileName);

            if (string.IsNullOrEmpty(sourceDirPath))
            {
                // No directory found containing the zipped DTA files; return False
                // (the FindDataFile procedure should have already logged an error)
                errorMessage = "Could not find " + sourceFileName + " using FindDataFile";
                return string.Empty;
            }

            OnWarningEvent("Warning: could not find the _dta.zip file, but was able to find " + sourceFileName + " in directory " + sourceDirPath);

            if (sourceDirPath.StartsWith(MYEMSL_PATH_FLAG))
            {
                return sourceDirPath;
            }

            // Return the path to the _dta.txt file
            return Path.Combine(sourceDirPath, sourceFileName);
        }

        /// <summary>
        /// Finds the server or archive directory where specified file is located
        /// </summary>
        /// <param name="fileToFind">Name of the file to search for</param>
        /// <returns>Path to the directory containing the file if the file was found; empty string if not found</returns>
        /// <remarks>If the file is found in MyEMSL, the directory path returned will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
        public string FindDataFile(string fileToFind)
        {
            return FindDataFile(fileToFind, searchArchivedDatasetDir: true);
        }

        /// <summary>
        /// Finds the server or archive directory where specified file is located
        /// </summary>
        /// <param name="fileToFind">Name of the file to search for</param>
        /// <param name="searchArchivedDatasetDir">
        /// TRUE if the EMSL archive (Aurora) or MyEMSL should also be searched
        /// (mAuroraAvailable and MyEMSLSearchDisabled take precedence)
        /// </param>
        /// <param name="logFileNotFound">True if an error should be logged when a file is not found</param>
        /// <returns>Path to the directory containing the file if the file was found; empty string if not found</returns>
        /// <remarks>If the file is found in MyEMSL, the directory path returned will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
        public string FindDataFile(string fileToFind, bool searchArchivedDatasetDir, bool logFileNotFound = true)
        {
            try
            {
                // Fill collection with possible directory locations
                // The order of searching is:
                //  a. Check the "inputDirectoryName" and then each of the Shared Results Directories in the Transfer directory
                //  b. Check the "inputDirectoryName" and then each of the Shared Results Directories in the dataset directory
                //  c. Check the "inputDirectoryName" and then each of the Shared Results Directories in MyEMSL for this dataset
                //  d. Check the "inputDirectoryName" and then each of the Shared Results Directories in the Archived dataset directory

                var datasetDirectoryName = mJobParams.GetParam(AnalysisResources.JOB_PARAM_DATASET_FOLDER_NAME);
                var inputDirectoryName = mJobParams.GetParam(AnalysisResources.JOB_PARAM_INPUT_FOLDER_NAME);

                var sharedResultDirNames = GetSharedResultDirList().ToList();

                var parentDirPaths = new List<string> {
                    mJobParams.GetParam(AnalysisResources.JOB_PARAM_TRANSFER_FOLDER_PATH),
                    mJobParams.GetParam("DatasetStoragePath")};

                if (searchArchivedDatasetDir)
                {
                    if (!MyEMSLSearchDisabled)
                    {
                        parentDirPaths.Add(MYEMSL_PATH_FLAG);
                    }
                    if (mAuroraAvailable)
                    {
                        parentDirPaths.Add(mJobParams.GetParam("DatasetArchivePath"));
                    }
                }

                var directoriesToSearch = new List<string>();

                foreach (var parentDirPath in parentDirPaths)
                {
                    if (!string.IsNullOrEmpty(parentDirPath))
                    {
                        if (!string.IsNullOrEmpty(inputDirectoryName))
                        {
                            // Parent directory \ Dataset directory \ Input directory
                            directoriesToSearch.Add(FindDataFileAddFolder(parentDirPath, datasetDirectoryName, inputDirectoryName));
                        }

                        foreach (var sharedDirName in sharedResultDirNames)
                        {
                            // Parent directory \ Dataset directory \ Shared results directory
                            directoriesToSearch.Add(FindDataFileAddFolder(parentDirPath, datasetDirectoryName, sharedDirName));
                        }

                        // Parent directory \ Dataset directory
                        directoriesToSearch.Add(FindDataFileAddFolder(parentDirPath, datasetDirectoryName, string.Empty));
                    }
                }

                var matchingDirectoryPath = string.Empty;
                var matchFound = false;

                // Now search for FileToFind in each directory in directoriesToSearch
                foreach (var directoryPath in directoriesToSearch)
                {
                    try
                    {
                        var directoryToCheck = new DirectoryInfo(directoryPath);

                        if (directoryPath.StartsWith(MYEMSL_PATH_FLAG))
                        {
                            var matchingMyEMSLFiles = mMyEMSLUtilities.FindFiles(fileToFind, directoryToCheck.Name, DatasetName, recurse: false);

                            if (matchingMyEMSLFiles.Count > 0)
                            {
                                matchFound = true;

                                // Include the MyEMSL FileID in TempDir so that it is available for downloading
                                matchingDirectoryPath = DatasetInfoBase.AppendMyEMSLFileID(directoryPath, matchingMyEMSLFiles.First().FileID);
                                break;
                            }
                        }
                        else
                        {
                            if (directoryToCheck.Exists)
                            {
                                if (File.Exists(Path.Combine(directoryPath, fileToFind)))
                                {
                                    matchFound = true;
                                    matchingDirectoryPath = directoryPath;
                                    break;
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        // Exception checking TempDir; log an error, but continue checking the other directories in directoriesToSearch
                        OnErrorEvent("Exception in FindDataFile looking for: " + fileToFind + " in " + directoryPath, ex);
                    }
                }

                if (matchFound)
                {
                    if (mDebugLevel >= 2)
                    {
                        OnDebugEvent("Data file found: " + fileToFind);
                    }
                    return matchingDirectoryPath;
                }

                // Data file not found
                // Log this as an error if searchArchivedDatasetDir=True
                // Log this as a warning if searchArchivedDatasetDir=False

                if (logFileNotFound)
                {
                    if (searchArchivedDatasetDir || (!mAuroraAvailable && MyEMSLSearchDisabled))
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

        private string FindDataFileAddFolder(string parentDirPath, string datasetDirName, string inputDirName)
        {
            var targetDirPath = Path.Combine(parentDirPath, datasetDirName);
            if (!string.IsNullOrEmpty(inputDirName))
            {
                targetDirPath = Path.Combine(targetDirPath, inputDirName);
            }

            return targetDirPath;
        }

        /// <summary>
        /// Looks for the file in directoryPath or any of its subdirectories
        /// The filename may contain a wildcard character, in which case the first match will be returned
        /// </summary>
        /// <param name="directoryPath">Folder path to examine</param>
        /// <param name="fileName">File name to find</param>
        /// <returns>Full path to the file, if found; empty string if no match</returns>
        public static string FindFileInDirectoryTree(string directoryPath, string fileName)
        {
            return FindFileInDirectoryTree(directoryPath, fileName, new SortedSet<string>());
        }

        /// <summary>
        /// Looks for the file in directoryPath or any of its subdirectories
        /// The filename may contain a wildcard character, in which case the first match will be returned
        /// </summary>
        /// <param name="directoryPath">Directory path to examine</param>
        /// <param name="fileName">File name to find</param>
        /// <param name="directoryNamesToSkip">List of directory names that should not be examined</param>
        /// <returns>Full path to the file, if found; empty string if no match</returns>
        public static string FindFileInDirectoryTree(string directoryPath, string fileName, SortedSet<string> directoryNamesToSkip)
        {
            var targetDirectory = new DirectoryInfo(directoryPath);

            if (targetDirectory.Exists)
            {
                // Examine the files for this directory
                foreach (var matchingFile in targetDirectory.GetFiles(fileName))
                {
                    var filePathMatch = matchingFile.FullName;
                    return filePathMatch;
                }

                // Match not found
                // Recursively call this function with the subdirectories in this directory

                foreach (var subDirectory in targetDirectory.GetDirectories())
                {
                    if (!directoryNamesToSkip.Contains(subDirectory.Name))
                    {
                        var filePathMatch = FindFileInDirectoryTree(subDirectory.FullName, fileName);
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
        /// Looks for the newest .mzXML or .mzML file for this dataset
        /// </summary>
        /// <param name="hashCheckFilePath">Output parameter: path to the hashcheck file if the .mzXML or .mzML file was found in the MSXml cache</param>
        /// <returns>Full path to the file, if found; empty string if no match</returns>
        /// <remarks>Supports both gzipped mzXML/mzML files and unzipped ones (gzipping was enabled in September 2014)</remarks>
        public string FindMZXmlFile(out string hashCheckFilePath)
        {
            // First look in the MsXML cache directory
            var matchingFilePath = FindMsXmlFileInCache(AnalysisResources.MSXMLOutputTypeConstants.mzXML, out hashCheckFilePath);

            if (!string.IsNullOrEmpty(matchingFilePath))
            {
                return matchingFilePath;
            }

            // Not found in the cache; look in the dataset directory

            var datasetID = mJobParams.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, "DatasetID");

            const string MSXmlDirectoryNameBase = "MSXML_Gen_1_";
            var mzXMLFilename = DatasetName + ".mzXML";

            const int MAX_ATTEMPTS = 1;

            // Initialize the values we'll look for
            // Note that these values are added to the list in the order of the preferred file to retrieve
            var valuesToCheck = new List<int>
            {
                //         Example directory name       CentroidMSXML  MSXMLGenerator   CentroidPeakCount    MSXMLOutputType
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
                var msXmlDirectoryName = MSXmlDirectoryNameBase + version + "_" + datasetID;

                // Look for the MSXml directory
                // If the directory cannot be found, mDirectorySearch.FindValidDirectory will return the directory defined by "DatasetStoragePath"
                var msXmlServerPath = mDirectorySearch.FindValidDirectory(DatasetName, "", msXmlDirectoryName, MAX_ATTEMPTS,
                                                                          logDirectoryNotFound: false,
                                                                          retrievingInstrumentDataDir: false);

                if (string.IsNullOrEmpty(msXmlServerPath))
                {
                    continue;
                }

                if (msXmlServerPath.StartsWith(MYEMSL_PATH_FLAG))
                {
                    // File found in MyEMSL
                    // Determine the MyEMSL FileID by searching for the expected file in mMyEMSLUtilities.RecentlyFoundMyEMSLFiles

                    long myEmslFileID = 0;

                    foreach (var archivedFileInfo in mMyEMSLUtilities.RecentlyFoundMyEMSLFiles)
                    {
                        var archivedFile = new FileInfo(archivedFileInfo.FileInfo.RelativePathWindows);
                        if (Global.IsMatch(archivedFile.Name, mzXMLFilename))
                        {
                            myEmslFileID = archivedFileInfo.FileID;
                            break;
                        }
                    }

                    if (myEmslFileID > 0)
                    {
                        return Path.Combine(msXmlServerPath, msXmlDirectoryName, DatasetInfoBase.AppendMyEMSLFileID(mzXMLFilename, myEmslFileID));
                    }
                }
                else
                {
                    // Due to quirks with how mDirectorySearch.FindValidDirectory behaves, we need to confirm that the mzXML file actually exists
                    var msXmlServerDirectory = new DirectoryInfo(msXmlServerPath);

                    if (msXmlServerDirectory.Exists)
                    {
                        // See if the ServerPath directory actually contains a subdirectory named MSXmlFolderName
                        var subDirectories = msXmlServerDirectory.GetDirectories(msXmlDirectoryName);

                        if (subDirectories.Length > 0)
                        {
                            // MSXml directory found; return the path to the file
                            return Path.Combine(subDirectories[0].FullName, mzXMLFilename);
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
            AnalysisResources.MSXMLOutputTypeConstants msXmlType,
            out string hashCheckFilePath)
        {
            var msXMLFilename = string.Copy(DatasetName);
            hashCheckFilePath = string.Empty;

            // ReSharper disable once SwitchStatementHandlesSomeKnownEnumValuesWithDefault
            // ReSharper disable once ConvertSwitchStatementToSwitchExpression
            switch (msXmlType)
            {
                case AnalysisResources.MSXMLOutputTypeConstants.mzXML:
                    msXMLFilename += AnalysisResources.DOT_MZXML_EXTENSION + AnalysisResources.DOT_GZ_EXTENSION;
                    break;

                case AnalysisResources.MSXMLOutputTypeConstants.mzML:
                    // All MzML files should be gzipped
                    msXMLFilename += AnalysisResources.DOT_MZML_EXTENSION + AnalysisResources.DOT_GZ_EXTENSION;
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(msXmlType), "Unsupported enum value for MSXMLOutputTypeConstants: " + msXmlType);
            }

            // Lookup the MSXML cache path (typically \\Proto-11\MSXML_Cache)
            var msXmlCacheDirectoryPath = mMgrParams.GetParam(AnalysisResources.JOB_PARAM_MSXML_CACHE_FOLDER_PATH, string.Empty);

            var msXmlCacheDirectory = new DirectoryInfo(msXmlCacheDirectoryPath);

            if (!msXmlCacheDirectory.Exists)
            {
                OnWarningEvent("Warning: MsXML cache directory not found: " + msXmlCacheDirectoryPath);
                return string.Empty;
            }

            // Determine the YearQuarter code for this dataset
            var datasetStoragePath = mJobParams.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, "DatasetStoragePath");
            if (string.IsNullOrEmpty(datasetStoragePath) && (mAuroraAvailable || !MyEMSLSearchDisabled))
            {
                datasetStoragePath = mJobParams.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, "DatasetArchivePath");
            }

            var yearQuarter = AnalysisResources.GetDatasetYearQuarter(datasetStoragePath);

            var matchingFiles = new List<FileInfo>();

            if (string.IsNullOrEmpty(yearQuarter))
            {
                // Perform an exhaustive recursive search of the MSXML file cache
                var filesToAppend = msXmlCacheDirectory.GetFiles(msXMLFilename, SearchOption.AllDirectories);

                if (filesToAppend.Length == 0 && msXmlType == AnalysisResources.MSXMLOutputTypeConstants.mzXML)
                {
                    // Older .mzXML files were not gzipped
                    filesToAppend = msXmlCacheDirectory.GetFiles(DatasetName + AnalysisResources.DOT_MZXML_EXTENSION, SearchOption.AllDirectories);
                }

                var query = (from item in filesToAppend orderby item.LastWriteTimeUtc descending select item).Take(1);

                matchingFiles.AddRange(query);
            }
            else
            {
                // Look for the file in the top level subDirectories of the MSXML file cache
                foreach (var toolDirectory in msXmlCacheDirectory.GetDirectories())
                {
                    var subDirectories = toolDirectory.GetDirectories(yearQuarter);

                    if (subDirectories.Length > 0)
                    {
                        var filesToAppend = subDirectories.First().GetFiles(msXMLFilename, SearchOption.TopDirectoryOnly);
                        if (filesToAppend.Length == 0 && msXmlType == AnalysisResources.MSXMLOutputTypeConstants.mzXML)
                        {
                            // Older .mzXML files were not gzipped
                            filesToAppend = subDirectories.First().GetFiles(DatasetName + AnalysisResources.DOT_MZXML_EXTENSION, SearchOption.TopDirectoryOnly);
                        }

                        var query = (from item in filesToAppend orderby item.LastWriteTimeUtc descending select item).Take(1);
                        matchingFiles.AddRange(query);
                    }
                }
            }

            if (matchingFiles.Count == 0)
            {
                return string.Empty;
            }

            // One or more matches were found; select the newest one
            var sortQuery = (from item in matchingFiles orderby item.LastWriteTimeUtc descending select item).Take(1);
            var dataFilePath = sortQuery.First().FullName;

            // Confirm that the file has a .hashcheck file and that the information in the .hashcheck file matches the file
            hashCheckFilePath = dataFilePath + Global.SERVER_CACHE_HASHCHECK_FILE_SUFFIX;

            const int recheckIntervalDays = 1;

            var validFile = FileSyncUtils.ValidateFileVsHashcheck(dataFilePath, hashCheckFilePath, out var errorMessage, HashUtilities.HashTypeConstants.MD5, recheckIntervalDays);

            if (validFile)
            {
                return dataFilePath;
            }

            OnWarningEvent("Warning: " + errorMessage);
            return string.Empty;
        }

        /// <summary>
        /// Split apart coordinates that look like "R00X438Y093" into R, X, and Y
        /// </summary>
        /// <param name="coordinate"></param>
        /// <param name="rxyMatcher"></param>
        /// <param name="rxMatcher"></param>
        /// <param name="R"></param>
        /// <param name="X"></param>
        /// <param name="Y"></param>
        /// <returns>True if success, false otherwise</returns>
        private bool GetBrukerImagingFileCoords(string coordinate, Regex rxyMatcher, Regex rxMatcher, out int R, out int X, out int Y)
        {
            // Try to match names like R00X438Y093
            var reMatch = rxyMatcher.Match(coordinate);

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
                reMatch = rxMatcher.Match(coordinate);

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
        /// Examines job parameter SharedResultsFolders (JOB_PARAM_SHARED_RESULTS_FOLDERS) to construct a list of the shared result directories
        /// </summary>
        /// <returns>List of directory names</returns>
        /// <remarks>
        /// Job param "SharedResultsFolders" typically only contains one directory path,
        /// but it can contain a comma-separated list of directory paths.
        /// </remarks>
        private IEnumerable<string> GetSharedResultDirList()
        {
            var sharedResultDirNames = new List<string>();

            var sharedResultsDirectoryList = mJobParams.GetParam(AnalysisResources.JOB_PARAM_SHARED_RESULTS_FOLDERS);

            if (sharedResultsDirectoryList.Contains(","))
            {
                // Split on commas and populate sharedResultDirNames
                foreach (var item in sharedResultsDirectoryList.Split(','))
                {
                    var itemTrimmed = item.Trim();
                    if (itemTrimmed.Length > 0)
                    {
                        sharedResultDirNames.Add(itemTrimmed);
                    }
                }

                // Reverse the list so that the last item in sharedResultDirNames is the first item in sharedResultsDirectoryList
                sharedResultDirNames.Reverse();
            }
            else
            {
                // Just one item in sharedResultsDirectoryList
                sharedResultDirNames.Add(sharedResultsDirectoryList);
            }

            return sharedResultDirNames;
        }

        /// <summary>
        /// Unzip gzipFilePath into the working directory
        /// Existing files will be overwritten
        /// </summary>
        /// <param name="gzipFilePath">.gz file to unzip</param>
        /// <returns>True if success; false if an error</returns>
        public bool GUnzipFile(string gzipFilePath)
        {
            return mDotNetZipTools.GUnzipFile(gzipFilePath);
        }

        private void NotifyInvalidParentDirectory(FileSystemInfo sourceFile)
        {
            OnErrorEvent("Unable to determine the parent directory of " + sourceFile.FullName);
        }

        /// <summary>
        /// Retrieve the dataset's cached .mzML file from the MsXML Cache
        /// </summary>
        /// <param name="unzip">True to unzip; otherwise, will remain as a .gzip file</param>
        /// <param name="errorMessage">Output parameter: Error message</param>
        /// <param name="fileMissingFromCache">Output parameter: will be True if the file was not found in the cache</param>
        /// <param name="sourceDirectoryPath">Output parameter: source directory path</param>
        /// <returns>True if success, false if an error or file not found</returns>
        /// <remarks>
        /// Uses the job's InputFolderName parameter to dictate which subDirectory to search at \\Proto-11\MSXML_Cache
        /// InputFolderName should be in the form MSXML_Gen_1_93_367204
        /// </remarks>
        public bool RetrieveCachedMzMLFile(bool unzip, out string errorMessage, out bool fileMissingFromCache, out string sourceDirectoryPath)
        {
            const bool callingMethodCanRegenerateMissingFile = false;

            return RetrieveCachedMSXMLFile(
                AnalysisResources.DOT_MZML_EXTENSION, unzip, callingMethodCanRegenerateMissingFile,
                out errorMessage, out fileMissingFromCache, out sourceDirectoryPath);
        }

        /// <summary>
        /// Retrieve the dataset's cached .mzXML file from the MsXML Cache
        /// </summary>
        /// <param name="unzip">True to unzip; otherwise, will remain as a .gzip file</param>
        /// <param name="errorMessage">Output parameter: Error message</param>
        /// <param name="fileMissingFromCache">Output parameter: will be True if the file was not found in the cache</param>
        /// <param name="sourceDirectoryPath">Output parameter: source directory path</param>
        /// <returns>True if success, false if an error or file not found</returns>
        /// <remarks>
        /// Uses the job's InputFolderName parameter to dictate which subDirectory to search at \\Proto-11\MSXML_Cache
        /// InputFolderName should be in the form MSXML_Gen_1_105_367204
        /// </remarks>
        public bool RetrieveCachedMzXMLFile(bool unzip, out string errorMessage, out bool fileMissingFromCache, out string sourceDirectoryPath)
        {
            const bool callingMethodCanRegenerateMissingFile = false;

            return RetrieveCachedMSXMLFile(
                AnalysisResources.DOT_MZXML_EXTENSION, unzip, callingMethodCanRegenerateMissingFile,
                out errorMessage, out fileMissingFromCache, out sourceDirectoryPath);
        }

        /// <summary>
        /// Retrieve the dataset's cached .PBF file from the MsXML Cache
        /// </summary>
        /// <param name="errorMessage">Output parameter: Error message</param>
        /// <param name="fileMissingFromCache">Output parameter: will be True if the file was not found in the cache</param>
        /// <param name="sourceDirectoryPath">Output parameter: source directory path</param>
        /// <returns>True if success, false if an error or file not found</returns>
        /// <remarks>
        /// Uses the job's InputFolderName parameter to dictate which subDirectory to search at \\Proto-11\MSXML_Cache
        /// InputFolderName should be in the form MSXML_Gen_1_93_367204
        /// </remarks>
        public bool RetrieveCachedPBFFile(out string errorMessage, out bool fileMissingFromCache, out string sourceDirectoryPath)
        {
            const bool unzip = false;
            const bool callingMethodCanRegenerateMissingFile = false;

            return RetrieveCachedMSXMLFile(
                AnalysisResources.DOT_PBF_EXTENSION, unzip, callingMethodCanRegenerateMissingFile,
                out errorMessage, out fileMissingFromCache, out sourceDirectoryPath);
        }

        /// <summary>
        /// Retrieve the dataset's cached .mzXML, .mzML, or .pbf file from the MsXML Cache
        /// </summary>
        /// <param name="resultFileExtension">File extension to retrieve (.mzXML or .mzML)</param>
        /// <param name="unzip">True to unzip; otherwise, will remain as a .gzip file</param>
        /// <param name="callingMethodCanRegenerateMissingFile">True if the calling method has logic defined for generating the .mzML file if it is not found</param>
        /// <param name="errorMessage">Output parameter: Error message</param>
        /// <param name="fileMissingFromCache">Output parameter: will be True if the file was not found in the cache</param>
        /// <param name="sourceDirectoryPath">Output parameter: source directory path</param>
        /// <returns>True if success, false if an error or file not found</returns>
        /// <remarks>
        /// Uses the job's InputFolderName parameter to dictate which subDirectory to search at \\Proto-11\MSXML_Cache
        /// InputFolderName should be in the form MSXML_Gen_1_93_367204
        /// </remarks>
        public bool RetrieveCachedMSXMLFile(
            string resultFileExtension,
            bool unzip,
            bool callingMethodCanRegenerateMissingFile,
            out string errorMessage,
            out bool fileMissingFromCache,
            out string sourceDirectoryPath)
        {
            errorMessage = string.Empty;
            fileMissingFromCache = false;
            sourceDirectoryPath = string.Empty;

            if (string.IsNullOrEmpty(resultFileExtension))
            {
                errorMessage = "resultFileExtension is empty; should be .mzXML or .mzML";
                return false;
            }

            if (Global.OfflineMode)
            {
                // Look for the .mzML file in the working directory
                var localMsXmlFile = new FileInfo(Path.Combine(mWorkDir, DatasetName + resultFileExtension));
                if (localMsXmlFile.Exists)
                {
                    OnStatusEvent(string.Format("Using {0} file {1} in {2}", resultFileExtension, localMsXmlFile.Name, mWorkDir));
                    sourceDirectoryPath = mWorkDir;
                    return true;
                }

                var localMsXmlGzFile = new FileInfo(localMsXmlFile.FullName + AnalysisResources.DOT_GZ_EXTENSION);
                if (!localMsXmlGzFile.Exists)
                {
                    errorMessage = string.Format(
                        "Could not find a {0} file or {1} file for this dataset in the working directory",
                        resultFileExtension, resultFileExtension + AnalysisResources.DOT_GZ_EXTENSION);

                    OnWarningEvent(errorMessage);
                    return false;
                }

                sourceDirectoryPath = mWorkDir;

                if (!unzip)
                    return true;

                if (GUnzipFile(localMsXmlGzFile.FullName))
                    return true;

                errorMessage = mDotNetZipTools.Message;
                return false;
            }

            var msXMLCacheDirPath = mMgrParams.GetParam(AnalysisResources.JOB_PARAM_MSXML_CACHE_FOLDER_PATH, string.Empty);

            if (string.IsNullOrWhiteSpace(msXMLCacheDirPath))
            {
                errorMessage = string.Format("Manager parameter {0} is not defined",
                                             AnalysisResources.JOB_PARAM_MSXML_CACHE_FOLDER_PATH);
                return false;
            }

            var msXmlCacheDir = new DirectoryInfo(msXMLCacheDirPath);

            if (!msXmlCacheDir.Exists)
            {
                errorMessage = "MSXmlCache directory not found: " + msXMLCacheDirPath;
                return false;
            }

            var directoriesToSearch = new List<string> {
                mJobParams.GetJobParameter(AnalysisResources.JOB_PARAM_INPUT_FOLDER_NAME, string.Empty)
            };

            if (directoriesToSearch[0].Length == 0)
            {
                directoriesToSearch.Clear();
            }

            foreach (var sharedResultDirectory in GetSharedResultDirList())
            {
                if (string.IsNullOrWhiteSpace(sharedResultDirectory))
                    continue;

                if (!directoriesToSearch.Contains(sharedResultDirectory))
                {
                    directoriesToSearch.Add(sharedResultDirectory);
                }
            }

            if (directoriesToSearch.Count == 0)
            {
                // Job parameters InputFolderName and SharedResultsFolders are empty; cannot retrieve the .mzML file
                errorMessage = string.Format("Job parameters {0} and {1} are empty; cannot retrieve the {2} file",
                                             AnalysisResources.JOB_PARAM_INPUT_FOLDER_NAME,
                                             AnalysisResources.JOB_PARAM_SHARED_RESULTS_FOLDERS,
                                             resultFileExtension);
                return false;
            }

            var msXmlToolNameVersionDirs = new List<string>();

            foreach (var directoryName in directoriesToSearch)
            {
                try
                {
                    // Remove the DatasetID suffix on directoryName
                    var msXmlToolNameVersionDirectory = AnalysisResources.GetMSXmlToolNameVersionFolder(directoryName);
                    msXmlToolNameVersionDirs.Add(msXmlToolNameVersionDirectory);
                }
                catch (Exception)
                {
                    // Directory in job param InputFolderName or SharedResultsFolders is not in the expected form of ToolName_Version_DatasetID
                    errorMessage = string.Format("Directory in job param {0} or {1} is not in the expected form of ToolName_Version_DatasetID ({2}); " +
                                                 "will not try to find the {3} file in this directory",
                                                 AnalysisResources.JOB_PARAM_INPUT_FOLDER_NAME,
                                                 AnalysisResources.JOB_PARAM_SHARED_RESULTS_FOLDERS,
                                                 directoryName,
                                                 resultFileExtension);

                    OnDebugEvent(errorMessage);
                }
            }

            if (msXmlToolNameVersionDirs.Count == 0)
            {
                if (string.IsNullOrEmpty(errorMessage))
                {
                    errorMessage = string.Format("Directories in job params {0} and {1} were not in the expected form of ToolName_Version_DatasetID",
                                                 AnalysisResources.JOB_PARAM_INPUT_FOLDER_NAME,
                                                 AnalysisResources.JOB_PARAM_SHARED_RESULTS_FOLDERS);
                }
                return false;
            }

            errorMessage = string.Empty;

            DirectoryInfo sourceDirectory = null;
            var sourceFilePath = string.Empty;

            foreach (var toolNameVersionDir in msXmlToolNameVersionDirs)
            {
                var candidateSourceDir = AnalysisResources.GetMSXmlCacheFolderPath(msXmlCacheDir.FullName, mJobParams, toolNameVersionDir, out errorMessage);
                if (!string.IsNullOrEmpty(errorMessage))
                {
                    continue;
                }

                sourceDirectory = new DirectoryInfo(candidateSourceDir);
                if (sourceDirectory.Exists)
                {
                    var candidateFilePath = Path.Combine(sourceDirectory.FullName, DatasetName + resultFileExtension);
                    if (File.Exists(candidateFilePath))
                    {
                        sourceFilePath = candidateFilePath;
                        break;
                    }

                    if (!string.Equals(resultFileExtension, AnalysisResources.DOT_PBF_EXTENSION, StringComparison.OrdinalIgnoreCase))
                    {
                        var candidateGzFilePath = candidateFilePath + AnalysisResources.DOT_GZ_EXTENSION;
                        if (File.Exists(candidateGzFilePath))
                        {
                            sourceFilePath = candidateGzFilePath;
                            break;
                        }
                    }

                    continue;
                }

                if (string.IsNullOrEmpty(errorMessage))
                {
                    errorMessage = "Cache directory does not exist (" + candidateSourceDir;
                }
                else
                {
                    errorMessage += " or " + candidateSourceDir;
                }
            }

            if (sourceDirectory?.Exists != true)
            {
                errorMessage += ")";
                fileMissingFromCache = true;
                return false;
            }

            if (string.IsNullOrWhiteSpace(sourceFilePath))
            {
                errorMessage = "msXML file not found in the source directory(s): " + string.Join(",", msXmlToolNameVersionDirs);
                fileMissingFromCache = true;
                return false;
            }

            var expectedFileDescription = resultFileExtension;
            if (!string.Equals(resultFileExtension, AnalysisResources.DOT_PBF_EXTENSION, StringComparison.OrdinalIgnoreCase))
            {
                expectedFileDescription += AnalysisResources.DOT_GZ_EXTENSION;
            }

            var sourceFile = new FileInfo(sourceFilePath);
            if (!sourceFile.Exists)
            {
                errorMessage = string.Format("Cached {0} file does not exist in {1}", expectedFileDescription, sourceDirectory.FullName);

                if (callingMethodCanRegenerateMissingFile)
                {
                    errorMessage += "; will re-generate it";
                }
                else
                {
                    errorMessage += "; you must manually re-create it";
                }

                fileMissingFromCache = true;
                return false;
            }

            sourceDirectoryPath = sourceFile.DirectoryName;

            // Match found; confirm that it has a .hashcheck file and that the information in the .hashcheck file matches the file

            var hashCheckFilePath = sourceFile.FullName + Global.SERVER_CACHE_HASHCHECK_FILE_SUFFIX;

            const int recheckIntervalDays = 1;

            var validFile = FileSyncUtils.ValidateFileVsHashcheck(sourceFile.FullName, hashCheckFilePath, out errorMessage, HashUtilities.HashTypeConstants.MD5, recheckIntervalDays);

            if (!validFile)
            {
                errorMessage = string.Format("Cached {0} file does not match the hashcheck file in {1}",
                    resultFileExtension,
                    sourceDirectory.FullName);

                if (callingMethodCanRegenerateMissingFile)
                {
                    errorMessage += "; will re-generate it";
                }
                else
                {
                    errorMessage += "; you must manually re-create it";
                }

                fileMissingFromCache = true;
                return false;
            }

            if (sourceFile.Directory == null)
            {
                NotifyInvalidParentDirectory(sourceFile);
                return false;
            }

            if (!mFileCopyUtilities.CopyFileToWorkDir(sourceFile.Name, sourceFile.Directory.FullName, mWorkDir, BaseLogger.LogLevels.ERROR))
            {
                errorMessage = "Error copying " + sourceFile.FullName;
                return false;
            }

            OnStatusEvent(string.Format("Copied {0} to {1}", sourceFile.FullName, mWorkDir));

            // If this is not a .gz file, return true
            if (!string.Equals(sourceFile.Extension, AnalysisResources.DOT_GZ_EXTENSION, StringComparison.OrdinalIgnoreCase))
                return true;

            // Do not skip all .gz files because we compress MS-GF+ results using .gz and we want to keep those

            mJobParams.AddResultFileToSkip(sourceFile.Name);
            mJobParams.AddResultFileToSkip(sourceFile.Name.Substring(0, sourceFile.Name.Length - AnalysisResources.DOT_GZ_EXTENSION.Length));

            if (!unzip)
                return true;

            var localZippedFile = Path.Combine(mWorkDir, sourceFile.Name);

            if (GUnzipFile(localZippedFile))
                return true;

            errorMessage = mDotNetZipTools.Message;
            return false;
        }

        /// <summary>
        /// Retrieves file PNNLOmicsElementData.xml from the program directory of the program specified by progLocName
        /// </summary>
        /// <param name="progLocName"></param>
        /// <remarks>progLocName is typically DeconToolsProgLoc, LipidToolsProgLoc, or TargetedWorkflowsProgLoc</remarks>
        public bool RetrievePNNLOmicsResourceFiles(string progLocName)
        {
            const string OMICS_ELEMENT_DATA_FILE = "PNNLOmicsElementData.xml";

            try
            {
                var progLoc = mMgrParams.GetParam(progLocName);
                if (string.IsNullOrEmpty(progLocName))
                {
                    OnErrorEvent("Manager parameter " + progLocName + " is not defined; cannot retrieve file " + OMICS_ELEMENT_DATA_FILE);
                    return false;
                }

                var sourceFile = new FileInfo(Path.Combine(progLoc, OMICS_ELEMENT_DATA_FILE));

                if (!sourceFile.Exists)
                {
                    OnErrorEvent("PNNLOmics Element Data file not found at: " + sourceFile.FullName);
                    return false;
                }

                sourceFile.CopyTo(Path.Combine(mWorkDir, OMICS_ELEMENT_DATA_FILE));
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
        private bool RetrieveDatasetFile(string fileExtension, bool createStoragePathInfoOnly, int maxAttempts)
        {
            var datasetFilePath = mDirectorySearch.FindDatasetFile(maxAttempts, fileExtension);
            if (string.IsNullOrEmpty(datasetFilePath))
            {
                return false;
            }

            if (datasetFilePath.StartsWith(MYEMSL_PATH_FLAG))
            {
                // Queue this file for download
                mMyEMSLUtilities.AddFileToDownloadQueue(mMyEMSLUtilities.RecentlyFoundMyEMSLFiles.First().FileInfo);
                return true;
            }

            var datasetFile = new FileInfo(datasetFilePath);
            if (!datasetFile.Exists)
            {
                OnErrorEvent("Source dataset file not found: " + datasetFile.FullName);
                return false;
            }

            if (mDebugLevel >= 1)
            {
                OnDebugEvent("Retrieving file " + datasetFile.FullName);
            }

            return mFileCopyUtilities.CopyFileToWorkDir(
                datasetFile.Name, datasetFile.DirectoryName,
                mWorkDir, BaseLogger.LogLevels.ERROR, createStoragePathInfoOnly);
        }

        /// <summary>
        /// Retrieves the _DTA.txt file (either zipped or unzipped).
        /// </summary>
        /// <returns>TRUE for success, FALSE for error</returns>
        /// <remarks>If the _dta.zip or _dta.txt file already exists in the working directory, will not re-copy it from the remote directory</remarks>
        public bool RetrieveDtaFiles()
        {
            var targetZipFilePath = Path.Combine(mWorkDir, DatasetName + AnalysisResources.CDTA_ZIPPED_EXTENSION);
            var targetCDTAFilePath = Path.Combine(mWorkDir, DatasetName + AnalysisResources.CDTA_EXTENSION);

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
                    mMyEMSLUtilities.AddFileToDownloadQueue(sourceFilePath);

                    // ReSharper disable once RedundantNameQualifier
                    if (mMyEMSLUtilities.ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
                    {
                        if (mDebugLevel >= 1)
                        {
                            OnDebugEvent("Downloaded " + mMyEMSLUtilities.DownloadedFiles.First().Value.Filename + " from MyEMSL");
                        }
                    }
                    else
                    {
                        return false;
                    }
                }
                else
                {
                    var sourceFile = new FileInfo(sourceFilePath);

                    if (sourceFile.Directory == null)
                    {
                        NotifyInvalidParentDirectory(sourceFile);
                        return false;
                    }

                    // Copy the file locally
                    if (!mFileCopyUtilities.CopyFileToWorkDir(sourceFile.Name, sourceFile.Directory.FullName, mWorkDir, BaseLogger.LogLevels.ERROR))
                    {
                        if (mDebugLevel >= 2)
                        {
                            OnStatusEvent("CopyFileToWorkDir returned False for " + sourceFile.Name + " using directory " + sourceFile.Directory.FullName);
                        }
                        return false;
                    }

                    if (mDebugLevel >= 1)
                    {
                        OnStatusEvent("Copied " + sourceFile.Name + " from directory " + sourceFile.FullName);
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
            if (UnzipFileStart(targetZipFilePath, mWorkDir, "RetrieveDtaFiles"))
            {
                if (mDebugLevel >= 1)
                {
                    OnDebugEvent("Concatenated DTA file unzipped");
                }
            }

            // Delete the _DTA.zip file to free up some disk space
            if (mDebugLevel >= 3)
            {
                OnDebugEvent("Deleting the _DTA.zip file");
            }

            try
            {
                ProgRunner.GarbageCollectNow();
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
        /// <param name="sourceDirectoryPath">Source directory that has the file</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        public bool RetrieveFile(string fileName, string sourceDirectoryPath)
        {
            // Copy the file
            return mFileCopyUtilities.CopyFileToWorkDir(
                fileName, sourceDirectoryPath, mWorkDir, BaseLogger.LogLevels.ERROR);
        }

        /// <summary>
        /// This is just a generic function to copy files to the working directory
        /// </summary>
        /// <param name="fileName">Name of file to be copied</param>
        /// <param name="sourceDirectoryPath">Source directory that has the file</param>
        /// <param name="maxCopyAttempts">Maximum number of attempts to make when errors are encountered while copying the file</param>
        /// <param name="logMsgTypeIfNotFound">Type of message to log if the file is not found</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        public bool RetrieveFile(string fileName, string sourceDirectoryPath, int maxCopyAttempts,
            BaseLogger.LogLevels logMsgTypeIfNotFound = BaseLogger.LogLevels.ERROR)
        {
            // Copy the file
            if (maxCopyAttempts < 1)
                maxCopyAttempts = 1;

            return mFileCopyUtilities.CopyFileToWorkDir(
                fileName, sourceDirectoryPath, mWorkDir,
                logMsgTypeIfNotFound, createStoragePathInfoOnly: false, maxCopyAttempts: maxCopyAttempts);
        }

        /// <summary>
        /// Retrieves an Agilent ion trap .mgf file or .cdf/.mgf pair for analysis job in progress
        /// </summary>
        /// <param name="getCdfAlso">TRUE if .cdf file is needed along with .mgf file; FALSE otherwise</param>
        /// <param name="createStoragePathInfoOnly"></param>
        /// <param name="maxAttempts"></param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        private bool RetrieveMgfFile(bool getCdfAlso, bool createStoragePathInfoOnly, int maxAttempts)
        {
            var mgfFilePath = mDirectorySearch.FindMGFFile(maxAttempts, assumeUnpurged: false);

            if (string.IsNullOrEmpty(mgfFilePath))
            {
                OnErrorEvent("Source mgf file not found using FindMGFFile");
                return false;
            }

            var mgfFile = new FileInfo(mgfFilePath);
            if (!mgfFile.Exists)
            {
                OnErrorEvent("Source mgf file not found: " + mgfFile.FullName);
                return false;
            }

            // Do the copy
            if (!mFileCopyUtilities.CopyFileToWorkDirWithRename(DatasetName, mgfFile.Name, mgfFile.DirectoryName, mWorkDir,
                BaseLogger.LogLevels.ERROR, createStoragePathInfoOnly, maxCopyAttempts: 3))
            {
                return false;
            }

            // If we don't need to copy the .cdf file, we're done; otherwise, find the .cdf file and copy it
            if (!getCdfAlso)
                return true;

            if (mgfFile.Directory == null)
            {
                NotifyInvalidParentDirectory(mgfFile);
                return false;
            }

            foreach (var cdfFile in mgfFile.Directory.GetFiles("*" + AnalysisResources.DOT_CDF_EXTENSION))
            {
                // Copy the .cdf file that was found
                if (mFileCopyUtilities.CopyFileToWorkDirWithRename(DatasetName, cdfFile.Name, cdfFile.DirectoryName, mWorkDir,
                    BaseLogger.LogLevels.ERROR, createStoragePathInfoOnly, maxCopyAttempts: 3))
                {
                    return true;
                }

                OnErrorEvent("Error obtaining CDF file from " + cdfFile.FullName);
                return false;
            }

            // CDF file not found
            OnErrorEvent("CDF File not found");

            return false;
        }

        /// <summary>
        /// Looks for the newest .mzXML or .mzML file for this dataset
        /// First looks for the newest file in \\Proto-11\MSXML_Cache
        /// If not found, looks in the dataset directory, looking for subDirectories
        /// MSXML_Gen_1_154_DatasetID, MSXML_Gen_1_93_DatasetID, or MSXML_Gen_1_39_DatasetID (plus some others)
        /// </summary>
        /// <param name="createStoragePathInfoOnly"></param>
        /// <param name="sourceFilePath">Output parameter: Returns the full path to the file that was retrieved</param>
        /// <returns>True if the file was found and retrieved, otherwise False</returns>
        /// <remarks>The retrieved file might be gzipped</remarks>
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
        /// Retrieves this dataset's .mzXML or .mzML file
        /// </summary>
        /// <param name="createStoragePathInfoOnly"></param>
        /// <param name="sourceFilePath">Full path to the file that should be retrieved</param>
        /// <param name="hashCheckFilePath"></param>
        /// <returns>True if success, false if not retrieved or a hash error</returns>
        public bool RetrieveMZXmlFileUsingSourceFile(bool createStoragePathInfoOnly, string sourceFilePath, string hashCheckFilePath)
        {
            if (sourceFilePath.StartsWith(MYEMSL_PATH_FLAG))
            {
                return mMyEMSLUtilities.AddFileToDownloadQueue(sourceFilePath);
            }

            var sourceFile = new FileInfo(sourceFilePath);

            if (sourceFile.Exists)
            {
                if (sourceFile.Directory == null)
                {
                    NotifyInvalidParentDirectory(sourceFile);
                    return false;
                }

                if (mFileCopyUtilities.CopyFileToWorkDir(sourceFile.Name, sourceFile.Directory.FullName, mWorkDir,
                    BaseLogger.LogLevels.ERROR, createStoragePathInfoOnly))
                {
                    if (!string.IsNullOrEmpty(hashCheckFilePath) && File.Exists(hashCheckFilePath))
                    {
                        return RetrieveMzXMLFileVerifyHash(sourceFile, hashCheckFilePath, createStoragePathInfoOnly);
                    }

                    return true;
                }
            }

            if (mDebugLevel >= 1)
            {
                OnStatusEvent("MzXML (or MzML) file not found; will need to generate it: " + sourceFile.Name);
            }

            return false;
        }

        /// <summary>
        /// Verify the hash value of a given .mzXML or .mzML file
        /// </summary>
        /// <param name="sourceFile"></param>
        /// <param name="hashCheckFilePath"></param>
        /// <param name="createStoragePathInfoOnly"></param>
        /// <returns>True if the hash of the file matches the expected hash, otherwise false</returns>
        /// <remarks>If createStoragePathInfoOnly is true and the source file matches the target file, the hash is not recomputed</remarks>
        private bool RetrieveMzXMLFileVerifyHash(FileSystemInfo sourceFile, string hashCheckFilePath, bool createStoragePathInfoOnly)
        {
            string targetFilePath;
            bool computeHash;

            if (createStoragePathInfoOnly)
            {
                targetFilePath = sourceFile.FullName;
                // Don't compute the hash, since we're accessing the file over the network
                computeHash = false;
            }
            else
            {
                targetFilePath = Path.Combine(mWorkDir, sourceFile.Name);
                computeHash = true;
            }

            if (FileSyncUtils.ValidateFileVsHashcheck(targetFilePath, hashCheckFilePath, out var errorMessage, checkDate: true, computeHash: computeHash))
            {
                return true;
            }

            OnErrorEvent("MzXML/MzML file validation error in RetrieveMzXMLFileVerifyHash: " + errorMessage);

            try
            {
                if (createStoragePathInfoOnly)
                {
                    // Delete the local StoragePathInfo file
                    var storagePathInfoFile = Path.Combine(mWorkDir, sourceFile.Name + AnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX);
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
                    sourceFile.Delete();
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
        public bool RetrieveOutFiles(bool unConcatenate)
        {
            // Retrieve zipped OUT file
            var zippedFileName = DatasetName + "_out.zip";
            var zippedDirectoryName = FindDataFile(zippedFileName);

            if (string.IsNullOrEmpty(zippedDirectoryName))
                return false;

            // No directory found containing the zipped OUT files
            // Copy the file
            if (!mFileCopyUtilities.CopyFileToWorkDir(zippedFileName, zippedDirectoryName, mWorkDir, BaseLogger.LogLevels.ERROR))
            {
                return false;
            }

            // Unzip concatenated OUT file
            OnStatusEvent("Unzipping concatenated OUT file");
            if (UnzipFileStart(Path.Combine(mWorkDir, zippedFileName), mWorkDir, "RetrieveOutFiles"))
            {
                if (mDebugLevel >= 1)
                {
                    OnDebugEvent("Concatenated OUT file unzipped");
                }
            }

            // Deconcatenate OUT file if needed
            if (unConcatenate)
            {
                OnStatusEvent("Splitting concatenated OUT file");

                var sourceFile = new FileInfo(Path.Combine(mWorkDir, DatasetName + "_out.txt"));

                if (!sourceFile.Exists)
                {
                    OnErrorEvent("_OUT.txt file not found after unzipping");
                    return false;
                }

                if (sourceFile.Length == 0)
                {
                    OnErrorEvent("_OUT.txt file is empty (zero-bytes)");
                    return false;
                }

                var fileSplitter = new SplitCattedFiles();
                fileSplitter.SplitCattedOutsOnly(DatasetName, mWorkDir);

                if (mDebugLevel >= 1)
                {
                    OnDebugEvent("Completed splitting concatenated OUT file");
                }
            }

            return true;
        }

        /// <summary>
        /// Looks for this dataset's ScanStats files (previously created by MASIC)
        /// Looks for the files in any SIC directory that exists for the dataset
        /// </summary>
        /// <param name="createStoragePathInfoOnly">If true, creates a storage path info file but doesn't actually copy the files</param>
        /// <returns>True if the file was found and retrieved, otherwise False</returns>
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
        /// Looks for the files in any SIC directory that exists for the dataset
        /// </summary>
        /// <param name="createStoragePathInfoOnly"></param>
        /// <param name="retrieveScanStatsFile">If True, retrieves the ScanStats.txt file</param>
        /// <param name="retrieveScanStatsExFile">If True, retrieves the ScanStatsEx.txt file</param>
        /// <returns>True if the file was found and retrieved, otherwise False</returns>
        public bool RetrieveScanStatsFiles(bool createStoragePathInfoOnly, bool retrieveScanStatsFile, bool retrieveScanStatsExFile)
        {
            const bool retrieveSICStatsFile = false;
            return RetrieveScanAndSICStatsFiles(
                retrieveSICStatsFile,
                createStoragePathInfoOnly,
                retrieveScanStatsFile,
                retrieveScanStatsExFile);
        }

        /// <summary>
        /// Looks for this dataset's MASIC results files
        /// Looks for the files in any SIC directory that exists for the dataset
        /// </summary>
        /// <param name="retrieveSICStatsFile">If True, also copies the _SICStats.txt file in addition to the ScanStats files</param>
        /// <param name="createStoragePathInfoOnly">If true, creates a storage path info file but doesn't actually copy the files</param>
        /// <param name="retrieveScanStatsFile">If True, retrieves the ScanStats.txt file</param>
        /// <param name="retrieveScanStatsExFile">If True, retrieves the ScanStatsEx.txt file</param>
        /// <returns>True if the file was found and retrieved, otherwise False</returns>
        public bool RetrieveScanAndSICStatsFiles(
            bool retrieveSICStatsFile,
            bool createStoragePathInfoOnly,
            bool retrieveScanStatsFile,
            bool retrieveScanStatsExFile)
        {
            var nonCriticalFileSuffixes = new List<string>();
            const bool RETRIEVE_REPORTERIONS_FILE = false;

            return RetrieveScanAndSICStatsFiles(
                retrieveSICStatsFile,
                createStoragePathInfoOnly,
                retrieveScanStatsFile,
                retrieveScanStatsExFile,
                RETRIEVE_REPORTERIONS_FILE,
                nonCriticalFileSuffixes);
        }

        /// <summary>
        /// Looks for this dataset's MASIC results files
        /// Looks for the files in any SIC directory that exists for the dataset
        /// </summary>
        /// <param name="retrieveSICStatsFile">If True, also copies the _SICStats.txt file in addition to the ScanStats files</param>
        /// <param name="createStoragePathInfoOnly">If true, creates a storage path info file but doesn't actually copy the files</param>
        /// <param name="retrieveScanStatsFile">If True, retrieves the ScanStats.txt file</param>
        /// <param name="retrieveScanStatsExFile">If True, retrieves the ScanStatsEx.txt file</param>
        /// <param name="retrieveReporterIonsFile">If True, retrieves the ReporterIons.txt file</param>
        /// <param name="nonCriticalFileSuffixes">Filename suffixes that can be missing.  For example, "ScanStatsEx.txt"</param>
        /// <returns>True if the file was found and retrieved, otherwise False</returns>
        public bool RetrieveScanAndSICStatsFiles(
            bool retrieveSICStatsFile,
            bool createStoragePathInfoOnly,
            bool retrieveScanStatsFile,
            bool retrieveScanStatsExFile,
            bool retrieveReporterIonsFile,
            List<string> nonCriticalFileSuffixes)
        {
            long bestScanStatsFileTransactionID = 0;

            const int MAX_ATTEMPTS = 1;

            var requiredFileSuffixes = new List<string>();

            if (retrieveSICStatsFile) requiredFileSuffixes.Add(AnalysisResources.SIC_STATS_FILE_SUFFIX);
            if (retrieveScanStatsFile) requiredFileSuffixes.Add(AnalysisResources.SCAN_STATS_FILE_SUFFIX);
            if (retrieveScanStatsExFile) requiredFileSuffixes.Add(AnalysisResources.SCAN_STATS_EX_FILE_SUFFIX);
            if (retrieveReporterIonsFile) requiredFileSuffixes.Add(AnalysisResources.REPORTERIONS_FILE_SUFFIX);

            var matchCount = requiredFileSuffixes.Count(fileSuffix => FileExistsInWorkDir(DatasetName + fileSuffix));

            if (matchCount == requiredFileSuffixes.Count)
            {
                // All required MASIC files are already present in the working directory
                return true;
            }

            // Look for the MASIC Results directory
            // If the directory cannot be found, DirectorySearch.FindValidDirectory will return the directory defined by "DatasetStoragePath"
            var scanStatsFilename = DatasetName + AnalysisResources.SCAN_STATS_FILE_SUFFIX;
            var serverPath = mDirectorySearch.FindValidDirectory(DatasetName, "", "SIC*", MAX_ATTEMPTS,
                                                                 logDirectoryNotFound: false,
                                                                 retrievingInstrumentDataDir: false);

            if (string.IsNullOrEmpty(serverPath))
            {
                OnErrorEvent("Dataset directory path not found in RetrieveScanAndSICStatsFiles");
                return false;
            }

            if (serverPath.StartsWith(MYEMSL_PATH_FLAG))
            {
                // Find the newest _ScanStats.txt file in MyEMSL
                var bestSICDirName = string.Empty;

                foreach (var myEmslFile in mMyEMSLUtilities.RecentlyFoundMyEMSLFiles)
                {
                    if (myEmslFile.IsDirectory)
                    {
                        continue;
                    }

                    if (Global.IsMatch(myEmslFile.FileInfo.Filename, scanStatsFilename) && myEmslFile.FileInfo.TransactionID > bestScanStatsFileTransactionID)
                    {
                        var scanStatsFile = new FileInfo(myEmslFile.FileInfo.RelativePathWindows);

                        if (scanStatsFile.Directory == null)
                        {
                            NotifyInvalidParentDirectory(scanStatsFile);
                            return false;
                        }
                        bestSICDirName = scanStatsFile.Directory.Name;
                        bestScanStatsFileTransactionID = myEmslFile.FileInfo.TransactionID;
                    }
                }

                if (bestScanStatsFileTransactionID == 0)
                {
                    OnErrorEvent("MASIC ScanStats file not found in the SIC results directory(s) in MyEMSL");
                    return false;
                }

                var bestSICDirPath = Path.Combine(MYEMSL_PATH_FLAG, bestSICDirName);
                return RetrieveScanAndSICStatsFiles(
                    bestSICDirPath, retrieveSICStatsFile, createStoragePathInfoOnly,
                    retrieveScanStatsFile: retrieveScanStatsFile,
                    retrieveScanStatsExFile: retrieveScanStatsExFile,
                    retrieveReporterIonsFile: retrieveReporterIonsFile,
                    nonCriticalFileSuffixes: nonCriticalFileSuffixes);
            }

            var datasetDirectory = new DirectoryInfo(serverPath);

            if (!datasetDirectory.Exists)
            {
                OnErrorEvent("Dataset directory not found: " + datasetDirectory.FullName);
                return false;
            }

            // See if the ServerPath directory actually contains a subdirectory that starts with "SIC"
            var subDirectories = datasetDirectory.GetDirectories("SIC*");
            if (subDirectories.Length == 0)
            {
                OnErrorEvent("Dataset directory does not contain any MASIC results directories");
                OnWarningEvent("Dataset directory path: " + datasetDirectory.FullName);
                return false;
            }

            // MASIC Results directory Found
            // If more than one directory, use the directory with the newest _ScanStats.txt file
            var newestScanStatsFileDate = DateTime.MinValue;
            var newestScanStatsFilePath = string.Empty;

            foreach (var subDirectory in subDirectories)
            {
                var scanStatsFile = new FileInfo(Path.Combine(subDirectory.FullName, scanStatsFilename));
                if (scanStatsFile.Exists)
                {
                    if (string.IsNullOrEmpty(newestScanStatsFilePath) || scanStatsFile.LastWriteTimeUtc > newestScanStatsFileDate)
                    {
                        newestScanStatsFilePath = scanStatsFile.FullName;
                        newestScanStatsFileDate = scanStatsFile.LastWriteTimeUtc;
                    }
                }
            }

            if (string.IsNullOrEmpty(newestScanStatsFilePath))
            {
                OnErrorEvent("MASIC ScanStats file not found below the dataset directory");
                OnWarningEvent("Dataset directory path: " + datasetDirectory.FullName);
                return false;
            }

            var sourceFile = new FileInfo(newestScanStatsFilePath);

            if (sourceFile.Directory == null)
            {
                NotifyInvalidParentDirectory(sourceFile);
                return false;
            }

            var masicResultsDirPath = sourceFile.Directory.FullName;

            return RetrieveScanAndSICStatsFiles(
                masicResultsDirPath, retrieveSICStatsFile, createStoragePathInfoOnly,
                retrieveScanStatsFile: retrieveScanStatsFile,
                retrieveScanStatsExFile: retrieveScanStatsExFile,
                retrieveReporterIonsFile: retrieveReporterIonsFile,
                nonCriticalFileSuffixes: nonCriticalFileSuffixes);
        }

        /// <summary>
        /// Retrieves the MASIC results for this dataset using the specified directory
        /// </summary>
        /// <param name="masicResultsDirPath">Source directory to copy files from</param>
        /// <param name="retrieveSICStatsFile">If True, also copies the _SICStats.txt file in addition to the ScanStats files</param>
        /// <param name="createStoragePathInfoOnly">If true, creates a storage path info file but doesn't actually copy the files</param>
        /// <param name="retrieveScanStatsFile">If True, retrieves the ScanStats.txt file</param>
        /// <param name="retrieveScanStatsExFile">If True, retrieves the ScanStatsEx.txt file</param>
        /// <param name="retrieveReporterIonsFile">If True, retrieves the ReporterIons.txt file</param>
        /// <param name="nonCriticalFileSuffixes">Filename suffixes that can be missing.  For example, "ScanStatsEx.txt"</param>
        /// <returns>True if the file was found and retrieved, otherwise False</returns>
        public bool RetrieveScanAndSICStatsFiles(
            string masicResultsDirPath,
            bool retrieveSICStatsFile,
            bool createStoragePathInfoOnly,
            bool retrieveScanStatsFile,
            bool retrieveScanStatsExFile,
            bool retrieveReporterIonsFile,
            List<string> nonCriticalFileSuffixes)
        {
            const int maxCopyAttempts = 2;

            // Copy the MASIC files from the MASIC results directory

            if (string.IsNullOrEmpty(masicResultsDirPath))
            {
                OnErrorEvent("MASIC results directory path not defined in RetrieveScanAndSICStatsFiles");
                return false;
            }

            if (masicResultsDirPath.StartsWith(MYEMSL_PATH_FLAG))
            {
                var myEmslMasicResultsDirectory = new DirectoryInfo(masicResultsDirPath);

                if (retrieveScanStatsFile)
                {
                    // Look for and copy the _ScanStats.txt file
                    if (!RetrieveSICFileMyEMSL(
                        DatasetName + AnalysisResources.SCAN_STATS_FILE_SUFFIX,
                        myEmslMasicResultsDirectory.Name, nonCriticalFileSuffixes))
                    {
                        return false;
                    }
                }

                if (retrieveScanStatsExFile)
                {
                    // Look for and copy the _ScanStatsEx.txt file
                    if (!RetrieveSICFileMyEMSL
                        (DatasetName + AnalysisResources.SCAN_STATS_EX_FILE_SUFFIX,
                         myEmslMasicResultsDirectory.Name, nonCriticalFileSuffixes))
                    {
                        return false;
                    }
                }

                if (retrieveSICStatsFile)
                {
                    // Look for and copy the _SICStats.txt file
                    if (!RetrieveSICFileMyEMSL(
                        DatasetName + AnalysisResources.SIC_STATS_FILE_SUFFIX,
                        myEmslMasicResultsDirectory.Name, nonCriticalFileSuffixes))
                    {
                        return false;
                    }
                }

                if (retrieveReporterIonsFile)
                {
                    // Look for and copy the _SICStats.txt file
                    if (!RetrieveSICFileMyEMSL(
                        DatasetName + "_ReporterIons.txt",
                        myEmslMasicResultsDirectory.Name, nonCriticalFileSuffixes))
                    {
                        return false;
                    }
                }

                // All files have been found
                // The calling process should download them using ProcessMyEMSLDownloadQueue()
                return true;
            }

            var masicResultsDirectory = new DirectoryInfo(masicResultsDirPath);

            if (!masicResultsDirectory.Exists)
            {
                OnErrorEvent("MASIC results directory not found: " + masicResultsDirectory.FullName);
                return false;
            }

            if (retrieveScanStatsFile)
            {
                // Look for and copy the _ScanStats.txt file
                if (!RetrieveSICFileUNC(
                    DatasetName + AnalysisResources.SCAN_STATS_FILE_SUFFIX,
                    masicResultsDirPath,
                    createStoragePathInfoOnly, maxCopyAttempts, nonCriticalFileSuffixes))
                {
                    return false;
                }
            }

            if (retrieveScanStatsExFile)
            {
                // Look for and copy the _ScanStatsEx.txt file
                if (!RetrieveSICFileUNC(
                    DatasetName + AnalysisResources.SCAN_STATS_EX_FILE_SUFFIX,
                    masicResultsDirPath,
                    createStoragePathInfoOnly, maxCopyAttempts, nonCriticalFileSuffixes))
                {
                    return false;
                }
            }

            if (retrieveSICStatsFile)
            {
                // Look for and copy the _SICStats.txt file
                if (!RetrieveSICFileUNC(
                    DatasetName + AnalysisResources.SIC_STATS_FILE_SUFFIX,
                    masicResultsDirPath,
                    createStoragePathInfoOnly, maxCopyAttempts, nonCriticalFileSuffixes))
                {
                    return false;
                }
            }

            if (retrieveReporterIonsFile)
            {
                // Look for and copy the _SICStats.txt file
                if (!RetrieveSICFileUNC(
                    DatasetName + "_ReporterIons.txt",
                    masicResultsDirPath,
                    createStoragePathInfoOnly, maxCopyAttempts, nonCriticalFileSuffixes))
                {
                    return false;
                }
            }

            // All files successfully copied
            return true;
        }

        private bool RetrieveSICFileMyEMSL(string fileToFind, string sicDirectoryName, IReadOnlyCollection<string> nonCriticalFileSuffixes)
        {
            var matchingMyEMSLFiles = mMyEMSLUtilities.FindFiles(fileToFind, sicDirectoryName, DatasetName, recurse: false);

            if (matchingMyEMSLFiles.Count > 0)
            {
                if (mDebugLevel >= 3)
                {
                    OnDebugEvent("Found MASIC results file in MyEMSL, " + Path.Combine(sicDirectoryName, fileToFind));
                }

                mMyEMSLUtilities.AddFileToDownloadQueue(matchingMyEMSLFiles.First().FileInfo);
            }
            else
            {
                var ignoreFile = SafeToIgnore(fileToFind, nonCriticalFileSuffixes);

                if (!ignoreFile)
                {
                    OnErrorEvent(fileToFind + " not found in MyEMSL, subdirectory " + sicDirectoryName);
                    return false;
                }
            }

            return true;
        }

        private bool RetrieveSICFileUNC(
            string fileToFind,
            string masicResultsDirectoryPath,
            bool createStoragePathInfoOnly,
            int maxCopyAttempts,
            IReadOnlyCollection<string> nonCriticalFileSuffixes)
        {
            var sourceFile = new FileInfo(Path.Combine(masicResultsDirectoryPath, fileToFind));

            if (mDebugLevel >= 3)
            {
                OnDebugEvent("Copying MASIC results file: " + sourceFile.FullName);
            }

            var ignoreFile = SafeToIgnore(sourceFile.Name, nonCriticalFileSuffixes);

            BaseLogger.LogLevels logMsgTypeIfNotFound;
            if (ignoreFile)
            {
                logMsgTypeIfNotFound = BaseLogger.LogLevels.DEBUG;
            }
            else
            {
                logMsgTypeIfNotFound = BaseLogger.LogLevels.ERROR;
            }

            if (sourceFile.Directory == null)
            {
                NotifyInvalidParentDirectory(sourceFile);
                return false;
            }

            var success = mFileCopyUtilities.CopyFileToWorkDir(
                sourceFile.Name, sourceFile.Directory.FullName, mWorkDir,
                logMsgTypeIfNotFound, createStoragePathInfoOnly, maxCopyAttempts);

            if (success)
                return true;

            if (ignoreFile)
            {
                if (mDebugLevel >= 3)
                {
                    OnDebugEvent("  File not found; this is not a problem");
                }
            }
            else
            {
                OnErrorEvent(fileToFind + " not found at " + sourceFile.Directory.FullName);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Retrieves the spectra file(s) based on raw data type and puts them in the working directory
        /// </summary>
        /// <param name="rawDataTypeName">Type of data to copy</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        public bool RetrieveSpectra(string rawDataTypeName)
        {
            const bool createStoragePathInfoOnly = false;
            return RetrieveSpectra(rawDataTypeName, createStoragePathInfoOnly);
        }

        /// <summary>
        /// Retrieves the spectra file(s) based on raw data type and puts them in the working directory
        /// </summary>
        /// <param name="rawDataTypeName">Type of data to copy</param>
        /// <param name="createStoragePathInfoOnly">
        /// When true, then does not actually copy the dataset file (or directory), and instead creates a file named Dataset.raw_StoragePathInfo.txt,
        /// and this file's first line will be the full path to the spectrum file (or spectrum directory)
        /// </param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        public bool RetrieveSpectra(string rawDataTypeName, bool createStoragePathInfoOnly)
        {
            return RetrieveSpectra(rawDataTypeName, createStoragePathInfoOnly, DirectorySearch.DEFAULT_MAX_RETRY_COUNT);
        }

        /// <summary>
        /// Retrieves the spectra file(s) based on raw data type and puts them in the working directory
        /// </summary>
        /// <param name="rawDataTypeName">Type of data to copy</param>
        /// <param name="createStoragePathInfoOnly">
        /// When true, does not actually copy the dataset file (or directory), and instead creates a file named Dataset.raw_StoragePathInfo.txt
        /// The first line in the StoragePathInfo file will be the full path to the spectrum file (or spectrum directory)
        /// </param>
        /// <param name="maxAttempts">Maximum number of attempts</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        public bool RetrieveSpectra(string rawDataTypeName, bool createStoragePathInfoOnly, int maxAttempts)
        {
            var success = false;
            var storagePath = mJobParams.GetParam("DatasetStoragePath");

            OnStatusEvent("Retrieving spectra file(s)");

            var rawDataType = AnalysisResources.GetRawDataType(rawDataTypeName);
            switch (rawDataType)
            {
                case AnalysisResources.RawDataTypeConstants.AgilentDFolder:
                    // Agilent ion trap data
                    if (storagePath.IndexOf("Agilent_SL1", StringComparison.OrdinalIgnoreCase) >= 0 ||
                        storagePath.IndexOf("Agilent_XCT1", StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        // For Agilent Ion Trap datasets acquired on Agilent_SL1 or Agilent_XCT1 in 2005,
                        //  we would pre-process the data beforehand to create MGF files
                        // The following call can be used to retrieve the files
                        success = RetrieveMgfFile(getCdfAlso: true, createStoragePathInfoOnly: createStoragePathInfoOnly, maxAttempts: maxAttempts);
                    }
                    else
                    {
                        // DeconTools_V2 now supports reading the .D files directly
                        // Call RetrieveDotDFolder() to copy the directory and all subdirectories
                        success = RetrieveDotDFolder(createStoragePathInfoOnly, skipBafAndTdfFiles: true);
                    }
                    break;

                case AnalysisResources.RawDataTypeConstants.AgilentQStarWiffFile:
                    // Agilent/QSTAR TOF data
                    success = RetrieveDatasetFile(AnalysisResources.DOT_WIFF_EXTENSION, createStoragePathInfoOnly, maxAttempts);
                    break;

                case AnalysisResources.RawDataTypeConstants.ZippedSFolders:
                    // FTICR data
                    success = RetrieveSFolders(createStoragePathInfoOnly, maxAttempts);
                    break;

                case AnalysisResources.RawDataTypeConstants.ThermoRawFile:
                    // Finnigan ion trap/LTQ-FT data
                    success = RetrieveDatasetFile(AnalysisResources.DOT_RAW_EXTENSION, createStoragePathInfoOnly, maxAttempts);
                    break;

                case AnalysisResources.RawDataTypeConstants.MicromassRawFolder:
                    // Micromass QTOF data
                    success = RetrieveDotRawFolder(createStoragePathInfoOnly);
                    break;

                case AnalysisResources.RawDataTypeConstants.UIMF:
                    // IMS UIMF data
                    success = RetrieveDatasetFile(AnalysisResources.DOT_UIMF_EXTENSION, createStoragePathInfoOnly, maxAttempts);
                    break;

                case AnalysisResources.RawDataTypeConstants.mzXML:
                    success = RetrieveDatasetFile(AnalysisResources.DOT_MZXML_EXTENSION, createStoragePathInfoOnly, maxAttempts);
                    break;

                case AnalysisResources.RawDataTypeConstants.mzML:
                    success = RetrieveDatasetFile(AnalysisResources.DOT_MZML_EXTENSION, createStoragePathInfoOnly, maxAttempts);
                    break;

                case AnalysisResources.RawDataTypeConstants.BrukerFTFolder:
                case AnalysisResources.RawDataTypeConstants.BrukerTOFBaf:
                case AnalysisResources.RawDataTypeConstants.BrukerTOFTdf:
                    // Call RetrieveDotDFolder() to copy the directory and all subdirectories

                    // Both the MSXml step tool and DeconTools require the .Baf file
                    // We previously didn't need this file for DeconTools, but, now that DeconTools is using CompassXtract, we need the file
                    // In contrast, ICR-2LS only needs the ser or FID file, plus the apexAcquisition.method file in the .md folder

                    var skipBafAndTdfFiles = false;

                    var stepTool = mJobParams.GetJobParameter("StepTool", "Unknown");

                    if (string.Equals(stepTool, "ICR2LS", StringComparison.OrdinalIgnoreCase))
                    {
                        skipBafAndTdfFiles = true;
                    }

                    success = RetrieveDotDFolder(createStoragePathInfoOnly, skipBafAndTdfFiles);
                    break;

                case AnalysisResources.RawDataTypeConstants.BrukerMALDIImaging:
                    success = RetrieveBrukerMALDIImagingFolders(unzipOverNetwork: true);
                    break;

                default:
                    // rawDataType is not recognized or not supported by this function
                    if (rawDataType == AnalysisResources.RawDataTypeConstants.Unknown)
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
        /// Retrieves an Agilent or Bruker .D directory for the analysis job in progress
        /// </summary>
        /// <returns>TRUE for success; FALSE for failure</returns>
        public bool RetrieveDotDFolder(bool createStoragePathInfoOnly, bool skipBafAndTdfFiles)
        {
            var fileNamesToSkip = new List<string>();
            if (skipBafAndTdfFiles)
            {
                fileNamesToSkip.Add("analysis.baf");
                fileNamesToSkip.Add("analysis.tdf");
                fileNamesToSkip.Add("analysis.tdf_bin");
            }

            return RetrieveDotXFolder(AnalysisResources.DOT_D_EXTENSION, createStoragePathInfoOnly, fileNamesToSkip);
        }

        /// <summary>
        /// Retrieves a Micromass .raw directory for the analysis job in progress
        /// </summary>
        /// <returns>TRUE for success; FALSE for failure</returns>
        private bool RetrieveDotRawFolder(bool createStoragePathInfoOnly)
        {
            return RetrieveDotXFolder(AnalysisResources.DOT_RAW_EXTENSION, createStoragePathInfoOnly, new List<string>());
        }

        /// <summary>
        /// Retrieves a directory with a name like Dataset.D or Dataset.Raw
        /// </summary>
        /// <param name="directoryExtension">Extension on the directory name; for example, ".D"</param>
        /// <param name="createStoragePathInfoOnly"></param>
        /// <param name="fileNamesToSkip"></param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        private bool RetrieveDotXFolder(
            string directoryExtension,
            bool createStoragePathInfoOnly,
            List<string> fileNamesToSkip)
        {
            // Copies a data directory ending in directoryExtension to the working directory

            // Find the instrument data directory (e.g. Dataset.D or Dataset.Raw) in the dataset directory
            var datasetDirectoryPath = mDirectorySearch.FindDotXFolder(directoryExtension, assumeUnpurged: false);

            if (string.IsNullOrEmpty(datasetDirectoryPath))
            {
                return false;
            }

            if (datasetDirectoryPath.StartsWith(MYEMSL_PATH_FLAG))
            {
                // Queue the MyEMSL files for download
                foreach (var archiveFile in mMyEMSLUtilities.RecentlyFoundMyEMSLFiles)
                {
                    mMyEMSLUtilities.AddFileToDownloadQueue(archiveFile.FileInfo);
                }
                return true;
            }

            // Do the copy
            try
            {
                var sourceDirectory = new DirectoryInfo(datasetDirectoryPath);
                if (!sourceDirectory.Exists)
                {
                    OnErrorEvent("Source dataset directory not found: " + sourceDirectory.FullName);
                    return false;
                }

                var destinationDirPath = Path.Combine(mWorkDir, sourceDirectory.Name);

                if (createStoragePathInfoOnly)
                {
                    mFileCopyUtilities.CreateStoragePathInfoFile(sourceDirectory.FullName, destinationDirPath);
                }
                else
                {
                    // Copy the directory and all subdirectories
                    // Skip any files defined by fileNamesToSkip
                    if (mDebugLevel >= 1)
                    {
                        OnStatusEvent("Retrieving directory " + sourceDirectory.FullName);
                    }

                    mFileCopyUtilities.CopyDirectory(sourceDirectory.FullName, destinationDirPath, fileNamesToSkip);
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error copying directory " + datasetDirectoryPath, ex);
                return false;
            }

            // If we get here, all is fine
            return true;
        }

        /// <summary>
        /// Retrieves a data from a Bruker MALDI imaging dataset
        /// The data is stored as zip files with names like 0_R00X433.zip
        /// This data is unzipped into a subdirectory in the Chameleon cached data directory
        /// </summary>
        /// <param name="unzipOverNetwork"></param>
        public bool RetrieveBrukerMALDIImagingFolders(bool unzipOverNetwork)
        {
            const string ZIPPED_BRUKER_IMAGING_SECTIONS_FILE_MASK = "*R*X*.zip";

            var chameleonCachedDataDirPath= mMgrParams.GetParam("ChameleonCachedDataFolder");
            DirectoryInfo chameleonCachedDataDir;

            string unzipDirPathBase;

            var filesToDelete = new Queue<string>();

            var zipFilePathRemote = string.Empty;

            try
            {
                if (string.IsNullOrEmpty(chameleonCachedDataDirPath))
                {
                    OnErrorEvent("Chameleon cached data directory not defined; unable to unzip MALDI imaging data");
                    return false;
                }

                // Delete any subdirectories at ChameleonCachedDataFolder that do not have this dataset's name
                chameleonCachedDataDir = new DirectoryInfo(chameleonCachedDataDirPath);
                if (!chameleonCachedDataDir.Exists)
                {
                    OnErrorEvent("Chameleon cached data directory does not exist: " + chameleonCachedDataDir.FullName);
                    return false;
                }

                unzipDirPathBase = Path.Combine(chameleonCachedDataDir.FullName, DatasetName);

                foreach (var subDirectory in chameleonCachedDataDir.GetDirectories())
                {
                    if (!Global.IsMatch(subDirectory.Name, DatasetName))
                    {
                        // Delete this directory
                        try
                        {
                            if (mDebugLevel >= 2)
                            {
                                OnDebugEvent("Deleting old dataset subdirectory from chameleon cached data directory: " + subDirectory.FullName);
                            }

                            if (mMgrParams.ManagerName.IndexOf("monroe", StringComparison.OrdinalIgnoreCase) >= 0)
                            {
                                OnDebugEvent(" Skipping delete since this is a development computer");
                            }
                            else
                            {
                                subDirectory.Delete(true);
                            }
                        }
                        catch (Exception ex)
                        {
                            OnErrorEvent("Error deleting cached subdirectory " + subDirectory.FullName, ex);
                            return false;
                        }
                    }
                }

                // Delete any .mis files that do not start with this dataset's name
                foreach (var misFile in chameleonCachedDataDir.GetFiles("*.mis"))
                {
                    if (!Global.IsMatch(Path.GetFileNameWithoutExtension(misFile.Name), DatasetName))
                    {
                        misFile.Delete();
                    }
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error cleaning out old data from the Chameleon cached data directory", ex);
                return false;
            }

            // See if any imaging section filters are defined
            var applySectionFilter = GetBrukerImagingSectionFilter(mJobParams, out var startSectionX, out var endSectionX);

            // Look for the dataset directory; it must contain .Zip files with names like 0_R00X442.zip
            // If a matching directory isn't found, ServerPath will contain the directory path defined by Job Param "DatasetStoragePath"
            var serverPath = mDirectorySearch.FindValidDirectory(DatasetName, ZIPPED_BRUKER_IMAGING_SECTIONS_FILE_MASK,
                                                                 retrievingInstrumentDataDir: true);

            try
            {
                // Look for the .mis file (ImagingSequence file)
                var imagingSeqFilePathFinal = Path.Combine(chameleonCachedDataDir.FullName, DatasetName + ".mis");

                if (!File.Exists(imagingSeqFilePathFinal))
                {
                    // Copy the .mis file (ImagingSequence file) over from the storage server
                    var misFiles = Directory.GetFiles(serverPath, "*.mis");

                    if (misFiles.Length == 0)
                    {
                        // No .mis files were found; unable to continue
                        OnErrorEvent("ImagingSequence (.mis) file not found in dataset directory; unable to process MALDI imaging data");
                        return false;
                    }

                    // We'll copy the first file in MisFiles[0]
                    // Log a warning if we will be renaming the file

                    if (!Global.IsMatch(Path.GetFileName(misFiles[0]), imagingSeqFilePathFinal))
                    {
                        OnDebugEvent(string.Format("Note: Renaming .mis file (ImagingSequence file) from {0} to {1}",
                                     Path.GetFileName(misFiles[0]),
                                     Path.GetFileName(imagingSeqFilePathFinal)));
                    }

                    if (!mFileCopyUtilities.CopyFileWithRetry(misFiles[0], imagingSeqFilePathFinal, true))
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
                // Unzip each of the *R*X*.zip files to the Chameleon cached data directory

                // However, consider limits defined by job params BrukerMALDI_Imaging_startSectionX and BrukerMALDI_Imaging_endSectionX
                // when processing the files

                var zipFiles = Directory.GetFiles(serverPath, ZIPPED_BRUKER_IMAGING_SECTIONS_FILE_MASK);

                var rxyMatcher = new Regex(@"R(?<R>\d+)X(?<X>\d+)Y(?<Y>\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
                var rxMatcher = new Regex(@"R(?<R>\d+)X(?<X>\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                foreach (var zipFilePath in zipFiles)
                {
                    zipFilePathRemote = zipFilePath;

                    bool unzipFile;
                    if (applySectionFilter)
                    {
                        unzipFile = false;

                        // Determine the R, X, and Y coordinates for this .Zip file

                        if (GetBrukerImagingFileCoords(zipFilePathRemote, rxyMatcher, rxMatcher, out _, out var xCoordinate, out _))
                        {
                            // Compare to startSectionX and endSectionX
                            if (xCoordinate >= startSectionX && xCoordinate <= endSectionX)
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
                    // If they already exist in the cached data directory, there is no need to continue

                    // Set this to false for now
                    unzipFile = false;

                    var remoteZipFile = new Ionic.Zip.ZipFile(zipFilePathRemote);

                    foreach (var entry in remoteZipFile.Entries)
                    {
                        if (!entry.IsDirectory)
                        {
                            var pathToCheck = Path.Combine(unzipDirPathBase, entry.FileName.Replace('/', '\\'));

                            if (!File.Exists(pathToCheck))
                            {
                                unzipFile = true;
                                break;
                            }
                        }
                    }

                    if (!unzipFile)
                        continue;

                    // Unzip the file to the Chameleon cached data directory
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

                            zipFilePathToExtract = Path.Combine(mWorkDir, sourceFileName);

                            if (mDebugLevel >= 2)
                            {
                                OnDebugEvent("Copying " + zipFilePathRemote);
                            }

                            if (!mFileCopyUtilities.CopyFileWithRetry(zipFilePathRemote, zipFilePathToExtract, true))
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

                    // Now use DotNetZip (aka Ionic.Zip) to unzip zipFilePathLocal to the data cache directory
                    // Do not overwrite existing files (assume they're already valid)

                    try
                    {
                        using var zipFile = new Ionic.Zip.ZipFile(zipFilePathToExtract);

                        if (mDebugLevel >= 2)
                        {
                            OnDebugEvent("Unzipping " + zipFilePathToExtract);
                        }

                        zipFile.ExtractAll(unzipDirPathBase, Ionic.Zip.ExtractExistingFileAction.DoNotOverwrite);
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
                    var startTime = DateTime.UtcNow;

                    while (filesToDelete.Count > 0)
                    {
                        // Try to process the files remaining in queue filesToDelete

                        DeleteQueuedFiles(filesToDelete, string.Empty);

                        if (filesToDelete.Count > 0)
                        {
                            if (DateTime.UtcNow.Subtract(startTime).TotalSeconds > 20)
                            {
                                // Stop trying to delete files; it's not worth continuing to try
                                OnWarningEvent("Unable to delete all of the files in queue filesToDelete; " +
                                    "Queue Length = " + filesToDelete.Count + "; " +
                                    "this warning can be safely ignored (function RetrieveBrukerMALDIImagingFolders)");
                                break;
                            }

                            Global.IdleLoop(0.5);
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
        /// Unzips dataset directories to the working directory
        /// </summary>
        /// <returns>TRUE for success; FALSE for failure</returns>
        private bool RetrieveSFolders(bool createStoragePathInfoOnly, int maxAttempts)
        {
            try
            {
                // First Check for the existence of a 0.ser directory
                // If 0.ser directory exists, either store the path to the 0.ser directory in a StoragePathInfo file, or copy the 0.ser directory to the working directory
                var datasetDirectoryPath = mDirectorySearch.FindValidDirectory(
                    DatasetName,
                    fileNameToFind: "",
                    directoryNameToFind: AnalysisResources.BRUKER_ZERO_SER_FOLDER,
                    maxRetryCount: maxAttempts,
                    logDirectoryNotFound: true,
                    retrievingInstrumentDataDir: true);

                if (!string.IsNullOrEmpty(datasetDirectoryPath))
                {
                    var sourceDirectory = new DirectoryInfo(Path.Combine(datasetDirectoryPath, AnalysisResources.BRUKER_ZERO_SER_FOLDER));

                    if (sourceDirectory.Exists)
                    {
                        if (createStoragePathInfoOnly)
                        {
                            return mFileCopyUtilities.CreateStoragePathInfoFile(sourceDirectory.FullName, mWorkDir + @"\");
                        }

                        // Copy the 0.ser directory to the Work directory
                        // First create the 0.ser subdirectory
                        var targetDirectory = Directory.CreateDirectory(Path.Combine(mWorkDir, AnalysisResources.BRUKER_ZERO_SER_FOLDER));

                        // Now copy the files from the source 0.ser directory to the target directory
                        // Typically there will only be two files: ACQUS and ser
                        foreach (var fileToCopy in sourceDirectory.GetFiles())
                        {
                            if (!mFileCopyUtilities.CopyFileToWorkDir(fileToCopy.Name, sourceDirectory.FullName, targetDirectory.FullName))
                            {
                                // Error has already been logged
                                return false;
                            }
                        }

                        return true;
                    }
                }

                // If the 0.ser directory does not exist, unzip the zipped s-folders
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
                var zipFiles = Directory.GetFiles(mWorkDir, "s*.zip");
                if (zipFiles.GetLength(0) < 1)
                {
                    OnErrorEvent("No zipped s-folders found in working directory");
                    return false;
                }

                // Create a dataset subdirectory under the working directory
                var datasetWorkDir = Path.Combine(mWorkDir, DatasetName);
                Directory.CreateDirectory(datasetWorkDir);

                // Set up the unzip tool
                var dotNetZipTools = new DotNetZipTools(mDebugLevel, datasetWorkDir);
                RegisterEvents(dotNetZipTools);

                // Unzip each of the zip files to the working directory
                foreach (var zipFilePath in zipFiles)
                {
                    if (mDebugLevel > 3)
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

                        var targetDirPath = Path.Combine(datasetWorkDir, fileNameBase);
                        Directory.CreateDirectory(targetDirPath);

                        var sourceFilePath = Path.Combine(mWorkDir, sourceFileName);

                        if (!dotNetZipTools.UnzipFile(sourceFilePath, targetDirPath))
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

                ProgRunner.GarbageCollectNow();

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

                        File.Delete(Path.Combine(mWorkDir, targetFileName));
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
        /// Returns True if the filename ends with any of the suffixes in nonCriticalFileSuffixes
        /// </summary>
        /// <param name="fileName"></param>
        /// <param name="nonCriticalFileSuffixes"></param>
        private bool SafeToIgnore(string fileName, IReadOnlyCollection<string> nonCriticalFileSuffixes)
        {
            if (nonCriticalFileSuffixes != null)
            {
                foreach (var suffix in nonCriticalFileSuffixes)
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
        /// Unzips all files in the specified zip file
        /// </summary>
        /// <param name="zipFilePath">File to unzip</param>
        /// <param name="outputDirectoryPath">Target directory for the extracted files</param>
        /// <param name="callingFunctionName">Calling function name (used for debugging purposes)</param>
        /// <returns>True if success, otherwise false</returns>
        public bool UnzipFileStart(string zipFilePath, string outputDirectoryPath, string callingFunctionName)
        {
            var unzipToolName = "??";

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

                var zipFile = new FileInfo(zipFilePath);

                if (!zipFile.Exists)
                {
                    // File not found
                    OnErrorEvent("Error unzipping '" + zipFilePath + "': File not found");
                    OnStatusEvent("CallingFunction: " + callingFunctionName);
                    return false;
                }

                if (zipFilePath.EndsWith(AnalysisResources.DOT_GZ_EXTENSION, StringComparison.OrdinalIgnoreCase))
                {
                    // This is a gzipped file
                    // Use DotNetZip
                    unzipToolName = DotNetZipTools.DOTNET_ZIP_NAME;
                    mDotNetZipTools.DebugLevel = mDebugLevel;
                    return mDotNetZipTools.GUnzipFile(zipFilePath, outputDirectoryPath);
                }

                // Use DotNetZip
                unzipToolName = DotNetZipTools.DOTNET_ZIP_NAME;
                mDotNetZipTools.DebugLevel = mDebugLevel;
                var success = mDotNetZipTools.UnzipFile(zipFilePath, outputDirectoryPath);

                return success;
            }
            catch (Exception ex)
            {
                var errMsg = "Exception while unzipping '" + zipFilePath + "'";
                if (!string.IsNullOrEmpty(unzipToolName))
                    errMsg += " using " + unzipToolName;

                OnErrorEvent(errMsg, ex);
                OnStatusEvent("CallingFunction: " + callingFunctionName);

                return false;
            }
        }

        #endregion

    }
}
