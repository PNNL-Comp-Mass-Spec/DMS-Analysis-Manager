using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Threading;
using System.Xml;
using PHRPReader;
using PRISM;

namespace AnalysisManagerBase
{
    public class clsDataPackageFileHandler : clsEventNotifier
    {

        #region "Constants"

        public const string JOB_INFO_FILE_PREFIX = "JobInfoFile_Job";

        private const string SP_NAME_GET_JOB_STEP_INPUT_FOLDER = "GetJobStepInputFolder";
        #endregion

        #region "Structures"
        public struct udtDataPackageRetrievalOptionsType
        {
            /// <summary>
            /// Set to true to create a text file for each job listing the full path to the files that would be retrieved for that job
            /// Example filename: FilePathInfo_Job950000.txt
            /// </summary>
            /// <remarks>No files are actually retrieved when this is set to True</remarks>
            public bool CreateJobPathFiles;

            /// <summary>
            /// Set to true to obtain the mzXML file for the dataset associated with this job
            /// </summary>
            /// <remarks>If the .mzXML file does not exist, then retrieves the instrument data file (e.g. Thermo .raw file)</remarks>
            public bool RetrieveMzXMLFile;

            /// <summary>
            /// Set to True to retrieve _DTA.txt files (the PRIDE Converter will convert these to .mgf files)
            /// </summary>
            /// <remarks>If the search used a .mzML instead of a _dta.txt file, the .mzML.gz file will be retrieved</remarks>
            public bool RetrieveDTAFiles;

            /// <summary>
            /// Set to True to obtain MSGF+ .mzID files
            /// </summary>
            /// <remarks></remarks>
            public bool RetrieveMZidFiles;

            /// <summary>
            /// Set to True to obtain .pepXML files (typically stored as _pepXML.zip)
            /// </summary>
            /// <remarks></remarks>
            public bool RetrievePepXMLFiles;
            /// <summary>
            /// Set to True to obtain the _syn.txt file and related PHRP files
            /// </summary>
            /// <remarks></remarks>
            public bool RetrievePHRPFiles;
            /// <summary>
            /// When True, assume that the instrument file (e.g. .raw file) exists in the dataset storage folder
            /// and do not search in MyEMSL or in the archive for the file
            /// </summary>
            /// <remarks>Even if the instrument file has been purged from the storage folder, still report "success" when searching for the instrument file</remarks>
            public bool AssumeInstrumentDataUnpurged;
        }
        #endregion


        #region "Module variables"

        private readonly clsAnalysisResources mAnalysisResources;

        /// <summary>
        /// Typically Gigasax.DMS_Pipeline
        /// </summary>
        private readonly string mBrokerDBConnectionString;

        private readonly clsDataPackageInfoLoader mDataPackageInfoLoader;

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="brokerDbConnectionString">Gigasax.DMS_Pipeline</param>
        /// <param name="dataPackageID">Data package ID</param>
        /// <param name="resourcesClass">Resource class</param>
        public clsDataPackageFileHandler(string brokerDbConnectionString, int dataPackageID, clsAnalysisResources resourcesClass)
        {
            mAnalysisResources = resourcesClass;

            mDataPackageInfoLoader = new clsDataPackageInfoLoader(brokerDbConnectionString, dataPackageID);

            mBrokerDBConnectionString = brokerDbConnectionString;
        }

        /// <summary>
        /// Contact the database to determine the exact folder name of the MSXML_Gen or Mz_Refinery folder
        /// that has the CacheInfo file for the mzML file used by a given job
        /// Look for the cache info file in that folder, then use that to find the location of the actual .mzML.gz file
        /// </summary>
        /// <param name="dataset"></param>
        /// <param name="job"></param>
        /// <param name="stepToolFilter">step tool to filter on; if an empty string, returns the input folder for the primary step tool for the job</param>
        /// <param name="workingDir"></param>
        /// <returns>Path to the .mzML or .mzML.gz file; empty string if not found</returns>
        /// <remarks>Uses the highest job step to determine the input folder, meaning the .mzML.gz file returned will be the one used by MSGF+</remarks>
        private string FindMzMLForJob(string dataset, int job, string stepToolFilter, string workingDir)
        {

            try
            {
                // Set up the command object prior to SP execution
                var cmd = new SqlCommand(SP_NAME_GET_JOB_STEP_INPUT_FOLDER) { CommandType = CommandType.StoredProcedure };

                cmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;
                cmd.Parameters.Add(new SqlParameter("@job", SqlDbType.Int)).Value = job;

                var stepToolFilterParam = cmd.Parameters.Add(new SqlParameter("@stepToolFilter", SqlDbType.VarChar, 8000));
                stepToolFilterParam.Value = stepToolFilter;

                cmd.Parameters.Add(new SqlParameter("@inputFolderName", SqlDbType.VarChar, 128)).Direction = ParameterDirection.Output;
                cmd.Parameters.Add(new SqlParameter("@stepToolMatch", SqlDbType.VarChar, 64)).Direction = ParameterDirection.Output;

                var matchFound = false;
                var inputFolderName = string.Empty;
                var stepToolMatch = string.Empty;

                var pipelineDBProcedureExecutor = new clsExecuteDatabaseSP(mBrokerDBConnectionString);
                pipelineDBProcedureExecutor.DebugEvent += OnDebugEvent;
                pipelineDBProcedureExecutor.ErrorEvent += ProcedureExecutor_DBErrorEvent;
                pipelineDBProcedureExecutor.WarningEvent += OnWarningEvent;

                while (!matchFound)
                {
                    // Execute the SP
                    pipelineDBProcedureExecutor.ExecuteSP(cmd, 1);

                    inputFolderName = Convert.ToString(cmd.Parameters["@inputFolderName"].Value);
                    stepToolMatch = Convert.ToString(cmd.Parameters["@stepToolMatch"].Value);

                    if (string.IsNullOrWhiteSpace(inputFolderName))
                    {
                        if (string.IsNullOrEmpty(stepToolFilter))
                        {
                            OnErrorEvent(string.Format("Unable to determine the input folder for job {0}", job));
                            return string.Empty;
                        }

                        OnStatusEvent(string.Format("Unable to determine the input folder for job {0} and step tool {1}; will try again without a step tool filter",
                            job, stepToolFilter));
                        stepToolFilter = string.Empty;
                        stepToolFilterParam.Value = stepToolFilter;
                    }
                    else
                    {
                        matchFound = true;
                    }
                }

                OnStatusEvent(string.Format("Determined the input folder for job {0} is {1}, step tool {2}", job, inputFolderName, stepToolMatch));

                // Look for a CacheInfo.txt file in the matched input folder
                // Note that FindValidFolder will search both the dataset folder and in folder inputFolderName below the dataset folder


                var datasetFolderPath = mAnalysisResources.FindValidFolder(
                    dataset, "*_CacheInfo.txt", folderNameToFind: inputFolderName, maxAttempts: 1,
                    logFolderNotFound: false, retrievingInstrumentDataFolder: false, assumeUnpurged: false,
                    validFolderFound: out var validFolderFound, folderNotFoundMessage: out var folderNotFoundMessage);

                if (!validFolderFound)
                {
                    return string.Empty;
                }

                var cacheInfoFileName = string.Empty;
                string cacheInfoFileSourceType;

                // datasetFolderPath will hold the full path to the actual folder with the CacheInfo.txt file
                // For example: \\proto-6\QExactP02\2016_2\Biodiversity_A_cryptum_FeTSB\Mz_Refinery_1_195_501572

                if (datasetFolderPath.StartsWith(clsAnalysisResources.MYEMSL_PATH_FLAG))
                {
                    // File found in MyEMSL
                    // Determine the MyEMSL FileID by searching for the expected file in m_MyEMSLUtilities.RecentlyFoundMyEMSLFiles

                    foreach (var udtArchivedFile in mAnalysisResources.MyEMSLUtilities.RecentlyFoundMyEMSLFiles)
                    {
                        var fiArchivedFile = new FileInfo(udtArchivedFile.FileInfo.RelativePathWindows);
                        if (fiArchivedFile.Name.EndsWith("_CacheInfo.txt", StringComparison.InvariantCultureIgnoreCase))
                        {
                            mAnalysisResources.MyEMSLUtilities.AddFileToDownloadQueue(udtArchivedFile.FileInfo);
                            cacheInfoFileName = udtArchivedFile.FileInfo.Filename;
                            break;
                        }
                    }

                    if (string.IsNullOrEmpty(cacheInfoFileName))
                    {
                        OnErrorEvent("FindValidFolder reported a match to a file in MyEMSL (" + datasetFolderPath + ") " + "but MyEMSLUtilities.RecentlyFoundMyEMSLFiles is empty");
                        return string.Empty;
                    }

                    mAnalysisResources.ProcessMyEMSLDownloadQueue();
                    cacheInfoFileSourceType = "MyEMSL";
                }
                else
                {
                    var sourceFolder = new DirectoryInfo(datasetFolderPath);
                    cacheInfoFileSourceType = sourceFolder.FullName;

                    if (!sourceFolder.Exists)
                    {
                        OnErrorEvent("FindValidFolder reported a match to folder " + cacheInfoFileSourceType + " but the folder was not found");
                        return string.Empty;
                    }

                    var cacheInfoFiles = sourceFolder.GetFiles("*_CacheInfo.txt");
                    if (cacheInfoFiles.Length == 0)
                    {
                        var sourceFolderAlt = new DirectoryInfo(Path.Combine(sourceFolder.FullName, inputFolderName));
                        if (sourceFolderAlt.Exists)
                        {
                            cacheInfoFiles = sourceFolderAlt.GetFiles("*_CacheInfo.txt");
                        }

                        if (cacheInfoFiles.Length > 0)
                        {
                            cacheInfoFileSourceType = sourceFolderAlt.FullName;
                        }
                        else
                        {
                            OnErrorEvent("FindValidFolder reported that folder " + cacheInfoFileSourceType +
                                         " has a _CacheInfo.txt file, but none was found");
                            return string.Empty;
                        }
                    }

                    var sourceCacheInfoFile = cacheInfoFiles.First();

                    var success = mAnalysisResources.CopyFileToWorkDir(
                        sourceCacheInfoFile.Name, sourceCacheInfoFile.DirectoryName,
                        workingDir, clsLogTools.LogLevels.ERROR);

                    if (!success)
                    {
                        // The error should have already been logged
                        return string.Empty;
                    }

                    cacheInfoFileName = sourceCacheInfoFile.Name;
                }

                // Open the CacheInfo file and read the first line

                var localCacheInfoFile = new FileInfo(Path.Combine(workingDir, cacheInfoFileName));
                if (!localCacheInfoFile.Exists)
                {
                    OnErrorEvent("CacheInfo file not found in the working directory; should have been retrieved from " + cacheInfoFileSourceType);
                    return string.Empty;
                }

                var remoteMsXmlFilePath = string.Empty;

                using (var reader = new StreamReader(new FileStream(localCacheInfoFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    if (!reader.EndOfStream)
                    {
                        remoteMsXmlFilePath = reader.ReadLine();
                    }
                }

                if (string.IsNullOrEmpty(remoteMsXmlFilePath))
                {
                    OnErrorEvent("CacheInfo file retrieved from " + cacheInfoFileSourceType + " was empty");
                }
                else
                {
                    OnStatusEvent(string.Format("Found remote mzML file for job {0}: {1}", job, remoteMsXmlFilePath));
                }

                Thread.Sleep(250);

                // Delete the locally cached file
                localCacheInfoFile.Delete();

                return remoteMsXmlFilePath;

            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception finding the .mzML file used by job " + job, ex);
                return string.Empty;
            }

        }

        /// <summary>
        /// Add expected .mzid file names
        /// Originally .mzid files were named _msgfplus.zip
        /// We switched to naming them _msgfplus.mzid.gz in January 2014
        /// </summary>
        /// <param name="datasetName"></param>
        /// <param name="splitFastaResultID"></param>
        /// <param name="zipFileCandidates">Potential names of zip files to decompress</param>
        /// <param name="gzipFileCandidates">Potential names of gzip files to decompress</param>
        private void GetMzIdFilesToFind(string datasetName, int splitFastaResultID, ICollection<string> zipFileCandidates, ICollection<string> gzipFileCandidates)
        {
            string zipFile;
            string gZipFile;

            if (splitFastaResultID > 0)
            {
                zipFile = datasetName + "_msgfplus_Part" + splitFastaResultID + ".zip";
                gZipFile = datasetName + "_msgfplus_Part" + splitFastaResultID + ".mzid.gz";
            }
            else
            {
                zipFile = datasetName + "_msgfplus.zip";
                gZipFile = datasetName + "_msgfplus.mzid.gz";
            }

            zipFileCandidates.Add(zipFile);
            gzipFileCandidates.Add(gZipFile);

        }

        private string GetJobInfoFilePath(int job)
        {
            return GetJobInfoFilePath(job, mAnalysisResources.WorkDir);
        }

        public static string GetJobInfoFilePath(int job, string workDirPath)
        {
            return Path.Combine(workDirPath, JOB_INFO_FILE_PREFIX + job + ".txt");
        }

        /// <summary>
        /// Return the full path to the most recently unzipped file (.zip or .gz)
        /// Returns an empty string if no recent unzipped files
        /// </summary>
        /// <param name="ionicZipTools"></param>
        /// <returns></returns>
        private string MostRecentUnzippedFile(clsIonicZipTools ionicZipTools)
        {
            if (ionicZipTools.MostRecentUnzippedFiles.Count > 0)
            {
                return ionicZipTools.MostRecentUnzippedFiles.First().Value;
            }

            return string.Empty;
        }

        /// <summary>
        /// Open an .mzid or .mzid.gz file and look for the SpectraData element
        /// If the location of the file specified by SpectraData points to a .mzML or .mzML.gz file, return true
        /// Otherwise, return false
        /// </summary>
        /// <param name="mzIdFileToInspect"></param>
        /// <param name="ionicZipTools"></param>
        /// <returns></returns>
        private bool MSGFPlusSearchUsedMzML(string mzIdFileToInspect, clsIonicZipTools ionicZipTools)
        {

            try
            {
                var mzidFile = new FileInfo(mzIdFileToInspect);
                if (!mzidFile.Exists)
                {
                    OnErrorEvent("Unable to examine the mzid file to determine whether MSGF+ searched a .mzML file; file Not found:   " + mzIdFileToInspect);
                    return false;
                }

                string mzidFilePathLocal;
                bool deleteLocalFile;
                var searchedUsedMzML = false;

                if (mzidFile.Name.EndsWith(".zip", StringComparison.InvariantCultureIgnoreCase))
                {
                    if (!ionicZipTools.UnzipFile(mzidFile.FullName))
                    {
                        OnErrorEvent("Error unzipping " + mzidFile.FullName);
                        return false;
                    }

                    mzidFilePathLocal = MostRecentUnzippedFile(ionicZipTools);
                    deleteLocalFile = true;

                }
                else if (mzidFile.Name.EndsWith(".gz", StringComparison.InvariantCultureIgnoreCase))
                {
                    if (!ionicZipTools.GUnzipFile(mzidFile.FullName))
                    {
                        OnErrorEvent("Error unzipping " + mzidFile.FullName);
                        return false;
                    }

                    mzidFilePathLocal = MostRecentUnzippedFile(ionicZipTools);
                    deleteLocalFile = true;
                }
                else
                {
                    mzidFilePathLocal = mzidFile.FullName;
                    deleteLocalFile = false;
                }

                if (!File.Exists(mzidFilePathLocal))
                {
                    OnErrorEvent("mzID file not found in the working directory; cannot inspect it in MSGFPlusSearchUsedMzML: " + Path.GetFileName(mzidFilePathLocal));
                    return false;
                }

                var spectraLocationFound = false;

                using (var reader = new XmlTextReader(mzidFilePathLocal))
                {

                    while (reader.Read())
                    {
                        switch (reader.NodeType)
                        {
                            case XmlNodeType.Element:
                                if (reader.Name == "SpectraData")
                                {
                                    // The location attribute of the SpectraData element has the input file name
                                    if (reader.MoveToAttribute("location"))
                                    {
                                        var spectraDataFileName = Path.GetFileName(reader.Value);
                                        const string DOT_MZML = clsAnalysisResources.DOT_MZML_EXTENSION;
                                        const string DOT_MZML_GZ = clsAnalysisResources.DOT_MZML_EXTENSION + clsAnalysisResources.DOT_GZ_EXTENSION;

                                        if (spectraDataFileName.EndsWith(DOT_MZML, StringComparison.InvariantCultureIgnoreCase) ||
                                            spectraDataFileName.EndsWith(DOT_MZML_GZ, StringComparison.InvariantCultureIgnoreCase))
                                        {
                                            searchedUsedMzML = true;
                                        }

                                        spectraLocationFound = true;
                                    }
                                }
                                break;
                        }

                        if (spectraLocationFound)
                            break;
                    }
                }

                if (deleteLocalFile)
                {
                    Thread.Sleep(200);
                    File.Delete(mzidFilePathLocal);
                }

                if (!spectraLocationFound)
                {
                    OnErrorEvent(".mzID file did not have node SpectraData with attribute location: " + Path.GetFileName(mzidFilePathLocal));
                    return false;
                }

                return searchedUsedMzML;

            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception examining the mzid file To determine whether MSGF+ searched a .mzML file", ex);
                return false;
            }

        }

        private bool RenameDuplicatePHRPFile(
            string sourceFolderPath,
            string sourceFilename,
            string targetFolderPath,
            string prefixToAdd,
            int job,
            out string newFilePath)
        {

            try
            {
                var fiFileToRename = new FileInfo(Path.Combine(sourceFolderPath, sourceFilename));
                newFilePath = Path.Combine(targetFolderPath, prefixToAdd + fiFileToRename.Name);

                Thread.Sleep(100);
                fiFileToRename.MoveTo(newFilePath);

                mAnalysisResources.AddResultFileToSkip(Path.GetFileName(newFilePath));
                return true;

            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception renaming PHRP file " + sourceFilename + " For job " + job + " (data package has multiple jobs For the same dataset)", ex);
                newFilePath = string.Empty;
                return false;
            }

        }

        /// <summary>
        /// Retrieves the PHRP files for the PeptideHit jobs defined for the data package associated with this aggregation job
        /// Also creates a batch file that can be manually run to retrieve the instrument data files
        /// </summary>
        /// <param name="udtOptions">File retrieval options</param>
        /// <param name="lstDataPackagePeptideHitJobs">Output parameter: Job info for the peptide_hit jobs associated with this data package (output parameter)</param>
        /// <param name="progressPercentAtStart">Percent complete value to use for computing incremental progress</param>
        /// <param name="progressPercentAtFinish">Percent complete value to use for computing incremental progress</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        public bool RetrieveDataPackagePeptideHitJobPHRPFiles(
            udtDataPackageRetrievalOptionsType udtOptions,
            out List<clsDataPackageJobInfo> lstDataPackagePeptideHitJobs,
            float progressPercentAtStart,
            float progressPercentAtFinish)
        {

            var sourceFolderPath = "??";
            var sourceFilename = "??";

            // Keys in this dictionary are DatasetID, values are a command of the form "Copy \\Server\Share\Folder\Dataset.raw Dataset.raw"
            // Note that we're explicitly defining the target filename to make sure the case of the letters matches the dataset name's case
            var dctRawFileRetrievalCommands = new Dictionary<int, string>();

            // Keys in this dictionary are dataset name, values are the full path to the instrument data file for the dataset
            // This information is stored in packed job parameter JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS
            var dctDatasetRawFilePaths = new Dictionary<string, string>();

            // This list tracks the info for the jobs associated with this aggregation job's data package
            lstDataPackagePeptideHitJobs = new List<clsDataPackageJobInfo>();

            // This dictionary tracks the datasets associated with this aggregation job's data package
            Dictionary<int, clsDataPackageDatasetInfo> dctDataPackageDatasets;

            var debugLevel = mAnalysisResources.DebugLevel;
            var workingDir = mAnalysisResources.WorkDir;

            try
            {
                var success = mDataPackageInfoLoader.LoadDataPackageDatasetInfo(out dctDataPackageDatasets);

                if (!success || dctDataPackageDatasets.Count == 0)
                {
                    OnErrorEvent("Did not find any datasets associated with this job's data package ID (" + mDataPackageInfoLoader.DataPackageID + ")");
                    return false;
                }

            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception calling LoadDataPackageDatasetInfo", ex);
                return false;
            }

            // The keys in this dictionary are data package job info entries
            // The values are KeyValuePairs of path to the .mzXML file and path to the .hashcheck file (if any)
            // The KeyValuePair will have empty strings if the .Raw file needs to be retrieved
            // This information is used by RetrieveDataPackageMzXMLFiles when copying files locally
            var dctInstrumentDataToRetrieve = new Dictionary<clsDataPackageJobInfo, KeyValuePair<string, string>>();

            // This list tracks analysis jobs that are not PeptideHit jobs
            List<clsDataPackageJobInfo> lstAdditionalJobs;

            try
            {
                lstDataPackagePeptideHitJobs = mDataPackageInfoLoader.RetrieveDataPackagePeptideHitJobInfo(out lstAdditionalJobs);

                if (lstDataPackagePeptideHitJobs.Count == 0)
                {
                    // Did not find any peptide hit jobs associated with this job's data package ID
                    // This is atypical, but is allowed
                }

            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception calling RetrieveDataPackagePeptideHitJobInfo", ex);
                return false;
            }


            try
            {
                var ionicZipTools = new clsIonicZipTools(debugLevel, workingDir);
                RegisterEvents(ionicZipTools);

                // Make sure the MyEMSL download queue is empty
                mAnalysisResources.ProcessMyEMSLDownloadQueue();

                // Cache the current dataset and job info
                mAnalysisResources.CacheCurrentDataAndJobInfo();

                var jobsProcessed = 0;


                foreach (var dataPkgJob in lstDataPackagePeptideHitJobs)
                {
                    if (!mAnalysisResources.OverrideCurrentDatasetAndJobInfo(dataPkgJob))
                    {
                        // Error message has already been logged
                        mAnalysisResources.RestoreCachedDataAndJobInfo();
                        return false;
                    }

                    var datasetName = mAnalysisResources.DatasetName;

                    if (dataPkgJob.PeptideHitResultType == clsPHRPReader.ePeptideHitResultType.Unknown)
                    {
                        var msg = "PeptideHit ResultType not recognized for job " + dataPkgJob.Job + ": " + dataPkgJob.ResultType;
                        clsGlobal.LogWarning(msg);
                    }
                    else
                    {
                        // Keys in this list are filenames; values are True if the file is required and False if not required
                        var lstFilesToGet = new SortedList<string, bool>();
                        string localFolderPath;
                        var lstPendingFileRenames = new List<string>();
                        clsLogTools.LogLevels eLogMsgTypeIfNotFound;

                        // These two variables track filenames that should be decompressed if they were copied locally
                        var zipFileCandidates = new List<string>();
                        var gzipFileCandidates = new List<string>();

                        // This tracks the _pepXML.zip filename, which will be unzipped if it was found
                        var zippedPepXmlFile = string.Empty;

                        bool prefixRequired;

                        var synopsisFileName = clsPHRPReader.GetPHRPSynopsisFileName(dataPkgJob.PeptideHitResultType, dataPkgJob.Dataset);
                        var synopsisMSGFFileName = clsPHRPReader.GetMSGFFileName(synopsisFileName);

                        if (udtOptions.RetrievePHRPFiles)
                        {
                            lstFilesToGet.Add(synopsisFileName, true);

                            lstFilesToGet.Add(clsPHRPReader.GetPHRPResultToSeqMapFileName(dataPkgJob.PeptideHitResultType, dataPkgJob.Dataset), true);
                            lstFilesToGet.Add(clsPHRPReader.GetPHRPSeqInfoFileName(dataPkgJob.PeptideHitResultType, dataPkgJob.Dataset), true);
                            lstFilesToGet.Add(clsPHRPReader.GetPHRPSeqToProteinMapFileName(dataPkgJob.PeptideHitResultType, dataPkgJob.Dataset), true);
                            lstFilesToGet.Add(clsPHRPReader.GetPHRPModSummaryFileName(dataPkgJob.PeptideHitResultType, dataPkgJob.Dataset), true);

                            lstFilesToGet.Add(synopsisMSGFFileName, false);
                        }

                        // Names of mzid result files that we should look for
                        // Examples include:
                        //  DatasetName_msgfplus.mzid.gz
                        //  DatasetName_msgfplus_Part5.mzid.gz
                        //  DatasetName_msgfplus.zip
                        var candidateMzIdFiles = new SortedSet<string>(StringComparer.InvariantCultureIgnoreCase);

                        if (udtOptions.RetrieveMZidFiles && dataPkgJob.PeptideHitResultType == clsPHRPReader.ePeptideHitResultType.MSGFDB)
                        {
                            // Retrieve MSGF+ .mzID files
                            // They will either be stored as .zip files or as .gz files

                            if (dataPkgJob.NumberOfClonedSteps > 0)
                            {
                                for (var splitFastaResultID = 1; splitFastaResultID <= dataPkgJob.NumberOfClonedSteps; splitFastaResultID++)
                                {
                                    GetMzIdFilesToFind(datasetName, splitFastaResultID, zipFileCandidates, gzipFileCandidates);
                                }
                            }
                            else
                            {
                                GetMzIdFilesToFind(datasetName, 0, zipFileCandidates, gzipFileCandidates);
                            }

                            foreach (var candidateFile in zipFileCandidates.Union(gzipFileCandidates))
                            {
                                candidateMzIdFiles.Add(candidateFile);
                                lstFilesToGet.Add(candidateFile, false);
                            }

                        }

                        if (udtOptions.RetrievePepXMLFiles && dataPkgJob.PeptideHitResultType != clsPHRPReader.ePeptideHitResultType.Unknown)
                        {
                            // Retrieve .pepXML files, which are stored as _pepXML.zip files
                            zippedPepXmlFile = datasetName + "_pepXML.zip";
                            lstFilesToGet.Add(zippedPepXmlFile, false);
                        }

                        sourceFolderPath = string.Empty;

                        // Check whether a synopsis file by this name has already been copied locally
                        // If it has, then we have multiple jobs for the same dataset with the same analysis tool, and we'll thus need to add a prefix to each filename
                        if (!udtOptions.CreateJobPathFiles && File.Exists(Path.Combine(workingDir, synopsisFileName)))
                        {
                            prefixRequired = true;

                            localFolderPath = Path.Combine(workingDir, "FileRename");
                            if (!Directory.Exists(localFolderPath))
                            {
                                Directory.CreateDirectory(localFolderPath);
                            }

                        }
                        else
                        {
                            prefixRequired = false;
                            localFolderPath = string.Copy(workingDir);
                        }

                        // This list tracks files that have been found
                        // If udtOptions.CreateJobPathFiles is true, it has the remote file paths
                        // Otherwise, the file has been copied locally and it has local file paths
                        // Note that files might need to be renamed; that's tracked via lstPendingFileRenames
                        var lstFoundFiles = new List<string>();


                        bool fileCopied;
                        foreach (var sourceFile in lstFilesToGet)
                        {
                            sourceFilename = sourceFile.Key;
                            var fileRequired = sourceFile.Value;

                            // Typically only use FindDataFile() for the first file in lstFilesToGet; we will assume the other files are in that folder
                            // However, if the file resides in MyEMSL then we need to call FindDataFile for every new file because FindDataFile will append the MyEMSL File ID for each file
                            if (string.IsNullOrEmpty(sourceFolderPath) || sourceFolderPath.StartsWith(clsAnalysisResources.MYEMSL_PATH_FLAG))
                            {
                                sourceFolderPath = mAnalysisResources.FileSearch.FindDataFile(sourceFilename);

                                if (string.IsNullOrEmpty(sourceFolderPath))
                                {
                                    var alternateFileName = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(sourceFilename, "Dataset_msgfdb.txt");
                                    sourceFolderPath = mAnalysisResources.FileSearch.FindDataFile(alternateFileName);
                                }
                            }

                            if (!fileRequired)
                            {
                                // It's OK if this file doesn't exist, we'll just log a debug message
                                eLogMsgTypeIfNotFound = clsLogTools.LogLevels.DEBUG;
                            }
                            else
                            {
                                // This file must exist; log an error if it's not found
                                eLogMsgTypeIfNotFound = clsLogTools.LogLevels.ERROR;
                            }

                            if (udtOptions.CreateJobPathFiles && !sourceFolderPath.StartsWith(clsAnalysisResources.MYEMSL_PATH_FLAG))
                            {
                                var sourceFilePath = Path.Combine(sourceFolderPath, sourceFilename);
                                var alternateFileNamelternateFileName = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(sourceFilePath, "Dataset_msgfdb.txt");

                                if (File.Exists(sourceFilePath))
                                {
                                    lstFoundFiles.Add(sourceFilePath);
                                }
                                else if (File.Exists(alternateFileNamelternateFileName))
                                {
                                    lstFoundFiles.Add(alternateFileNamelternateFileName);
                                }
                                else
                                {
                                    if (eLogMsgTypeIfNotFound != clsLogTools.LogLevels.DEBUG)
                                    {
                                        var warningMessage = "Required PHRP file not found: " + sourceFilename;
                                        if (sourceFilename.EndsWith("_msgfplus.zip", StringComparison.InvariantCultureIgnoreCase) ||
                                            sourceFilename.EndsWith("_msgfplus.mzid.gz", StringComparison.InvariantCultureIgnoreCase))
                                        {
                                            warningMessage += "; Confirm job used MSGF+ and not MSGFDB";
                                        }
                                        mAnalysisResources.UpdateStatusMessage(warningMessage);
                                        OnWarningEvent("Required PHRP file not found: " + sourceFilePath);
                                        mAnalysisResources.RestoreCachedDataAndJobInfo();
                                        return false;
                                    }
                                }

                            }
                            else
                            {
                                // Note for files in MyEMSL, this call will simply add the file to the download queue; use ProcessMyEMSLDownloadQueue() to retrieve the file
                                fileCopied = mAnalysisResources.CopyFileToWorkDir(sourceFilename, sourceFolderPath, localFolderPath, eLogMsgTypeIfNotFound);

                                if (!fileCopied)
                                {
                                    var alternateFileName = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(sourceFilename, "Dataset_msgfdb.txt");
                                    fileCopied = mAnalysisResources.CopyFileToWorkDir(alternateFileName, sourceFolderPath, localFolderPath, eLogMsgTypeIfNotFound);
                                    if (fileCopied)
                                    {
                                        sourceFilename = alternateFileName;
                                    }
                                }


                                if (!fileCopied)
                                {
                                    if (eLogMsgTypeIfNotFound != clsLogTools.LogLevels.DEBUG)
                                    {
                                        OnErrorEvent("CopyFileToWorkDir returned False for " + sourceFilename + " using folder " + sourceFolderPath);
                                        mAnalysisResources.RestoreCachedDataAndJobInfo();
                                        return false;
                                    }

                                }
                                else
                                {
                                    OnStatusEvent("Copied " + sourceFilename + " from folder " + sourceFolderPath);
                                    lstFoundFiles.Add(Path.Combine(localFolderPath, sourceFilename));

                                    if (prefixRequired)
                                    {
                                        lstPendingFileRenames.Add(sourceFilename);
                                    }
                                    else
                                    {
                                        mAnalysisResources.AddResultFileToSkip(sourceFilename);
                                    }
                                }
                            }

                        }
                        // in lstFilesToGet

                        var success = mAnalysisResources.ProcessMyEMSLDownloadQueue();
                        if (!success)
                        {
                            mAnalysisResources.RestoreCachedDataAndJobInfo();
                            return false;
                        }

                        // Now perform any required file renames
                        foreach (var fileToRename in lstPendingFileRenames)
                        {
                            if (RenameDuplicatePHRPFile(localFolderPath, fileToRename, workingDir,
                                "Job" + dataPkgJob.Job + "_", dataPkgJob.Job,
                                out var newFilePath))
                            {
                                // Rename succeeded
                                lstFoundFiles.Remove(Path.Combine(localFolderPath, fileToRename));
                                lstFoundFiles.Add(newFilePath);
                            }
                            else
                            {
                                // Rename failed
                                mAnalysisResources.RestoreCachedDataAndJobInfo();
                                return false;
                            }
                        }

                        if (udtOptions.RetrieveDTAFiles)
                        {
                            // Find the _dta.txt or .mzML.gz file that was used for a MSGF+ search

                            var mzIDFileToInspect = string.Empty;

                            // Need to examine the .mzid file to determine which file was used for the search
                            foreach (var foundFile in lstFoundFiles)
                            {
                                if (candidateMzIdFiles.Contains(Path.GetFileName(foundFile)))
                                {
                                    mzIDFileToInspect = foundFile;
                                    break;
                                }
                            }

                            var searchUsedmzML = false;
                            if (!string.IsNullOrEmpty(mzIDFileToInspect))
                            {
                                searchUsedmzML = MSGFPlusSearchUsedMzML(mzIDFileToInspect, ionicZipTools);
                            }

                            if (searchUsedmzML)
                            {
                                var stepToolFilter = string.Empty;
                                if (dataPkgJob.Tool.StartsWith("msgfplus", StringComparison.InvariantCultureIgnoreCase))
                                {
                                    stepToolFilter = "MSGFPlus";
                                }

                                var mzMLFilePathRemote = FindMzMLForJob(dataPkgJob.Dataset, dataPkgJob.Job, stepToolFilter, workingDir);

                                if (!string.IsNullOrEmpty(mzMLFilePathRemote))
                                {
                                    if (udtOptions.CreateJobPathFiles && !mzMLFilePathRemote.StartsWith(clsAnalysisResources.MYEMSL_PATH_FLAG))
                                    {
                                        if (!lstFoundFiles.Contains(mzMLFilePathRemote))
                                        {
                                            lstFoundFiles.Add(mzMLFilePathRemote);
                                        }
                                    }
                                    else
                                    {
                                        // Note for files in MyEMSL, this call will simply add the file to the download queue
                                        // Use ProcessMyEMSLDownloadQueue() to retrieve the file
                                        var sourceMzMLFile = new FileInfo(mzMLFilePathRemote);
                                        var targetMzMLFile = new FileInfo(Path.Combine(localFolderPath, sourceMzMLFile.Name));

                                        // Only copy the .mzML.gz file if it does not yet exist locally
                                        if (!targetMzMLFile.Exists)
                                        {
                                            eLogMsgTypeIfNotFound = clsLogTools.LogLevels.ERROR;

                                            fileCopied = mAnalysisResources.CopyFileToWorkDir(sourceMzMLFile.Name, sourceMzMLFile.DirectoryName, localFolderPath, eLogMsgTypeIfNotFound);

                                            if (fileCopied)
                                            {
                                                OnStatusEvent("Copied " + sourceMzMLFile.Name + " from folder " + sourceMzMLFile.DirectoryName);
                                                lstFoundFiles.Add(Path.Combine(localFolderPath, sourceMzMLFile.Name));

                                            }
                                        }

                                    }
                                }

                            }
                            else
                            {
                                // Find the CDTA file

                                if (udtOptions.CreateJobPathFiles)
                                {
                                    var sourceConcatenatedDTAFilePath = mAnalysisResources.FileSearch.FindCDTAFile(out var errorMessage);

                                    if (string.IsNullOrEmpty(sourceConcatenatedDTAFilePath))
                                    {
                                        OnErrorEvent(errorMessage);
                                        mAnalysisResources.RestoreCachedDataAndJobInfo();
                                        return false;
                                    }

                                    lstFoundFiles.Add(sourceConcatenatedDTAFilePath);
                                }
                                else
                                {
                                    if (!mAnalysisResources.FileSearch.RetrieveDtaFiles())
                                    {
                                        // Errors were reported in function call, so just return
                                        mAnalysisResources.RestoreCachedDataAndJobInfo();
                                        return false;
                                    }

                                    // The retrieved file is probably named Dataset_dta.zip
                                    // We'll add it to lstFoundFiles, but the exact name is not critical
                                    lstFoundFiles.Add(Path.Combine(workingDir, datasetName + "_dta.zip"));
                                }
                            }

                        }

                        if (udtOptions.CreateJobPathFiles)
                        {
                            var jobInfoFilePath = GetJobInfoFilePath(dataPkgJob.Job);
                            using (var swJobInfoFile = new StreamWriter(new FileStream(jobInfoFilePath, FileMode.Append, FileAccess.Write, FileShare.Read)))
                            {
                                foreach (var filePath in lstFoundFiles)
                                {
                                    swJobInfoFile.WriteLine(filePath);
                                }
                            }
                        }
                        else
                        {
                            // Unzip any mzid files that were found

                            if (zipFileCandidates.Count > 0 || gzipFileCandidates.Count > 0)
                            {
                                var unzippedFilePath = string.Empty;

                                foreach (var gzipCandidate in gzipFileCandidates)
                                {
                                    var fiFileToUnzip = new FileInfo(Path.Combine(workingDir, gzipCandidate));
                                    if (fiFileToUnzip.Exists)
                                    {
                                        ionicZipTools.GUnzipFile(fiFileToUnzip.FullName);
                                        unzippedFilePath = MostRecentUnzippedFile(ionicZipTools);
                                        break;
                                    }
                                }

                                if (string.IsNullOrEmpty(unzippedFilePath))
                                {
                                    foreach (var zipCandidate in zipFileCandidates)
                                    {
                                        var fiFileToUnzip = new FileInfo(Path.Combine(workingDir, zipCandidate));
                                        if (fiFileToUnzip.Exists)
                                        {
                                            ionicZipTools.UnzipFile(fiFileToUnzip.FullName);
                                            unzippedFilePath = MostRecentUnzippedFile(ionicZipTools);
                                            break;
                                        }
                                    }
                                }

                                if (string.IsNullOrEmpty(unzippedFilePath))
                                {
                                    OnErrorEvent("Could not find either the _msgfplus.zip file or the _msgfplus.mzid.gz file for dataset");
                                    mAnalysisResources.RestoreCachedDataAndJobInfo();
                                    return false;
                                }

                                if (prefixRequired)
                                {
                                    if (RenameDuplicatePHRPFile(
                                        workingDir, datasetName + "_msgfplus.mzid",
                                        workingDir, "Job" + dataPkgJob.Job + "_",
                                        dataPkgJob.Job,
                                        out unzippedFilePath))
                                    {
                                    }
                                    else
                                    {
                                        mAnalysisResources.RestoreCachedDataAndJobInfo();
                                        return false;
                                    }
                                }

                                lstFoundFiles.Add(unzippedFilePath);

                            }

                            if (!string.IsNullOrWhiteSpace(zippedPepXmlFile))
                            {
                                // Unzip _pepXML.zip if it exists
                                var fiFileToUnzip = new FileInfo(Path.Combine(workingDir, zippedPepXmlFile));
                                if (fiFileToUnzip.Exists)
                                {
                                    ionicZipTools.UnzipFile(fiFileToUnzip.FullName);
                                    lstFoundFiles.Add(MostRecentUnzippedFile(ionicZipTools));
                                }
                            }
                        }

                    }

                    // Find the instrument data file or folder if a new dataset
                    if (!dctRawFileRetrievalCommands.ContainsKey(dataPkgJob.DatasetID))
                    {
                        if (!RetrieveDataPackageInstrumentFile(dataPkgJob, udtOptions, dctRawFileRetrievalCommands, dctInstrumentDataToRetrieve, dctDatasetRawFilePaths))
                        {
                            mAnalysisResources.RestoreCachedDataAndJobInfo();
                            return false;
                        }
                    }

                    jobsProcessed += 1;
                    var sngProgress = clsAnalysisToolRunnerBase.ComputeIncrementalProgress(
                        progressPercentAtStart, progressPercentAtFinish, jobsProcessed,
                        lstDataPackagePeptideHitJobs.Count + lstAdditionalJobs.Count);

                    OnProgressUpdate("RetrieveDataPackagePeptideHitJobPHRPFiles (PeptideHit Jobs)", sngProgress);


                }
                // dataPkgJob in lstDataPackagePeptideHitJobs

                // Now process the additional jobs to retrieve the instrument data for each one

                foreach (var dataPkgJob in lstAdditionalJobs)
                {
                    // Find the instrument data file or folder if a new dataset
                    if (!dctRawFileRetrievalCommands.ContainsKey(dataPkgJob.DatasetID))
                    {
                        if (!RetrieveDataPackageInstrumentFile(dataPkgJob, udtOptions, dctRawFileRetrievalCommands, dctInstrumentDataToRetrieve, dctDatasetRawFilePaths))
                        {
                            mAnalysisResources.RestoreCachedDataAndJobInfo();
                            return false;
                        }
                    }

                    jobsProcessed += 1;
                    var sngProgress = clsAnalysisToolRunnerBase.ComputeIncrementalProgress(
                        progressPercentAtStart, progressPercentAtFinish, jobsProcessed,
                        lstDataPackagePeptideHitJobs.Count + lstAdditionalJobs.Count);

                    OnProgressUpdate("RetrieveDataPackagePeptideHitJobPHRPFiles (Additional Jobs)", sngProgress);


                }
                // in lstAdditionalJobs

                // Look for any datasets that are associated with this data package yet have no jobs
                foreach (var datasetItem in dctDataPackageDatasets)
                {
                    if (!dctRawFileRetrievalCommands.ContainsKey(datasetItem.Key))
                    {
                        var dataPkgJob = clsAnalysisResources.GetPseudoDataPackageJobInfo(datasetItem.Value);
                        dataPkgJob.ResultsFolderName = "Undefined_Folder";

                        if (!mAnalysisResources.OverrideCurrentDatasetAndJobInfo(dataPkgJob))
                        {
                            // Error message has already been logged
                            mAnalysisResources.RestoreCachedDataAndJobInfo();
                            return false;
                        }

                        if (!RetrieveDataPackageInstrumentFile(dataPkgJob, udtOptions, dctRawFileRetrievalCommands, dctInstrumentDataToRetrieve, dctDatasetRawFilePaths))
                        {
                            mAnalysisResources.RestoreCachedDataAndJobInfo();
                            return false;
                        }
                    }
                }

                // Restore the dataset and job info for this aggregation job
                mAnalysisResources.RestoreCachedDataAndJobInfo();

                if (dctRawFileRetrievalCommands.Count == 0)
                {
                    OnErrorEvent("Did not find any datasets associated with this job's data package ID (" + mDataPackageInfoLoader.DataPackageID + ")");
                    return false;
                }

                if (dctRawFileRetrievalCommands.Count > 0)
                {
                    // Create a batch file with commands for retrieve the dataset files
                    var batchFilePath = Path.Combine(workingDir, "RetrieveInstrumentData.bat");
                    using (var swOutfile = new StreamWriter(new FileStream(batchFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    {
                        foreach (var item in dctRawFileRetrievalCommands.Values)
                        {
                            swOutfile.WriteLine(item);
                        }
                    }

                    // Store the dataset paths in a Packed Job Parameter
                    mAnalysisResources.StorePackedJobParameterDictionary(dctDatasetRawFilePaths, clsAnalysisResources.JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS);

                }

                if (udtOptions.RetrieveMzXMLFile)
                {
                    // All of the PHRP data files have been successfully retrieved; now retrieve the mzXML files or the .Raw files
                    // If udtOptions.CreateJobPathFiles = True then we will create StoragePathInfo files
                    var success = RetrieveDataPackageMzXMLFiles(dctInstrumentDataToRetrieve, udtOptions);
                    return success;
                }
                else
                {
                    return true;
                }

            }
            catch (Exception ex)
            {
                OnErrorEvent("RetrieveDataPackagePeptideHitJobPHRPFiles; Exception during copy of file: " + sourceFilename + " from folder " + sourceFolderPath, ex);
                return false;
            }

        }


        /// <summary>
        /// Look for the .mzXML and/or .raw file
        /// </summary>
        /// <param name="dataPkgJob"></param>
        /// <param name="udtOptions"></param>
        /// <param name="dctRawFileRetrievalCommands">Commands to copy .raw files to the local computer (to be placed in a batch file)</param>
        /// <param name="dctInstrumentDataToRetrieve">Instrument files that need to be copied locally so that an mzXML file can be made</param>
        /// <param name="dctDatasetRawFilePaths">Mapping of dataset name to the remote location of the .raw file</param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool RetrieveDataPackageInstrumentFile(
            clsDataPackageJobInfo dataPkgJob, udtDataPackageRetrievalOptionsType udtOptions,
            IDictionary<int, string> dctRawFileRetrievalCommands,
            IDictionary<clsDataPackageJobInfo,
            KeyValuePair<string, string>> dctInstrumentDataToRetrieve,
            IDictionary<string, string> dctDatasetRawFilePaths)
        {

            if (udtOptions.RetrieveMzXMLFile)
            {
                // See if a .mzXML file already exists for this dataset

                var mzXMLFilePath = mAnalysisResources.FileSearch.FindMZXmlFile(out var hashcheckFilePath);

                if (string.IsNullOrEmpty(mzXMLFilePath))
                {
                    // mzXML file not found
                    if (dataPkgJob.RawDataType == clsAnalysisResources.RAW_DATA_TYPE_DOT_RAW_FILES)
                    {
                        // Will need to retrieve the .Raw file for this dataset
                        dctInstrumentDataToRetrieve.Add(dataPkgJob, new KeyValuePair<string, string>(string.Empty, string.Empty));
                    }
                    else
                    {
                        OnErrorEvent("mzXML file not found for dataset " + dataPkgJob.Dataset + " and dataset file type is not a .Raw file and we thus cannot auto-create the missing mzXML file");
                        return false;
                    }
                }
                else
                {
                    dctInstrumentDataToRetrieve.Add(dataPkgJob, new KeyValuePair<string, string>(mzXMLFilePath, hashcheckFilePath));
                }
            }

            var rawFilePath = mAnalysisResources.FolderSearch.FindDatasetFileOrFolder(out var isFolder, udtOptions.AssumeInstrumentDataUnpurged);


            if (!string.IsNullOrEmpty(rawFilePath))
            {
                string copyCommand;
                if (isFolder)
                {
                    var fileName = Path.GetFileName(rawFilePath);
                    copyCommand = "if not exist " + fileName + " copy " + rawFilePath + @" .\" + fileName + " /S /I";
                }
                else
                {
                    // Make sure the case of the filename matches the case of the dataset name
                    // Also, make sure the extension is lowercase
                    var fileName = dataPkgJob.Dataset + Path.GetExtension(rawFilePath).ToLower();
                    copyCommand = "if not exist " + fileName + " copy " + rawFilePath + " " + fileName;
                }
                dctRawFileRetrievalCommands.Add(dataPkgJob.DatasetID, copyCommand);
                dctDatasetRawFilePaths.Add(dataPkgJob.Dataset, rawFilePath);
            }

            return true;

        }

        /// <summary>
        /// Retrieve the .mzXML files for the jobs in dctInstrumentDataToRetrieve
        /// </summary>
        /// <param name="dctInstrumentDataToRetrieve">
        /// The keys in this dictionary are JobInfo entries
        /// The values in this dictionary are KeyValuePairs of path to the .mzXML file and path to the .hashcheck file (if any).
        /// The KeyValuePair will have empty strings if the .Raw file needs to be retrieved
        /// </param>
        /// <param name="udtOptions">File retrieval options</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>If udtOptions.CreateJobPathFiles is True, then will create StoragePathInfo files for the .mzXML or .Raw files</remarks>
        public bool RetrieveDataPackageMzXMLFiles(Dictionary<clsDataPackageJobInfo, KeyValuePair<string, string>> dctInstrumentDataToRetrieve, udtDataPackageRetrievalOptionsType udtOptions)
        {

            bool success;

            var currentJob = 0;

            try
            {
                // Make sure we don't move the .Raw, .mzXML, .mzML or .gz files into the results folder
                mAnalysisResources.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_RAW_EXTENSION);
                // .Raw file
                mAnalysisResources.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_MZXML_EXTENSION);
                // .mzXML file
                mAnalysisResources.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_MZML_EXTENSION);
                // .mzML file
                mAnalysisResources.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_GZ_EXTENSION);
                // .gz file

                bool createStoragePathInfoOnly;
                if (udtOptions.CreateJobPathFiles)
                {
                    createStoragePathInfoOnly = true;
                }
                else
                {
                    createStoragePathInfoOnly = false;
                }

                var lstDatasetsProcessed = new SortedSet<string>();

                mAnalysisResources.CacheCurrentDataAndJobInfo();

                var dtLastProgressUpdate = DateTime.UtcNow;
                var datasetsProcessed = 0;
                var datasetsToProcess = dctInstrumentDataToRetrieve.Count;

                // Retrieve the instrument data
                // Note that RetrieveMZXmlFileUsingSourceFile will add MyEMSL files to the download queue

                foreach (var kvItem in dctInstrumentDataToRetrieve)
                {
                    // The key in kvMzXMLFileInfo is the path to the .mzXML or .mzML file
                    // The value in kvMzXMLFileInfo is the path to the .hashcheck file
                    var kvMzXMLFileInfo = kvItem.Value;
                    var mzXMLFilePath = kvMzXMLFileInfo.Key;
                    var hashcheckFilePath = kvMzXMLFileInfo.Value;

                    currentJob = kvItem.Key.Job;


                    if (!lstDatasetsProcessed.Contains(kvItem.Key.Dataset))
                    {
                        if (!mAnalysisResources.OverrideCurrentDatasetAndJobInfo(kvItem.Key))
                        {
                            // Error message has already been logged
                            return false;
                        }

                        if (string.IsNullOrEmpty(mzXMLFilePath))
                        {
                            // The .mzXML or .mzML file was not found; we will need to obtain the .Raw file
                            success = false;
                        }
                        else
                        {
                            // mzXML or .mzML file exists; either retrieve it or create a StoragePathInfo file
                            success = mAnalysisResources.FileSearch.RetrieveMZXmlFileUsingSourceFile(createStoragePathInfoOnly, mzXMLFilePath, hashcheckFilePath);
                        }

                        if (success)
                        {
                            // .mzXML or .mzML file found and copied locally
                            string msXmlFileExtension;

                            if (mzXMLFilePath.EndsWith(clsAnalysisResources.DOT_GZ_EXTENSION, StringComparison.InvariantCultureIgnoreCase))
                            {
                                msXmlFileExtension = Path.GetExtension(mzXMLFilePath.Substring(0, mzXMLFilePath.Length - clsAnalysisResources.DOT_GZ_EXTENSION.Length));
                            }
                            else
                            {
                                msXmlFileExtension = Path.GetExtension(mzXMLFilePath);
                            }

                            if (udtOptions.CreateJobPathFiles)
                            {
                                OnStatusEvent(msXmlFileExtension + " file found for job " + currentJob + " at " + mzXMLFilePath);
                            }
                            else
                            {
                                OnStatusEvent("Copied " + msXmlFileExtension + " file for job " + currentJob + " from " + mzXMLFilePath);
                            }
                        }
                        else
                        {
                            // .mzXML file not found (or problem adding to the MyEMSL download queue)
                            // Find or retrieve the .Raw file, which can be used to create the .mzXML file
                            // (the plugin will actually perform the work of converting the file; as an example, see the MSGF plugin)

                            if (!mAnalysisResources.FileSearch.RetrieveSpectra(kvItem.Key.RawDataType, createStoragePathInfoOnly, maxAttempts: 1))
                            {
                                OnErrorEvent("Error occurred retrieving instrument data file for job " + currentJob);
                                return false;
                            }

                        }

                        lstDatasetsProcessed.Add(kvItem.Key.Dataset);
                    }

                    datasetsProcessed += 1;

                    // Compute a % complete value between 0 and 2%
                    var percentComplete = datasetsProcessed / (float)datasetsToProcess * 2;
                    OnProgressUpdate("Retrieving MzXML files", percentComplete);


                    if ((DateTime.UtcNow.Subtract(dtLastProgressUpdate).TotalSeconds >= 30))
                    {
                        dtLastProgressUpdate = DateTime.UtcNow;

                        OnStatusEvent("Retrieving mzXML files: " + datasetsProcessed + " / " + datasetsToProcess + " datasets");
                    }

                }

                // Restore the dataset and job info for this aggregation job
                mAnalysisResources.RestoreCachedDataAndJobInfo();

                success = true;

            }
            catch (Exception ex)
            {
                OnErrorEvent("RetrieveDataPackageMzXMLFiles; Exception retrieving mzXML file or .Raw file for job " + currentJob, ex);
                success = false;
            }

            return success;

        }

        #region "Event Handlers"

        private void ProcedureExecutor_DBErrorEvent(string message, Exception ex)
        {
            if (message.Contains("permission was denied"))
            {
                try
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, message);
                }
                catch (Exception ex2)
                {
                    clsGlobal.ErrorWritingToLog(message, ex2);
                }
            }

            OnErrorEvent(message);
        }

        #endregion
    }
}