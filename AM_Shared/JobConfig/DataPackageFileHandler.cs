using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.FileAndDirectoryTools;
using MyEMSLReader;
using PHRPReader;
using PRISM;
using PRISM.Logging;
using PRISMDatabaseUtils;

namespace AnalysisManagerBase.JobConfig
{
    /// <summary>
    /// Data package file handler
    /// </summary>
    public class DataPackageFileHandler : EventNotifier
    {
        // ReSharper disable once CommentTypo
        // Ignore Spelling: cryptum, gzip, kv, mzid, msgfplus, unpurged

        /// <summary>
        /// Job info file prefix
        /// </summary>
        public const string JOB_INFO_FILE_PREFIX = "JobInfoFile_Job";

        private const string SP_NAME_GET_JOB_STEP_INPUT_FOLDER = "get_job_step_input_folder";

        /// <summary>
        /// File that tracks Job number and whether the search used a .mzML file
        /// </summary>
        public const string DATA_PKG_JOB_METADATA_FILE = "DataPkgJobMetadata.txt";

        /// <summary>
        /// Data package retrieval options
        /// </summary>
        public struct DataPackageRetrievalOptionsType
        {
            /// <summary>
            /// Set to true to create a text file for each job listing the full path to the files that would be retrieved for that job
            /// Example filename: FilePathInfo_Job950000.txt
            /// </summary>
            /// <remarks>No files are actually retrieved when this is set to true</remarks>
            public bool CreateJobPathFiles;

            /// <summary>
            /// Set to true to obtain the mzXML or mzML file for the dataset associated with this job
            /// </summary>
            /// <remarks>If the .mzXML file does not exist, retrieves the instrument data file (e.g. Thermo .raw file)</remarks>
            public bool RetrieveMzXMLFile;

            /// <summary>
            /// Set to true to retrieve _DTA.txt files (the PRIDE Converter will convert these to .mgf files)
            /// </summary>
            /// <remarks>If the search used a .mzML instead of a _dta.txt file, the .mzML.gz file will be retrieved</remarks>
            public bool RetrieveDTAFiles;

            /// <summary>
            /// Set to true to obtain MS-GF+ .mzid.gz files
            /// </summary>
            public bool RetrieveMzidFiles;

            /// <summary>
            /// Set to true to obtain .pepXML files (typically stored as _pepXML.zip)
            /// </summary>
            public bool RetrievePepXMLFiles;

            /// <summary>
            /// Set to true to obtain the _syn.txt file and related PHRP files
            /// </summary>
            public bool RetrievePHRPFiles;

            /// <summary>
            /// When true, assume that the instrument file (e.g., .raw file) exists in the dataset storage directory
            /// and do not search in MyEMSL or in the archive for the file
            /// </summary>
            /// <remarks>Even if the instrument file has been purged from the storage directory, still report "success" when searching for the instrument file</remarks>
            public bool AssumeInstrumentDataUnpurged;

            /// <summary>
            /// Remote transfer directory path; used to save DataPkgJobMetadata.txt
            /// </summary>
            public string RemoteTransferDirectoryPath;
        }

        private struct DataPackageJobMetadata
        {
            /// <summary>
            /// True if MS-GF+ searched a .mzML file; if false, likely searched a _dta.txt file
            /// </summary>
            public bool SearchUsedMzML;
        }

        private readonly AnalysisResources mAnalysisResources;

        /// <summary>
        /// Instance of IDBTools
        /// </summary>
        private readonly IDBTools mDbTools;

        private readonly DataPackageInfoLoader mDataPackageInfoLoader;

        /// <summary>
        /// Error message
        /// </summary>
        /// <remarks>
        /// This is cleared when <see cref="RetrieveDataPackageDatasetFiles"/>
        /// or <see cref="RetrieveDataPackagePeptideHitJobPHRPFiles"/> is called
        /// </remarks>
        public string ErrorMessage { get; private set; } = "";

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="dbTools">Instance of IDBTools</param>
        /// <param name="dataPackageID">Data package ID</param>
        /// <param name="resourcesClass">Resource class</param>
        public DataPackageFileHandler(IDBTools dbTools, int dataPackageID, AnalysisResources resourcesClass)
        {
            mAnalysisResources = resourcesClass;

            mDataPackageInfoLoader = new DataPackageInfoLoader(resourcesClass, dbTools, dataPackageID);

            mDbTools = dbTools;
        }

        /// <summary>
        /// Contact the database to determine the exact directory name of the MSXML_Gen or Mz_Refinery directory
        /// that has the CacheInfo file for the mzML file used by a given job
        /// Look for the cache info file in that directory, then use that to find the location of the actual .mzML.gz file
        /// </summary>
        /// <remarks>Uses the highest job step to determine the input directory, meaning the .mzML.gz file returned will be the one used by MS-GF+</remarks>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="job">Job number</param>
        /// <param name="stepToolFilter">step tool to filter on; if an empty string, returns the input directory for the primary step tool for the job</param>
        /// <param name="workDirInfo">Working directory</param>
        /// <returns>Path to the .mzML or .mzML.gz file; empty string if not found</returns>
        private string FindMzMLForJob(string datasetName, int job, string stepToolFilter, FileSystemInfo workDirInfo)
        {
            if (Global.OfflineMode)
            {
                throw new Exception("FindMzMLForJob does not support offline mode");
            }

            try
            {
                // Set up the command object prior to SP execution
                var cmd = mDbTools.CreateCommand(SP_NAME_GET_JOB_STEP_INPUT_FOLDER, CommandType.StoredProcedure);

                mDbTools.AddParameter(cmd, "@job", SqlType.Int).Value = job;

                var stepToolFilterParam = mDbTools.AddParameter(cmd, "@stepToolFilter", SqlType.VarChar, 8000, stepToolFilter);

                var inputFolderParam = mDbTools.AddParameter(cmd, "@inputFolderName", SqlType.VarChar, 128, string.Empty, ParameterDirection.InputOutput);
                var stepToolMatchParam = mDbTools.AddParameter(cmd, "@stepToolMatch", SqlType.VarChar, 64, string.Empty, ParameterDirection.InputOutput);

                var matchFound = false;
                var inputDirectoryName = string.Empty;
                var stepToolMatch = string.Empty;

                mDbTools.DebugEvent += OnDebugEvent;
                mDbTools.ErrorEvent += ProcedureExecutor_DBErrorEvent;
                mDbTools.WarningEvent += OnWarningEvent;

                while (!matchFound)
                {
                    // Call the procedure
                    var resCode = mDbTools.ExecuteSP(cmd, 1);

                    if (resCode != 0)
                    {
                        OnErrorEvent("ExecuteSP() reported result code {0} calling {1}",
                            resCode, SP_NAME_GET_JOB_STEP_INPUT_FOLDER);
                    }

                    inputDirectoryName = Convert.ToString(inputFolderParam.Value);
                    stepToolMatch = Convert.ToString(stepToolMatchParam.Value);

                    if (string.IsNullOrWhiteSpace(inputDirectoryName))
                    {
                        if (string.IsNullOrEmpty(stepToolFilter))
                        {
                            ErrorMessage = string.Format("Unable to determine the input directory for job {0}", job);
                            OnErrorEvent(ErrorMessage);
                            return string.Empty;
                        }

                        OnStatusEvent("Unable to determine the input directory for job {0} and step tool {1}; " +
                                      "will try again without a step tool filter", job, stepToolFilter);
                        stepToolFilter = string.Empty;
                        stepToolFilterParam.Value = stepToolFilter;
                    }
                    else
                    {
                        matchFound = true;
                    }
                }

                OnStatusEvent("Determined the input directory for job {0} is {1}, step tool {2}", job, inputDirectoryName, stepToolMatch);

                // Look for a CacheInfo.txt file in the matched input directory
                // Note that FindValidDirectory will search both the dataset directory and in inputDirectoryName below the dataset directory

                var datasetDirectoryPath = mAnalysisResources.FindValidDirectory(
                    datasetName, "*_CacheInfo.txt", inputDirectoryName,
                    maxAttempts: 1, logDirectoryNotFound: false,
                    retrievingInstrumentDataDir: false, validDirectoryFound: out var validDirectoryFound,
                    assumeUnpurged: false, directoryNotFoundMessage: out _);

                if (!validDirectoryFound)
                {
                    return string.Empty;
                }

                var cacheInfoFileName = string.Empty;
                string cacheInfoFileSourceType;

                // datasetFolderPath will hold the full path to the actual directory with the CacheInfo.txt file
                // ReSharper disable once CommentTypo
                // For example: \\proto-6\QExactP02\2016_2\Biodiversity_A_cryptum_FeTSB\Mz_Refinery_1_195_501572

                if (datasetDirectoryPath.StartsWith(AnalysisResources.MYEMSL_PATH_FLAG))
                {
                    // File found in MyEMSL
                    // Determine the MyEMSL FileID by searching for the expected file in mMyEMSLUtilities.RecentlyFoundMyEMSLFiles

                    foreach (var myEmslFile in mAnalysisResources.MyEMSLUtils.RecentlyFoundMyEMSLFiles)
                    {
                        var archivedFile = new FileInfo(myEmslFile.FileInfo.RelativePathWindows);

                        if (archivedFile.Name.EndsWith("_CacheInfo.txt", StringComparison.OrdinalIgnoreCase))
                        {
                            mAnalysisResources.MyEMSLUtils.AddFileToDownloadQueue(myEmslFile.FileInfo);
                            cacheInfoFileName = myEmslFile.FileInfo.Filename;
                            break;
                        }
                    }

                    if (string.IsNullOrEmpty(cacheInfoFileName))
                    {
                        OnErrorEvent("FindValidDirectory reported a match to a file in MyEMSL (" + datasetDirectoryPath + ") but MyEMSLUtilities.RecentlyFoundMyEMSLFiles is empty");
                        return string.Empty;
                    }

                    mAnalysisResources.ProcessMyEMSLDownloadQueue();
                    cacheInfoFileSourceType = "MyEMSL";
                }
                else
                {
                    var sourceDirectory = new DirectoryInfo(datasetDirectoryPath);
                    cacheInfoFileSourceType = sourceDirectory.FullName;

                    if (!sourceDirectory.Exists)
                    {
                        OnErrorEvent("FindValidDirectory reported a match to directory " + cacheInfoFileSourceType + " but the directory was not found");
                        return string.Empty;
                    }

                    var cacheInfoFiles = sourceDirectory.GetFiles("*_CacheInfo.txt");

                    if (cacheInfoFiles.Length == 0)
                    {
                        var sourceDirectoryAlt = new DirectoryInfo(Path.Combine(sourceDirectory.FullName, inputDirectoryName));

                        if (sourceDirectoryAlt.Exists)
                        {
                            cacheInfoFiles = sourceDirectoryAlt.GetFiles("*_CacheInfo.txt");
                        }

                        if (cacheInfoFiles.Length > 0)
                        {
                            cacheInfoFileSourceType = sourceDirectoryAlt.FullName;
                        }
                        else
                        {
                            OnErrorEvent("FindValidDirectory reported that directory " + cacheInfoFileSourceType +
                                         " has a _CacheInfo.txt file, but none was found");
                            return string.Empty;
                        }
                    }

                    var sourceCacheInfoFile = cacheInfoFiles.First();

                    var success = mAnalysisResources.CopyFileToWorkDir(
                        sourceCacheInfoFile.Name, sourceCacheInfoFile.DirectoryName,
                        workDirInfo.FullName, BaseLogger.LogLevels.ERROR);

                    if (!success)
                    {
                        // The error should have already been logged
                        return string.Empty;
                    }

                    cacheInfoFileName = sourceCacheInfoFile.Name;
                }

                // Open the CacheInfo file and read the first line

                var localCacheInfoFile = new FileInfo(Path.Combine(workDirInfo.FullName, cacheInfoFileName));

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
                    OnStatusEvent("Found remote mzML file for job {0}: {1}", job, remoteMsXmlFilePath);
                }

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
        /// <param name="datasetName">dataset name</param>
        /// <param name="splitFastaResultID">Split FASTA result ID</param>
        /// <param name="zipFileCandidates">Potential names of zip files to decompress</param>
        /// <param name="gzipFileCandidates">Potential names of gzip files to decompress</param>
        private void GetMzIdFilesToFind(
            string datasetName,
            int splitFastaResultID,
            ICollection<string> zipFileCandidates,
            ICollection<string> gzipFileCandidates)
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

        /// <summary>
        /// Construct the path to the JobInfo file for the given job
        /// </summary>
        /// <param name="job">Job number</param>
        /// <param name="workDirPath">Working directory path</param>
        public static string GetJobInfoFilePath(int job, string workDirPath)
        {
            return Path.Combine(workDirPath, JOB_INFO_FILE_PREFIX + job + ".txt");
        }

        /// <summary>
        /// Return the full path to the most recently unzipped file (.zip or .gz)
        /// Returns an empty string if no recent unzipped files
        /// </summary>
        /// <param name="zipTools">Zip tools instance</param>
        private string MostRecentUnzippedFile(ZipFileTools zipTools)
        {
            if (zipTools.MostRecentUnzippedFiles.Count > 0)
            {
                return zipTools.MostRecentUnzippedFiles.First().Value;
            }

            return string.Empty;
        }

        /// <summary>
        /// Move the file into a subdirectory below the working directory
        /// </summary>
        /// <param name="workDirInfo">Working directory</param>
        /// <param name="dataPkgJob">Data package job info</param>
        /// <param name="sourceFile">Source file</param>
        /// <returns>Full path to the destination file path</returns>
        private string MoveFileToJobSubdirectory(FileSystemInfo workDirInfo, DataPackageJobInfo dataPkgJob, FileInfo sourceFile)
        {
            var jobSubDirectory = new DirectoryInfo(Path.Combine(workDirInfo.FullName, "Job" + dataPkgJob.Job));

            if (!jobSubDirectory.Exists)
            {
                jobSubDirectory.Create();
            }

            var destinationFilePath = Path.Combine(jobSubDirectory.FullName, sourceFile.Name);
            sourceFile.MoveTo(destinationFilePath);
            sourceFile.Refresh();

            return sourceFile.FullName;
        }

        /// <summary>
        /// Open an .mzid or .mzid.gz file and look for the SpectraData element
        /// If the location of the file specified by SpectraData points to a .mzML or .mzML.gz file, return true
        /// Otherwise, return false
        /// </summary>
        /// <param name="mzIdFileToInspect">Path of the .mzid file to inspect</param>
        /// <param name="zipTools">Zip tools instance</param>
        private bool MSGFPlusSearchUsedMzML(string mzIdFileToInspect, ZipFileTools zipTools)
        {
            try
            {
                var mzidFile = new FileInfo(mzIdFileToInspect);

                if (!mzidFile.Exists)
                {
                    OnErrorEvent("Unable to examine the mzid file to determine whether MS-GF+ searched a .mzML file; file Not found:   " + mzIdFileToInspect);
                    return false;
                }

                string mzidFilePathLocal;
                bool deleteLocalFile;
                bool SearchUsedMzML;

                if (mzidFile.Name.EndsWith(".zip", StringComparison.OrdinalIgnoreCase))
                {
                    if (!zipTools.UnzipFile(mzidFile.FullName))
                    {
                        OnErrorEvent("Error unzipping " + mzidFile.FullName);
                        return false;
                    }

                    mzidFilePathLocal = MostRecentUnzippedFile(zipTools);
                    deleteLocalFile = true;
                }
                else
                {
                    mzidFilePathLocal = mzidFile.FullName;
                    deleteLocalFile = false;
                }

                if (!File.Exists(mzidFilePathLocal))
                {
                    OnErrorEvent("mzid file not found in the working directory; cannot inspect it in MSGFPlusSearchUsedMzML: " + Path.GetFileName(mzidFilePathLocal));
                    return false;
                }

                if (mzidFilePathLocal.EndsWith(AnalysisResources.DOT_GZ_EXTENSION, StringComparison.OrdinalIgnoreCase))
                {
                    using Stream unzippedStream = new GZipStream(new FileStream(mzidFilePathLocal, FileMode.Open, FileAccess.Read, FileShare.Read), CompressionMode.Decompress);
                    using var sourceFileReader = new StreamReader(unzippedStream, Encoding.GetEncoding("ISO-8859-1"));
                    using var xmlReader = new XmlTextReader(sourceFileReader);

                    SearchUsedMzML = MSGFPlusSearchUsedMzML(xmlReader, mzidFilePathLocal);
                }
                else
                {
                    using var xmlReader = new XmlTextReader(mzidFilePathLocal);

                    SearchUsedMzML = MSGFPlusSearchUsedMzML(xmlReader, mzidFilePathLocal);
                }

                if (deleteLocalFile)
                {
                    File.Delete(mzidFilePathLocal);
                }

                return SearchUsedMzML;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception examining the mzid file To determine whether MS-GF+ searched a .mzML file", ex);
                return false;
            }
        }

        /// <summary>
        /// Examine the contents of a .mzid file to determine if a .mzML file was searched
        /// </summary>
        /// <param name="reader">XML reader</param>
        /// <param name="mzidFilePath">.mzid file path</param>
        /// <returns>True if the location attribute of the SpectraData element has a .mzML or .mzML.gz file</returns>
        private bool MSGFPlusSearchUsedMzML(XmlReader reader, string mzidFilePath)
        {
            while (reader.Read())
            {
                if (reader.NodeType != XmlNodeType.Element)
                    continue;

                if (reader.Name != "SpectraData")
                    continue;

                // The location attribute of the SpectraData element has the input file name
                if (!reader.MoveToAttribute("location"))
                {
                    OnErrorEvent(".mzid file has node SpectraData but it does not have attribute location: " + Path.GetFileName(mzidFilePath));
                    return false;
                }

                var spectraDataFileName = Path.GetFileName(reader.Value);
                const string DOT_MZML = AnalysisResources.DOT_MZML_EXTENSION;
                const string DOT_MZML_GZ = AnalysisResources.DOT_MZML_EXTENSION + AnalysisResources.DOT_GZ_EXTENSION;

                return spectraDataFileName.EndsWith(DOT_MZML, StringComparison.OrdinalIgnoreCase) ||
                       spectraDataFileName.EndsWith(DOT_MZML_GZ, StringComparison.OrdinalIgnoreCase);
            }

            OnErrorEvent(".mzid file did not have node SpectraData: " + Path.GetFileName(mzidFilePath));

            return false;
        }

        /// <summary>
        /// Process a single data package peptide hit job
        /// </summary>
        /// <param name="retrievalOptions">File retrieval options</param>
        /// <param name="cachedJobMetadata">Cached job metadata</param>
        /// <param name="zipTools">Zip tools instance</param>
        /// <param name="workDirInfo">Working directory</param>
        /// <param name="dataPkgJob">Data package job</param>
        private bool ProcessOnePeptideHitJob(
            DataPackageRetrievalOptionsType retrievalOptions,
            IDictionary<int, DataPackageJobMetadata> cachedJobMetadata,
            ZipFileTools zipTools,
            FileSystemInfo workDirInfo,
            DataPackageJobInfo dataPkgJob)
        {
            try
            {
                // Keys in this list are filenames; values are true if the file is required and false if not required
                var filesToGet = new SortedList<string, bool>();
                string localDirectoryPath;

                // These two variables track compressed mzid files to look for
                var zipFileCandidates = new List<string>();
                var gzipFileCandidates = new List<string>();

                // This tracks the _pepXML.zip filename, which will be unzipped if it was found
                var zippedPepXmlFile = string.Empty;

                bool prefixRequired;

                var synopsisFileName = ReaderFactory.GetPHRPSynopsisFileName(dataPkgJob.PeptideHitResultType, dataPkgJob.Dataset);
                var synopsisMSGFFileName = ReaderFactory.GetMSGFFileName(synopsisFileName);

                if (retrievalOptions.RetrievePHRPFiles)
                {
                    filesToGet.Add(synopsisFileName, true);

                    filesToGet.Add(ReaderFactory.GetPHRPResultToSeqMapFileName(dataPkgJob.PeptideHitResultType, dataPkgJob.Dataset), true);
                    filesToGet.Add(ReaderFactory.GetPHRPSeqInfoFileName(dataPkgJob.PeptideHitResultType, dataPkgJob.Dataset), true);
                    filesToGet.Add(ReaderFactory.GetPHRPSeqToProteinMapFileName(dataPkgJob.PeptideHitResultType, dataPkgJob.Dataset), true);
                    filesToGet.Add(ReaderFactory.GetPHRPModSummaryFileName(dataPkgJob.PeptideHitResultType, dataPkgJob.Dataset), true);

                    filesToGet.Add(synopsisMSGFFileName, false);
                }

                // Names of mzid result files that we should look for
                // Examples include:
                //  DatasetName_msgfplus.mzid.gz
                //  DatasetName_msgfplus_Part5.mzid.gz
                //  DatasetName_msgfplus.zip
                var candidateMzIdFiles = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

                var datasetName = dataPkgJob.Dataset;

                if (retrievalOptions.RetrieveMzidFiles && dataPkgJob.PeptideHitResultType == PeptideHitResultTypes.MSGFPlus)
                {
                    // Retrieve MS-GF+ .mzid files
                    // They will either be stored as .zip files or as .gz files

                    GetMzIdFilesToFind(datasetName, 0, zipFileCandidates, gzipFileCandidates);

                    foreach (var candidateFile in zipFileCandidates.Union(gzipFileCandidates))
                    {
                        candidateMzIdFiles.Add(candidateFile);
                        filesToGet.Add(candidateFile, false);
                    }

                    if (dataPkgJob.NumberOfClonedSteps > 0)
                    {
                        zipFileCandidates.Clear();
                        gzipFileCandidates.Clear();

                        for (var splitFastaResultID = 1; splitFastaResultID <= dataPkgJob.NumberOfClonedSteps; splitFastaResultID++)
                        {
                            GetMzIdFilesToFind(datasetName, splitFastaResultID, zipFileCandidates, gzipFileCandidates);
                        }

                        foreach (var candidateFile in zipFileCandidates.Union(gzipFileCandidates))
                        {
                            candidateMzIdFiles.Add(candidateFile);
                            filesToGet.Add(candidateFile, false);
                        }
                    }
                }

                if (retrievalOptions.RetrievePepXMLFiles && dataPkgJob.PeptideHitResultType != PeptideHitResultTypes.Unknown ||
                    dataPkgJob.PeptideHitResultType == PeptideHitResultTypes.Sequest)
                {
                    // Retrieve .pepXML files, which are stored as _pepXML.zip files
                    zippedPepXmlFile = datasetName + "_pepXML.zip";
                    filesToGet.Add(zippedPepXmlFile, false);
                }

                // Check whether a synopsis file by this name has already been copied locally
                // If it has, we have multiple jobs for the same dataset with the same analysis tool, and we'll thus need to add a prefix to each filename
                if (!retrievalOptions.CreateJobPathFiles && File.Exists(Path.Combine(workDirInfo.FullName, synopsisFileName)))
                {
                    prefixRequired = true;

                    localDirectoryPath = Path.Combine(workDirInfo.FullName, "FileRename");

                    if (!Directory.Exists(localDirectoryPath))
                    {
                        Directory.CreateDirectory(localDirectoryPath);
                    }
                }
                else
                {
                    prefixRequired = false;
                    localDirectoryPath = workDirInfo.FullName;
                }

                // foundFiles tracks files that have been found
                // If retrievalOptions.CreateJobPathFiles is true, it has the remote file paths
                // Otherwise, the file has been copied locally, and it has local file paths
                // Note that files might need to be renamed; that's tracked via pendingFileRenames

                var processSuccess = ProcessPeptideHitJobFiles(
                    retrievalOptions,
                    localDirectoryPath,
                    prefixRequired,
                    filesToGet,
                    out var foundFiles,
                    out var pendingFileRenames);

                if (!processSuccess)
                {
                    return false;
                }

                var myEmslSuccess = mAnalysisResources.ProcessMyEMSLDownloadQueue();

                if (!myEmslSuccess)
                {
                    return false;
                }

                // Now perform any required file renames
                foreach (var fileToRename in pendingFileRenames)
                {
                    if (RenameDuplicatePHRPFile(localDirectoryPath, fileToRename, workDirInfo.FullName,
                                                "Job" + dataPkgJob.Job + "_", dataPkgJob.Job,
                                                out var newFilePath))
                    {
                        // Rename succeeded
                        foundFiles.Remove(Path.Combine(localDirectoryPath, fileToRename));
                        foundFiles.Add(newFilePath);
                    }
                    else
                    {
                        // Rename failed
                        return false;
                    }
                }

                if (retrievalOptions.RetrieveDTAFiles)
                {
                    var success = FindAndProcessPeakDataFile(
                        retrievalOptions,
                        candidateMzIdFiles,
                        cachedJobMetadata,
                        zipTools,
                        dataPkgJob,
                        workDirInfo,
                        localDirectoryPath,
                        foundFiles);

                    if (!success)
                        return false;
                }

                if (retrievalOptions.CreateJobPathFiles)
                {
                    var jobInfoFilePath = GetJobInfoFilePath(dataPkgJob.Job);

                    using var writer = new StreamWriter(new FileStream(jobInfoFilePath, FileMode.Append, FileAccess.Write, FileShare.Read));

                    foreach (var filePath in foundFiles)
                    {
                        var currentFileInfo = new FileInfo(filePath);

                        if (currentFileInfo.Name.EndsWith("_msgfplus.zip", StringComparison.OrdinalIgnoreCase))
                        {
                            // Convert the _msgfplus.zip file to a .mzid.gz file
                            if (currentFileInfo.Exists)
                            {
                                zipTools.UnzipFile(currentFileInfo.FullName, workDirInfo.FullName);
                                var unzippedFilePath = MostRecentUnzippedFile(zipTools);

                                zipTools.GZipFile(unzippedFilePath, true);
                                var gzipFilePath = zipTools.MostRecentZipFilePath;
                                var gzipFileSource = new FileInfo(gzipFilePath);

                                // Move the file into a subdirectory below the working directory
                                // This is necessary in case a dataset has multiple analysis jobs in the same data package
                                var gzipFilePathNew = MoveFileToJobSubdirectory(workDirInfo, dataPkgJob, gzipFileSource);

                                writer.WriteLine(gzipFilePathNew);
                                continue;
                            }
                        }

                        if (currentFileInfo.Exists &&
                            currentFileInfo.Directory?.FullName.Equals(workDirInfo.FullName, StringComparison.OrdinalIgnoreCase) == true)
                        {
                            // Move the file into a subdirectory below the working directory
                            // This is necessary in case a dataset has multiple analysis jobs in the same data package

                            // Furthermore, method RetrievePHRPFiles in the tool runner class of the PRIDE Converter plugin
                            // requires that files be copied from a separate location to the working directory
                            // It raises an error if the file is found to exist in the working directory

                            var newFilePath = MoveFileToJobSubdirectory(workDirInfo, dataPkgJob, currentFileInfo);

                            writer.WriteLine(newFilePath);
                            continue;
                        }

                        writer.WriteLine(filePath);
                    }
                }
                else
                {
                    var unzipSuccess = UnzipFiles(
                        zipTools, workDirInfo, prefixRequired, dataPkgJob,
                        foundFiles, zipFileCandidates, gzipFileCandidates,
                        zippedPepXmlFile);

                    if (!unzipSuccess)
                    {
                        return false;
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ProcessOnePeptideHitJob", ex);
                return false;
            }
        }

        /// <summary>
        /// Process files found for a single peptide hit job (as tracked by filesToGet)
        /// </summary>
        /// <param name="retrievalOptions">File retrieval options</param>
        /// <param name="localDirectoryPath">Local directory path</param>
        /// <param name="prefixRequired">If true, add the file to pendingFileRenames</param>
        /// <param name="filesToGet">Keys in this list are filenames; values are true if the file is required and false if not required</param>
        /// <param name="foundFiles">Output: list of found files</param>
        /// <param name="pendingFileRenames">Output: files to rename</param>
        private bool ProcessPeptideHitJobFiles(
            DataPackageRetrievalOptionsType retrievalOptions,
            string localDirectoryPath,
            bool prefixRequired,
            SortedList<string, bool> filesToGet,
            out List<string> foundFiles,
            out List<string> pendingFileRenames)
        {
            var sourceFilename = "??";
            var sourceDirectoryPath = string.Empty;

            foundFiles = new List<string>();
            pendingFileRenames = new List<string>();

            try
            {
                var mzidFileFound = false;

                foreach (var sourceFile in filesToGet)
                {
                    sourceFilename = sourceFile.Key;
                    var fileRequired = sourceFile.Value;

                    if (mzidFileFound && IsMzidFile(sourceFilename, out var splitFasta) && splitFasta)
                    {
                        // This is a split FASTA .mzid file
                        // Skip it since we already found Dataset_msgfplus.mzid.gz
                        // (assuming that Dataset_msgfplus.mzid.gz is listed earlier in filesToGet than Dataset_msgfplus_Part6.mzid.gz is listed)
                        continue;
                    }

                    // Typically only use FindDataFile() for the first file in filesToGet; we will assume the other files are in that directory
                    // However, if the file resides in MyEMSL, we need to call FindDataFile for every new file because FindDataFile will append the MyEMSL File ID for each file
                    if (string.IsNullOrEmpty(sourceDirectoryPath) || sourceDirectoryPath.StartsWith(AnalysisResources.MYEMSL_PATH_FLAG))
                    {
                        sourceDirectoryPath = mAnalysisResources.FileSearchTool.FindDataFile(sourceFilename);

                        if (string.IsNullOrEmpty(sourceDirectoryPath))
                        {
                            var alternateFileName = ReaderFactory.AutoSwitchToLegacyMSGFDBIfRequired(sourceFilename, "Dataset_msgfdb.txt");
                            sourceDirectoryPath = mAnalysisResources.FileSearchTool.FindDataFile(alternateFileName);
                        }
                    }

                    BaseLogger.LogLevels logMsgTypeIfNotFound;

                    if (!fileRequired)
                    {
                        // It's OK if this file doesn't exist, we'll just log a debug message
                        logMsgTypeIfNotFound = BaseLogger.LogLevels.DEBUG;
                    }
                    else
                    {
                        // This file must exist; log an error if it's not found
                        logMsgTypeIfNotFound = BaseLogger.LogLevels.ERROR;
                    }

                    if (retrievalOptions.CreateJobPathFiles && !sourceDirectoryPath.StartsWith(AnalysisResources.MYEMSL_PATH_FLAG))
                    {
                        var sourceFilePath = Path.Combine(sourceDirectoryPath, sourceFilename);
                        var alternateFileName = ReaderFactory.AutoSwitchToLegacyMSGFDBIfRequired(sourceFilePath, "Dataset_msgfdb.txt");

                        if (File.Exists(sourceFilePath))
                        {
                            foundFiles.Add(sourceFilePath);

                            if (IsMzidFile(sourceFilePath, out var splitFastaMzid) && !splitFastaMzid)
                            {
                                mzidFileFound = true;
                            }
                        }
                        else if (File.Exists(alternateFileName))
                        {
                            foundFiles.Add(alternateFileName);

                            if (IsMzidFile(alternateFileName, out var splitFastaMzid) && !splitFastaMzid)
                            {
                                mzidFileFound = true;
                            }
                        }
                        else
                        {
                            if (logMsgTypeIfNotFound != BaseLogger.LogLevels.DEBUG)
                            {
                                var warningMessage = "Required PHRP file not found: " + sourceFilename;

                                if (sourceFilename.EndsWith("_msgfplus.zip", StringComparison.OrdinalIgnoreCase) ||
                                    sourceFilename.EndsWith("_msgfplus.mzid.gz", StringComparison.OrdinalIgnoreCase))
                                {
                                    warningMessage += "; Confirm job used MS-GF+ and not MSGFDB";
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
                        var fileCopied = mAnalysisResources.CopyFileToWorkDir(sourceFilename, sourceDirectoryPath, localDirectoryPath, logMsgTypeIfNotFound);

                        if (!fileCopied)
                        {
                            var alternateFileName = ReaderFactory.AutoSwitchToLegacyMSGFDBIfRequired(sourceFilename, "Dataset_msgfdb.txt");
                            fileCopied = mAnalysisResources.CopyFileToWorkDir(alternateFileName, sourceDirectoryPath, localDirectoryPath, logMsgTypeIfNotFound);

                            if (fileCopied)
                            {
                                sourceFilename = alternateFileName;
                            }
                        }

                        if (!fileCopied)
                        {
                            if (logMsgTypeIfNotFound != BaseLogger.LogLevels.DEBUG)
                            {
                                OnErrorEvent("CopyFileToWorkDir returned false for " + sourceFilename + " using directory " + sourceDirectoryPath);
                                mAnalysisResources.RestoreCachedDataAndJobInfo();
                                return false;
                            }
                        }
                        else
                        {
                            OnStatusEvent("Copied " + sourceFilename + " from directory " + sourceDirectoryPath);
                            foundFiles.Add(Path.Combine(localDirectoryPath, sourceFilename));

                            if (prefixRequired)
                            {
                                pendingFileRenames.Add(sourceFilename);
                            }
                            else
                            {
                                mAnalysisResources.AddResultFileToSkip(sourceFilename);
                            }
                        }
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("ProcessPeptideHitJobFiles; Exception processing file " + sourceFilename + " in directory " + sourceDirectoryPath, ex);
                return false;
            }
        }

        private bool FindAndProcessPeakDataFile(
            DataPackageRetrievalOptionsType retrievalOptions,
            ICollection<string> candidateMzIdFiles,
            IDictionary<int, DataPackageJobMetadata> cachedJobMetadata,
            ZipFileTools zipTools,
            DataPackageJobInfo dataPkgJob,
            FileSystemInfo workDirInfo,
            string localDirectoryPath,
            ICollection<string> foundFiles)
        {
            // Find the _dta.txt or .mzML.gz file that was used for an MS-GF+ search

            var mzIDFileToInspect = string.Empty;

            // Need to examine the .mzid file to determine which file was used for the search
            foreach (var foundFile in foundFiles)
            {
                if (candidateMzIdFiles.Contains(Path.GetFileName(foundFile)))
                {
                    mzIDFileToInspect = foundFile;
                    break;
                }
            }

            var searchUsedMzML = false;

            if (!string.IsNullOrEmpty(mzIDFileToInspect))
            {
                if (cachedJobMetadata.TryGetValue(dataPkgJob.Job, out var jobMetadata))
                {
                    searchUsedMzML = jobMetadata.SearchUsedMzML;
                }
                else
                {
                    // Examine the .mzid file to determine whether a .mzML file was used
                    searchUsedMzML = MSGFPlusSearchUsedMzML(mzIDFileToInspect, zipTools);

                    var newMetadata = new DataPackageJobMetadata
                    {
                        SearchUsedMzML = searchUsedMzML
                    };

                    cachedJobMetadata.Add(dataPkgJob.Job, newMetadata);
                }
            }

            if (searchUsedMzML)
            {
                var stepToolFilter = string.Empty;

                if (dataPkgJob.Tool.StartsWith("msgfplus", StringComparison.OrdinalIgnoreCase))
                {
                    stepToolFilter = "MSGFPlus";
                }

                var mzMLFilePathRemote = FindMzMLForJob(dataPkgJob.Dataset, dataPkgJob.Job, stepToolFilter, workDirInfo);

                if (string.IsNullOrEmpty(mzMLFilePathRemote))
                    return true;

                if (retrievalOptions.CreateJobPathFiles && !mzMLFilePathRemote.StartsWith(AnalysisResources.MYEMSL_PATH_FLAG))
                {
                    if (!foundFiles.Contains(mzMLFilePathRemote))
                    {
                        foundFiles.Add(mzMLFilePathRemote);
                    }
                }
                else
                {
                    // Note for files in MyEMSL, this call will simply add the file to the download queue
                    // Use ProcessMyEMSLDownloadQueue() to retrieve the file
                    var sourceMzMLFile = new FileInfo(mzMLFilePathRemote);
                    var targetMzMLFile = new FileInfo(Path.Combine(localDirectoryPath, sourceMzMLFile.Name));

                    // Only copy the .mzML.gz file if it does not yet exist locally
                    if (targetMzMLFile.Exists)
                        return true;

                    const BaseLogger.LogLevels logMsgTypeIfNotFound = BaseLogger.LogLevels.ERROR;

                    var fileCopied = mAnalysisResources.CopyFileToWorkDir(sourceMzMLFile.Name, sourceMzMLFile.DirectoryName, localDirectoryPath, logMsgTypeIfNotFound);

                    if (fileCopied)
                    {
                        OnStatusEvent("Copied " + sourceMzMLFile.Name + " from directory " + sourceMzMLFile.DirectoryName);
                        foundFiles.Add(Path.Combine(localDirectoryPath, sourceMzMLFile.Name));
                    }
                }
            }
            else
            {
                // Find the CDTA file

                if (retrievalOptions.CreateJobPathFiles)
                {
                    var sourceConcatenatedDTAFilePath = mAnalysisResources.FileSearchTool.FindCDTAFile(out var errorMessage);

                    if (string.IsNullOrEmpty(sourceConcatenatedDTAFilePath))
                    {
                        OnErrorEvent(errorMessage);
                        return false;
                    }

                    foundFiles.Add(sourceConcatenatedDTAFilePath);
                }
                else
                {
                    if (!mAnalysisResources.FileSearchTool.RetrieveDtaFiles())
                    {
                        // Errors were reported in method call, so just return
                        return false;
                    }

                    // The retrieved file is probably named Dataset_dta.zip
                    // We'll add it to foundFiles, but the exact name is not critical
                    foundFiles.Add(Path.Combine(workDirInfo.FullName, dataPkgJob.Dataset + AnalysisResources.CDTA_ZIPPED_EXTENSION));
                }
            }

            return true;
        }

        private bool IsMzidFile(string fileNameOrPath, out bool splitFastaMzid)
        {
            splitFastaMzid = false;

            if (fileNameOrPath.EndsWith("_msgfplus.mzid.gz", StringComparison.OrdinalIgnoreCase) ||
                fileNameOrPath.EndsWith("_msgfplus.zip", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            var splitFastaMatcher = new Regex(@"_msgfplus_Part\d+\.mzid\.gz", RegexOptions.IgnoreCase);

            if (splitFastaMatcher.IsMatch(fileNameOrPath))
            {
                splitFastaMzid = true;
                return true;
            }

            return false;
        }

        private bool RenameDuplicatePHRPFile(
            string sourceDirectoryPath,
            string sourceFileName,
            string targetDirectoryPath,
            string prefixToAdd,
            int job,
            out string newFilePath)
        {
            try
            {
                var fileToRename = new FileInfo(Path.Combine(sourceDirectoryPath, sourceFileName));
                newFilePath = Path.Combine(targetDirectoryPath, prefixToAdd + fileToRename.Name);

                fileToRename.MoveTo(newFilePath);

                mAnalysisResources.AddResultFileToSkip(Path.GetFileName(newFilePath));
                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception renaming PHRP file " + sourceFileName + " for job " + job + " (data package has multiple jobs for the same dataset)", ex);
                newFilePath = string.Empty;
                return false;
            }
        }

        /// <summary>
        /// Retrieves the instrument files for the datasets defined for the data package associated with this aggregation job
        /// </summary>
        /// <param name="retrieveMsXmlFiles">
        /// Set to true to obtain mzML (or .mzXML) files for the datasets;
        /// will return false if a .mzML (or .mzXML) file cannot be found for any of the datasets
        /// </param>
        /// <param name="dataPackageDatasets">
        /// Output: Dataset info for the datasets associated with this data package; keys are Dataset ID, values are data package info
        /// </param>
        /// <param name="datasetRawFilePaths">
        /// <para>
        /// Output: Keys in this dictionary are dataset name, values are paths to the local file or directory for the dataset
        /// </para>
        /// <para>
        /// If skipDatasetsWithExistingMzML is true, datasets will not be added to datasetRawFilePaths if an existing .mzML file is found
        /// </para>
        /// </param>
        /// <param name="progressPercentAtStart">Percent complete value to use for computing incremental progress</param>
        /// <param name="progressPercentAtFinish">Percent complete value to use for computing incremental progress</param>
        /// <param name="skipDatasetsWithExistingMzML">
        /// When true, for each dataset in the data package, if an existing .mzML file can be found,
        /// do not retrieve the .raw file and do not add to datasetRawFilePaths
        /// </param>
        /// <returns>True if success, false if an error</returns>
        public bool RetrieveDataPackageDatasetFiles(
            bool retrieveMsXmlFiles,
            out Dictionary<int, DataPackageDatasetInfo> dataPackageDatasets,
            out Dictionary<string, string> datasetRawFilePaths,
            float progressPercentAtStart,
            float progressPercentAtFinish,
            bool skipDatasetsWithExistingMzML = false)
        {
            ErrorMessage = string.Empty;

            // Keys in this dictionary are dataset name, values are paths to the local file or directory for the dataset
            datasetRawFilePaths = new Dictionary<string, string>();

            var workingDir = mAnalysisResources.WorkDir;

            if (Global.OfflineMode)
            {
                ErrorMessage = "RetrieveDataPackageDatasetFiles does not support offline mode";
                throw new Exception(ErrorMessage);
            }

            try
            {
                var success = mDataPackageInfoLoader.LoadDataPackageDatasetInfo(out dataPackageDatasets, out var errorMessage, false);

                if (!success || dataPackageDatasets.Count == 0)
                {
                    if (string.IsNullOrWhiteSpace(errorMessage))
                    {
                        ErrorMessage = string.Format(
                            "Did not find any datasets associated with this job's data package (ID {0})",
                            mDataPackageInfoLoader.DataPackageID);
                    }
                    else
                    {
                        ErrorMessage = errorMessage;
                    }

                    OnErrorEvent(ErrorMessage);
                    return false;
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error calling LoadDataPackageDatasetInfo", ex);
                dataPackageDatasets = new Dictionary<int, DataPackageDatasetInfo>();
                return false;
            }

            try
            {
                var fileTools = new FileTools();
                RegisterEvents(fileTools);

                // Make sure the MyEMSL download queue is empty
                mAnalysisResources.ProcessMyEMSLDownloadQueue();

                // Cache the current dataset and job info
                mAnalysisResources.CacheCurrentDataAndJobInfo();

                var datasetsProcessed = 0;

                var msXMLOutputType = mAnalysisResources.JobParams.GetJobParameter("MSXMLOutputType", string.Empty);

                var msXmlType = msXMLOutputType.Equals("mzXML", StringComparison.OrdinalIgnoreCase)
                    ? AnalysisResources.MSXMLOutputTypeConstants.mzXML
                    : AnalysisResources.MSXMLOutputTypeConstants.mzML;

                foreach (var item in dataPackageDatasets)
                {
                    var dataPkgDataset = item.Value;

                    if (!mAnalysisResources.OverrideCurrentDatasetInfo(dataPkgDataset))
                    {
                        // Error message has already been logged
                        mAnalysisResources.RestoreCachedDataAndJobInfo();
                        return false;
                    }

                    if (!string.Equals(dataPkgDataset.Dataset, mAnalysisResources.DatasetName))
                    {
                        OnWarningEvent("Dataset name mismatch: {0} vs. {1}", dataPkgDataset.Dataset, mAnalysisResources.DatasetName);
                    }

                    if (skipDatasetsWithExistingMzML)
                    {
                        var fileFound = mAnalysisResources.FileSearchTool.FindMsXmlFileForJobInCache(
                            msXmlType, false, true, true,
                            out var errorMessage, out _, out var msXmlFile, out _);

                        if (fileFound)
                        {
                            OnStatusEvent(
                                "Skipping dataset {0} since an existing {1} file was found in {2} (by RetrieveCachedMSXMLFile)",
                                dataPkgDataset.Dataset, msXmlType, msXmlFile.DirectoryName);
                            continue;
                        }

                        if (!string.IsNullOrWhiteSpace(errorMessage))
                        {
                            if (retrieveMsXmlFiles)
                            {
                                // Example message:
                                //   Error looking for the mzML file for dataset 51912_Lewis_Bundle_A1: msXML file not found in the source directory(s): MSXML_Gen_1_297
                                OnWarningEvent(
                                    "Error looking for the {0} file for dataset {1}: {2}",
                                    msXmlType, dataPkgDataset.Dataset, errorMessage);
                            }
                            else
                            {
                                // Example message:
                                //   mzML file not found for dataset 51912_Lewis_Bundle_A1; will copy the instrument file locally
                                OnStatusEvent(
                                    "{0} file not found for dataset {1}; will copy the instrument file locally",
                                    msXmlType, dataPkgDataset.Dataset);
                            }
                        }
                    }

                    bool success;

                    // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
                    if (retrieveMsXmlFiles)
                    {
                        success = RetrieveDataPackageDatasetMsXMLFile(datasetRawFilePaths, dataPkgDataset, workingDir, msXmlType);
                    }
                    else
                    {
                        success = RetrieveDataPackageDatasetFile(datasetRawFilePaths, dataPkgDataset, workingDir, fileTools);
                    }

                    if (!success)
                        return false;

                    datasetsProcessed++;

                    var progress = AnalysisToolRunnerBase.ComputeIncrementalProgress(
                        progressPercentAtStart, progressPercentAtFinish, datasetsProcessed,
                        dataPackageDatasets.Count);

                    OnProgressUpdate("RetrieveDataPackageDatasetFiles (PeptideHit Jobs)", progress);
                }

                // Restore the dataset info for this aggregation job
                mAnalysisResources.RestoreCachedDataAndJobInfo();

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in RetrieveDataPackageDatasetFiles", ex);
                return false;
            }
        }

        private bool RetrieveDataPackageDatasetFile(
            IDictionary<string, string> datasetRawFilePaths,
            DataPackageDatasetInfo dataPkgDataset,
            string workingDir,
            FileTools fileTools)
        {
            var rawFilePath = mAnalysisResources.DirectorySearchTool.FindDatasetFileOrDirectory(out var isDirectory, false);

            if (string.IsNullOrEmpty(rawFilePath))
            {
                OnErrorEvent("FindDatasetFileOrDirectory could not find the dataset file for dataset " + dataPkgDataset.Dataset);
                return false;
            }

            string fileName;

            if (rawFilePath.Contains(DatasetInfoBase.MYEMSL_FILE_ID_TAG))
            {
                DatasetInfoBase.ExtractMyEMSLFileID(rawFilePath, out var newFilePath);
                fileName = Path.GetFileName(newFilePath);
            }
            else
            {
                fileName = Path.GetFileName(rawFilePath);
            }

            if (rawFilePath.StartsWith(MyEMSLUtilities.MYEMSL_PATH_FLAG))
            {
                mAnalysisResources.MyEMSLUtils.AddFileToDownloadQueue(rawFilePath);

                var myEmslSuccess = mAnalysisResources.ProcessMyEMSLDownloadQueue();

                if (!myEmslSuccess)
                {
                    return false;
                }

                foreach (var downloadedFile in mAnalysisResources.MyEMSLUtils.DownloadedFiles)
                {
                    var localFilePath = downloadedFile.Key;

                    if (Path.GetFileName(localFilePath).Equals(fileName, StringComparison.OrdinalIgnoreCase))
                    {
                        if (!File.Exists(localFilePath))
                        {
                            OnErrorEvent("Dataset file retrieved from MyEMSL not found in the working directory: " + localFilePath);
                            return false;
                        }

                        dataPkgDataset.IsDirectoryBased = false;
                        datasetRawFilePaths.Add(dataPkgDataset.Dataset, localFilePath);
                        return true;
                    }
                }

                OnErrorEvent("Dataset file could not be retrieved from MyEMSL (not found in DownloadedFiles): " + rawFilePath);
                return false;
            }

            if (isDirectory)
            {
                var localInstrumentDirectoryPath = Path.Combine(workingDir, fileName);
                fileTools.CopyDirectory(rawFilePath, localInstrumentDirectoryPath);

                dataPkgDataset.IsDirectoryBased = true;
                datasetRawFilePaths.Add(dataPkgDataset.Dataset, localInstrumentDirectoryPath);
            }
            else
            {
                var localInstrumentFilePath = Path.Combine(workingDir, fileName);
                var success = fileTools.CopyFileUsingLocks(rawFilePath, localInstrumentFilePath);

                if (!success)
                {
                    OnErrorEvent("Error copying dataset file " + rawFilePath);
                    return false;
                }

                dataPkgDataset.IsDirectoryBased = false;
                datasetRawFilePaths.Add(dataPkgDataset.Dataset, localInstrumentFilePath);
            }

            return true;
        }

        private bool RetrieveDataPackageDatasetMsXMLFile(
            IDictionary<string, string> datasetRawFilePaths,
            DataPackageDatasetInfo dataPkgDataset,
            string workingDir,
            AnalysisResources.MSXMLOutputTypeConstants msXmlType)
        {
            const bool unzipFile = true;

            var success = msXmlType == AnalysisResources.MSXMLOutputTypeConstants.mzXML
                ? mAnalysisResources.FileSearchTool.RetrieveCachedMzXMLFile(unzipFile, out var errorMessage, out _, out _)
                : mAnalysisResources.FileSearchTool.RetrieveCachedMzMLFile(unzipFile, out errorMessage, out _, out _);

            if (!success)
            {
                var methodName = msXmlType == AnalysisResources.MSXMLOutputTypeConstants.mzXML
                    ? "RetrieveCachedMzXMLFile"
                    : "RetrieveCachedMzMLFile";

                // RetrieveCachedMzMLFile could not find the mzML file for dataset
                OnErrorEvent("{0} could not find the {1} file for dataset {2}: {3}", methodName, msXmlType, dataPkgDataset.Dataset, errorMessage);
                return false;
            }

            var fileExtension = msXmlType == AnalysisResources.MSXMLOutputTypeConstants.mzXML
                ? AnalysisResources.DOT_MZXML_EXTENSION
                : AnalysisResources.DOT_MZML_EXTENSION;

            var localMsXMLFilePath = Path.Combine(workingDir, dataPkgDataset.Dataset + fileExtension);

            dataPkgDataset.IsDirectoryBased = false;
            datasetRawFilePaths.Add(dataPkgDataset.Dataset, localMsXMLFilePath);

            return true;
        }

        /// <summary>
        /// Retrieves the PHRP files for the PeptideHit jobs defined for the data package associated with this aggregation job
        /// Also creates a batch file that can be manually run to retrieve the instrument data files
        /// </summary>
        /// <param name="retrievalOptions">File retrieval options</param>
        /// <param name="dataPackagePeptideHitJobs">Output: Job info for the peptide_hit jobs associated with this data package</param>
        /// <param name="progressPercentAtStart">Percent complete value to use for computing incremental progress</param>
        /// <param name="progressPercentAtFinish">Percent complete value to use for computing incremental progress</param>
        /// <returns>True if success, false if an error</returns>
        public bool RetrieveDataPackagePeptideHitJobPHRPFiles(
            DataPackageRetrievalOptionsType retrievalOptions,
            out List<DataPackageJobInfo> dataPackagePeptideHitJobs,
            float progressPercentAtStart,
            float progressPercentAtFinish)
        {
            ErrorMessage = string.Empty;

            // Keys in this dictionary are DatasetID, values are a command of the form "Copy \\Server\Share\Directory\Dataset.raw Dataset.raw"
            // Note that we're explicitly defining the target filename to make sure the case of the letters matches the dataset name's case
            var rawFileRetrievalCommands = new Dictionary<int, string>();

            // Keys in this dictionary are dataset name, values are the full path to the instrument data file for the dataset
            // This information is stored in packed job parameter JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS
            var datasetRawFilePaths = new Dictionary<string, string>();

            // This list tracks the info for the jobs associated with this aggregation job's data package
            dataPackagePeptideHitJobs = new List<DataPackageJobInfo>();

            // This dictionary tracks the datasets associated with this aggregation job's data package
            // Keys are DatasetID, values are dataset info
            Dictionary<int, DataPackageDatasetInfo> dataPackageDatasets;

            var debugLevel = mAnalysisResources.DebugLevel;
            var workingDir = mAnalysisResources.WorkDir;

            if (Global.OfflineMode)
            {
                ErrorMessage = "RetrieveDataPackagePeptideHitJobPHRPFiles does not support offline mode";
                throw new Exception(ErrorMessage);
            }

            try
            {
                var success = mDataPackageInfoLoader.LoadDataPackageDatasetInfo(out dataPackageDatasets, out var errorMessage, false);

                if (!success || dataPackageDatasets.Count == 0)
                {
                    if (string.IsNullOrWhiteSpace(errorMessage))
                    {
                        ErrorMessage = string.Format(
                            "Did not find any datasets associated with this job's data package (ID {0})",
                            mDataPackageInfoLoader.DataPackageID);
                    }
                    else
                    {
                        ErrorMessage = errorMessage;
                    }

                    OnErrorEvent(ErrorMessage);
                    return false;
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error calling LoadDataPackageDatasetInfo", ex);
                return false;
            }

            // The keys in this dictionary are data package job info entries
            // The values are KeyValuePairs of path to the .mzXML file and path to the .hashcheck file (if any)
            // The KeyValuePair will have empty strings if the .Raw file needs to be retrieved
            // This information is used by RetrieveDataPackageMzXMLFiles when copying files locally
            var instrumentDataToRetrieve = new Dictionary<DataPackageJobInfo, KeyValuePair<string, string>>();

            // This list tracks analysis jobs that are not PeptideHit jobs
            List<DataPackageJobInfo> additionalJobs;

            try
            {
                dataPackagePeptideHitJobs = mDataPackageInfoLoader.RetrieveDataPackagePeptideHitJobInfo(out additionalJobs);

                if (dataPackagePeptideHitJobs.Count == 0)
                {
                    // Did not find any peptide hit jobs associated with this job's data package
                    // This is atypical, but is allowed
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error calling RetrieveDataPackagePeptideHitJobInfo", ex);
                return false;
            }

            try
            {
                var workDirInfo = new DirectoryInfo(workingDir);

                var zipTools = new ZipFileTools(debugLevel, workDirInfo.FullName);
                RegisterEvents(zipTools);

                // Make sure the MyEMSL download queue is empty
                mAnalysisResources.ProcessMyEMSLDownloadQueue();

                // Cache the current dataset and job info
                mAnalysisResources.CacheCurrentDataAndJobInfo();

                // Look for a DataPkgJobMetadata.txt file
                var dataPkgJobMetadataFile = new FileInfo(Path.Combine(retrievalOptions.RemoteTransferDirectoryPath, DATA_PKG_JOB_METADATA_FILE));

                Dictionary<int, DataPackageJobMetadata> cachedJobMetadata;
                int cachedMetadataCountAtStart;

                if (!string.IsNullOrWhiteSpace(retrievalOptions.RemoteTransferDirectoryPath) && dataPkgJobMetadataFile.Exists)
                {
                    cachedJobMetadata = LoadCachedDataPkgJobMetadata(dataPkgJobMetadataFile);
                    cachedMetadataCountAtStart = cachedJobMetadata.Count;
                }
                else
                {
                    cachedJobMetadata = new Dictionary<int, DataPackageJobMetadata>();
                    cachedMetadataCountAtStart = 0;
                }

                var jobsProcessed = 0;

                foreach (var dataPkgJob in dataPackagePeptideHitJobs)
                {
                    if (!mAnalysisResources.OverrideCurrentDatasetAndJobInfo(dataPkgJob))
                    {
                        // Error message has already been logged
                        mAnalysisResources.RestoreCachedDataAndJobInfo();
                        return false;
                    }

                    if (!string.Equals(dataPkgJob.Dataset, mAnalysisResources.DatasetName))
                    {
                        OnWarningEvent("Dataset name mismatch: {0} vs. {1}", dataPkgJob.Dataset, mAnalysisResources.DatasetName);
                    }

                    if (dataPkgJob.PeptideHitResultType == PeptideHitResultTypes.Unknown)
                    {
                        LogTools.LogWarning("PeptideHit ResultType not recognized for job {0}: {1}", dataPkgJob.Job, dataPkgJob.ResultType);
                    }
                    else
                    {
                        var success = ProcessOnePeptideHitJob(retrievalOptions, cachedJobMetadata, zipTools, workDirInfo, dataPkgJob);

                        if (!success)
                        {
                            mAnalysisResources.RestoreCachedDataAndJobInfo();
                            return false;
                        }
                    }

                    // Find the instrument data file or directory if a new dataset
                    if (!rawFileRetrievalCommands.ContainsKey(dataPkgJob.DatasetID))
                    {
                        if (!RetrieveDataPackageInstrumentFile(dataPkgJob, retrievalOptions, rawFileRetrievalCommands, instrumentDataToRetrieve, datasetRawFilePaths))
                        {
                            mAnalysisResources.RestoreCachedDataAndJobInfo();
                            return false;
                        }
                    }

                    jobsProcessed++;
                    var progress = AnalysisToolRunnerBase.ComputeIncrementalProgress(
                        progressPercentAtStart, progressPercentAtFinish, jobsProcessed,
                        dataPackagePeptideHitJobs.Count + additionalJobs.Count);

                    OnProgressUpdate("RetrieveDataPackagePeptideHitJobPHRPFiles (PeptideHit Jobs)", progress);
                }

                // Now process the additional jobs to retrieve the instrument data for each one
                foreach (var dataPkgJob in additionalJobs)
                {
                    // Find the instrument data file or directory if a new dataset
                    if (!rawFileRetrievalCommands.ContainsKey(dataPkgJob.DatasetID))
                    {
                        if (!RetrieveDataPackageInstrumentFile(dataPkgJob, retrievalOptions, rawFileRetrievalCommands, instrumentDataToRetrieve, datasetRawFilePaths))
                        {
                            mAnalysisResources.RestoreCachedDataAndJobInfo();
                            return false;
                        }
                    }

                    jobsProcessed++;
                    var progress = AnalysisToolRunnerBase.ComputeIncrementalProgress(
                        progressPercentAtStart, progressPercentAtFinish, jobsProcessed,
                        dataPackagePeptideHitJobs.Count + additionalJobs.Count);

                    OnProgressUpdate("RetrieveDataPackagePeptideHitJobPHRPFiles (Additional Jobs)", progress);
                }

                // Look for any datasets that are associated with this data package yet have no jobs
                foreach (var datasetItem in dataPackageDatasets)
                {
                    if (rawFileRetrievalCommands.ContainsKey(datasetItem.Key))
                        continue;

                    var dataPkgJob = AnalysisResources.GetPseudoDataPackageJobInfo(datasetItem.Value);
                    dataPkgJob.ResultsFolderName = "Undefined_Directory";

                    if (!mAnalysisResources.OverrideCurrentDatasetAndJobInfo(dataPkgJob))
                    {
                        // Error message has already been logged
                        mAnalysisResources.RestoreCachedDataAndJobInfo();
                        return false;
                    }

                    if (!RetrieveDataPackageInstrumentFile(dataPkgJob, retrievalOptions, rawFileRetrievalCommands, instrumentDataToRetrieve, datasetRawFilePaths))
                    {
                        mAnalysisResources.RestoreCachedDataAndJobInfo();
                        return false;
                    }
                }

                if (cachedJobMetadata.Count > 0 && cachedJobMetadata.Count != cachedMetadataCountAtStart)
                {
                    SaveCachedDataPkgJobMetadata(dataPkgJobMetadataFile, cachedJobMetadata);
                }

                // Restore the dataset and job info for this aggregation job
                mAnalysisResources.RestoreCachedDataAndJobInfo();

                if (rawFileRetrievalCommands.Count == 0)
                {
                    ErrorMessage = string.Format(
                        "Did not find any datasets associated with this job's data package (ID {0})",
                        mDataPackageInfoLoader.DataPackageID);

                    OnErrorEvent(ErrorMessage);
                    return false;
                }

                if (rawFileRetrievalCommands.Count > 0)
                {
                    // Create a batch file with commands for retrieve the dataset files
                    var batchFilePath = Path.Combine(workDirInfo.FullName, "RetrieveInstrumentData.bat");

                    using (var writer = new StreamWriter(new FileStream(batchFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    {
                        foreach (var item in rawFileRetrievalCommands.Values)
                        {
                            writer.WriteLine(item);
                        }
                    }

                    // Store the dataset paths in a Packed Job Parameter
                    mAnalysisResources.StorePackedJobParameterDictionary(datasetRawFilePaths, AnalysisResources.JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS);
                }

                if (retrievalOptions.RetrieveMzXMLFile)
                {
                    // All PHRP data files have been successfully retrieved; now retrieve the mzXML files or the .Raw files
                    // If retrievalOptions.CreateJobPathFiles is true, create StoragePathInfo files
                    return RetrieveDataPackageMzXMLFiles(instrumentDataToRetrieve, retrievalOptions);
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in RetrieveDataPackagePeptideHitJobPHRPFiles", ex);
                return false;
            }
        }

        /// <summary>
        /// Look for the .mzXML, .mzML, or .raw file
        /// Create the copy command or call to MyEMSLDownloader.exe to retrieve the file
        /// Does not actually copy the file locally, just updates rawFileRetrievalCommands and datasetRawFilePaths
        /// </summary>
        /// <param name="dataPkgJob">Data package job info</param>
        /// <param name="retrievalOptions">File retrieval options</param>
        /// <param name="rawFileRetrievalCommands">Commands to copy .raw files to the local computer (to be placed in batch file RetrieveInstrumentData.bat)</param>
        /// <param name="instrumentDataToRetrieve">
        /// Instrument files that need to be copied locally
        /// Keys are dataPkgJob, values are a KeyValuePair of the existing .mzML file (or .mzXML file) and its hash check file, or empty strings if not found
        /// </param>
        /// <param name="datasetRawFilePaths">Mapping of dataset name to the remote location of the .raw file</param>
        private bool RetrieveDataPackageInstrumentFile(
            DataPackageJobInfo dataPkgJob,
            DataPackageRetrievalOptionsType retrievalOptions,
            IDictionary<int, string> rawFileRetrievalCommands,
            IDictionary<DataPackageJobInfo, KeyValuePair<string, string>> instrumentDataToRetrieve,
            IDictionary<string, string> datasetRawFilePaths)
        {
            if (retrievalOptions.RetrieveMzXMLFile)
            {
                // See if a .mzML or .mzXML file already exists for this dataset

                // First look for a .mzML file

                var mzMLFileFound = mAnalysisResources.FileSearchTool.FindMsXmlFileForJobInCache(
                    AnalysisResources.MSXMLOutputTypeConstants.mzML, false, true, true,
                    out _, out _, out var mzMLFile, out var mzMLHashcheckFilePath);

                bool msXmlFileFound;
                FileInfo msXmlFileInfo;
                string hashcheckFilePath;

                if (!mzMLFileFound)
                {
                    msXmlFileFound = mAnalysisResources.FileSearchTool.FindMsXmlFileForJobInCache(
                        AnalysisResources.MSXMLOutputTypeConstants.mzXML, false, true, true,
                        out _, out _, out msXmlFileInfo, out hashcheckFilePath);
                }
                else
                {
                    msXmlFileFound = true;
                    msXmlFileInfo = mzMLFile;
                    hashcheckFilePath = mzMLHashcheckFilePath;
                }

                if (!msXmlFileFound)
                {
                    // mzML or mzXML file not found
                    if (dataPkgJob.RawDataType == AnalysisResources.RAW_DATA_TYPE_DOT_RAW_FILES)
                    {
                        // Will need to retrieve the .Raw file for this dataset
                        instrumentDataToRetrieve.Add(dataPkgJob, new KeyValuePair<string, string>(string.Empty, string.Empty));
                    }
                    else
                    {
                        OnErrorEvent("mzML/mzXML file not found for dataset {0} (job {1}) and the dataset is not a .Raw file, " +
                                     "so we cannot auto-create the missing mzML file", dataPkgJob.Dataset, dataPkgJob.Job);

                        return false;
                    }
                }
                else
                {
                    instrumentDataToRetrieve.Add(dataPkgJob, new KeyValuePair<string, string>(msXmlFileInfo.FullName, hashcheckFilePath));
                }
            }

            var rawFilePath = mAnalysisResources.DirectorySearchTool.FindDatasetFileOrDirectory(
                out var isDirectory, retrievalOptions.AssumeInstrumentDataUnpurged);

            if (string.IsNullOrEmpty(rawFilePath))
                return true;

            string copyCommandIfNotExist;

            if (isDirectory)
            {
                var directoryName = Path.GetFileName(rawFilePath);
                string copyCommand;

                if (rawFilePath.StartsWith(MyEMSLUtilities.MYEMSL_PATH_FLAG))
                {
                    // The path starts with \\MyEMSL
                    copyCommand = string.Format(
                        @"C:\DMS_Programs\MyEMSLDownloader\MyEMSLDownloader.exe /Dataset:{0} /Files:{1}",
                        dataPkgJob.Dataset, directoryName);
                }
                else
                {
                    copyCommand = "copy " + rawFilePath + @" .\" + directoryName + " /S /I";
                }

                copyCommandIfNotExist = "if not exist " + directoryName + " " + copyCommand;
            }
            else
            {
                // Make sure the case of the filename matches the case of the dataset name
                // Also, make sure the extension is lowercase
                var fileName = dataPkgJob.Dataset + Path.GetExtension(rawFilePath).ToLower();
                string copyCommand;

                if (rawFilePath.StartsWith(MyEMSLUtilities.MYEMSL_PATH_FLAG))
                {
                    // The path starts with \\MyEMSL
                    copyCommand = string.Format(
                        @"C:\DMS_Programs\MyEMSLDownloader\MyEMSLDownloader.exe /Dataset:{0} /Files:{1}",
                        dataPkgJob.Dataset, fileName);
                }
                else
                {
                    copyCommand = "copy " + rawFilePath + " " + fileName;
                }

                copyCommandIfNotExist = "if not exist " + fileName + " " + copyCommand;
            }

            rawFileRetrievalCommands.Add(dataPkgJob.DatasetID, copyCommandIfNotExist);
            datasetRawFilePaths.Add(dataPkgJob.Dataset, rawFilePath);

            return true;
        }

        /// <summary>
        /// Retrieve the .mzXML files for the jobs in instrumentDataToRetrieve
        /// </summary>
        /// <remarks>If retrievalOptions.CreateJobPathFiles is true, will create StoragePathInfo files for the .mzXML or .Raw files</remarks>
        /// <param name="instrumentDataToRetrieve">
        /// The keys in this dictionary are JobInfo entries
        /// The values in this dictionary are KeyValuePairs of path to the .mzXML file and path to the .hashcheck file (if any).
        /// The KeyValuePair will have empty strings if the .Raw file needs to be retrieved
        /// </param>
        /// <param name="retrievalOptions">File retrieval options</param>
        /// <returns>True if success, false if an error</returns>
        public bool RetrieveDataPackageMzXMLFiles(
            Dictionary<DataPackageJobInfo, KeyValuePair<string, string>> instrumentDataToRetrieve,
            DataPackageRetrievalOptionsType retrievalOptions)
        {
            bool success;

            var currentJob = 0;

            try
            {
                // Make sure we don't move the .Raw, .mzXML, .mzML or .gz files into the results directory
                mAnalysisResources.AddResultFileExtensionToSkip(AnalysisResources.DOT_RAW_EXTENSION);
                mAnalysisResources.AddResultFileExtensionToSkip(AnalysisResources.DOT_MZXML_EXTENSION);
                mAnalysisResources.AddResultFileExtensionToSkip(AnalysisResources.DOT_MZML_EXTENSION);
                mAnalysisResources.AddResultFileExtensionToSkip(AnalysisResources.DOT_GZ_EXTENSION);

                var createStoragePathInfoOnly = retrievalOptions.CreateJobPathFiles;

                var datasetNamesProcessed = new SortedSet<string>();

                mAnalysisResources.CacheCurrentDataAndJobInfo();

                var lastProgressUpdate = DateTime.UtcNow;
                var datasetsProcessed = 0;
                var datasetsToProcess = instrumentDataToRetrieve.Count;

                // Retrieve the instrument data
                // Note that RetrieveMZXmlFileUsingSourceFile will add MyEMSL files to the download queue

                foreach (var kvItem in instrumentDataToRetrieve)
                {
                    // The key in kvMzXMLFileInfo is the path to the .mzXML or .mzML file
                    // The value in kvMzXMLFileInfo is the path to the .hashcheck file
                    var kvMzXMLFileInfo = kvItem.Value;
                    var mzXMLFilePath = kvMzXMLFileInfo.Key;
                    var hashcheckFilePath = kvMzXMLFileInfo.Value;

                    currentJob = kvItem.Key.Job;

                    if (!datasetNamesProcessed.Contains(kvItem.Key.Dataset))
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
                            success = mAnalysisResources.FileSearchTool.RetrieveMZXmlFileUsingSourceFile(createStoragePathInfoOnly, mzXMLFilePath, hashcheckFilePath);
                        }

                        if (success)
                        {
                            // .mzXML or .mzML file found and copied locally
                            string msXmlFileExtension;

                            if (mzXMLFilePath.EndsWith(AnalysisResources.DOT_GZ_EXTENSION, StringComparison.OrdinalIgnoreCase))
                            {
                                msXmlFileExtension = Path.GetExtension(mzXMLFilePath.Substring(0, mzXMLFilePath.Length - AnalysisResources.DOT_GZ_EXTENSION.Length));
                            }
                            else
                            {
                                msXmlFileExtension = Path.GetExtension(mzXMLFilePath);
                            }

                            if (retrievalOptions.CreateJobPathFiles)
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

                            if (!mAnalysisResources.FileSearchTool.RetrieveSpectra(kvItem.Key.RawDataType, createStoragePathInfoOnly, maxAttempts: 1))
                            {
                                OnErrorEvent("Error occurred retrieving instrument data file for job " + currentJob);
                                return false;
                            }
                        }

                        datasetNamesProcessed.Add(kvItem.Key.Dataset);
                    }

                    datasetsProcessed++;

                    // Compute a % complete value between 0 and 2%
                    var percentComplete = datasetsProcessed / (float)datasetsToProcess * 2;
                    OnProgressUpdate("Retrieving MzXML files", percentComplete);

                    if (DateTime.UtcNow.Subtract(lastProgressUpdate).TotalSeconds >= 30)
                    {
                        lastProgressUpdate = DateTime.UtcNow;

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

        private Dictionary<int, DataPackageJobMetadata> LoadCachedDataPkgJobMetadata(
            FileSystemInfo dataPkgJobMetadataFile)
        {
            var cachedJobMetadata = new Dictionary<int, DataPackageJobMetadata>();

            try
            {
                if (!dataPkgJobMetadataFile.Exists)
                    return cachedJobMetadata;

                using var reader = new StreamReader(new FileStream(dataPkgJobMetadataFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                var headersParsed = false;
                var jobColIndex = 0;
                var mzMlUsedColIndex = 0;

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    var dataList = dataLine.Split('\t');

                    if (!headersParsed)
                    {
                        var requiredColumns = new List<string> { "Job", "SearchUsedMzML" };

                        var columnMap = Global.ParseHeaderLine(dataLine, requiredColumns);

                        foreach (var column in requiredColumns)
                        {
                            if (columnMap[column] >= 0)
                                continue;

                            OnWarningEvent("{0} column not found in {1}", column, dataPkgJobMetadataFile.FullName);
                            return cachedJobMetadata;
                        }

                        jobColIndex = columnMap["Job"];
                        mzMlUsedColIndex = columnMap["SearchUsedMzML"];

                        headersParsed = true;

                        continue;
                    }

                    if (!Global.TryGetValueInt(dataList, jobColIndex, out var job))
                        continue;

                    if (!Global.TryGetValue(dataList, mzMlUsedColIndex, out var searchUsedMzML))
                        continue;

                    var jobMetadata = new DataPackageJobMetadata
                    {
                        SearchUsedMzML = bool.Parse(searchUsedMzML)
                    };

                    cachedJobMetadata.Add(job, jobMetadata);
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in LoadCachedDataPkgJobMetadata", ex);
            }

            return cachedJobMetadata;
        }

        private void SaveCachedDataPkgJobMetadata(
            FileInfo dataPkgJobMetadataFile,
            Dictionary<int, DataPackageJobMetadata> cachedJobMetadata)
        {
            try
            {
                Global.CreateDirectoryIfMissing(dataPkgJobMetadataFile.DirectoryName);

                using var writer = new StreamWriter(new FileStream(dataPkgJobMetadataFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read));

                // Write the header line
                writer.WriteLine(string.Join("\t",
                    new List<string> { "Job", "SearchUsedMzML" }));

                foreach (var item in cachedJobMetadata)
                {
                    writer.WriteLine(string.Join("\t",
                        new List<string> { item.Key.ToString(), item.Value.SearchUsedMzML.ToString() }));
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in SaveCachedDataPkgJobMetadata", ex);
            }
        }

        /// <summary>
        /// Unzip the PepXML file (_pepXML.zip) if it was retrieved
        /// If the .mzid file is named Dataset_msgfplus.zip, unzip it, then compress it so that it's named Dataset.mzid.gz
        /// </summary>
        /// <param name="zipTools">Zip tools instance</param>
        /// <param name="workDirInfo">Working directory</param>
        /// <param name="prefixRequired">If true, add the job number to the start of the file name</param>
        /// <param name="dataPkgJob">Data package job</param>
        /// <param name="foundFiles">List of found files</param>
        /// <param name="zipFileCandidates">Candidate .mzid.zip files</param>
        /// <param name="gzipFileCandidates">Candidate .mzid.gz files</param>
        /// <param name="zippedPepXmlFile">Zipped .pepXML file</param>
        private bool UnzipFiles(
            ZipFileTools zipTools,
            FileSystemInfo workDirInfo,
            bool prefixRequired,
            DataPackageJobInfo dataPkgJob,
            ICollection<string> foundFiles,
            ICollection<string> zipFileCandidates,
            ICollection<string> gzipFileCandidates,
            string zippedPepXmlFile)
        {
            if (zipFileCandidates.Count > 0 || gzipFileCandidates.Count > 0)
            {
                var matchedFilePath = string.Empty;

                foreach (var gzipCandidate in gzipFileCandidates)
                {
                    var gzippedMzidFile = new FileInfo(Path.Combine(workDirInfo.FullName, gzipCandidate));

                    if (gzippedMzidFile.Exists)
                    {
                        matchedFilePath = gzippedMzidFile.FullName;
                        break;
                    }
                }

                if (string.IsNullOrEmpty(matchedFilePath))
                {
                    foreach (var zipCandidate in zipFileCandidates)
                    {
                        var fileToUnzip = new FileInfo(Path.Combine(workDirInfo.FullName, zipCandidate));

                        if (fileToUnzip.Exists)
                        {
                            zipTools.UnzipFile(fileToUnzip.FullName);
                            var unzippedFilePath = MostRecentUnzippedFile(zipTools);

                            zipTools.GZipFile(unzippedFilePath, true);
                            matchedFilePath = zipTools.MostRecentZipFilePath;
                            break;
                        }
                    }
                }

                if (string.IsNullOrEmpty(matchedFilePath))
                {
                    OnErrorEvent("Could not find either the _msgfplus.zip file or the _msgfplus.mzid.gz file for dataset");
                    mAnalysisResources.RestoreCachedDataAndJobInfo();
                    return false;
                }

                if (prefixRequired)
                {
                    var sourceFileName = dataPkgJob.Dataset + "_msgfplus.mzid.gz";
                    var prefixToAdd = "Job" + dataPkgJob.Job + "_";

                    var success = RenameDuplicatePHRPFile(
                        workDirInfo.FullName, sourceFileName,
                        workDirInfo.FullName, prefixToAdd,
                        dataPkgJob.Job,
                        out matchedFilePath);

                    if (!success)
                    {
                        mAnalysisResources.RestoreCachedDataAndJobInfo();
                        return false;
                    }
                }

                foundFiles.Add(matchedFilePath);
            }

            if (!string.IsNullOrWhiteSpace(zippedPepXmlFile))
            {
                // Unzip _pepXML.zip if it exists
                var fileToUnzip = new FileInfo(Path.Combine(workDirInfo.FullName, zippedPepXmlFile));

                if (fileToUnzip.Exists)
                {
                    zipTools.UnzipFile(fileToUnzip.FullName);
                    foundFiles.Add(MostRecentUnzippedFile(zipTools));
                }
            }

            return true;
        }

        private void ProcedureExecutor_DBErrorEvent(string message, Exception ex)
        {
            if (message.IndexOf("permission was denied", StringComparison.OrdinalIgnoreCase) >= 0 ||
                message.IndexOf("permission denied", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                try
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogDb, BaseLogger.LogLevels.ERROR, message);
                }
                catch (Exception ex2)
                {
                    Global.ErrorWritingToLog(message, ex2);
                }
            }

            OnErrorEvent(message);
        }
    }
}
