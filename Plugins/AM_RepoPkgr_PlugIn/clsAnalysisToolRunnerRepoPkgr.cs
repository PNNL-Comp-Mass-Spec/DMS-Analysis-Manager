using System;
using System.Collections.Generic;
using System.IO;
using System.Data.SqlClient;
using System.Text.RegularExpressions;
using AnalysisManager_RepoPkgr_PlugIn;
using AnalysisManagerBase;
using AnalysisManagerMsXmlGenPlugIn;

namespace AnalysisManager_RepoPkgr_Plugin
{
    /// <summary>
    /// Class for running the RepoPkgr
    /// </summary>
    public class clsAnalysisToolRunnerRepoPkgr : clsAnalysisToolRunnerBase
    {
        #region Constants

        protected const int PROGRESS_PCT_FASTA_FILES_COPIED = 10;
        protected const int PROGRESS_PCT_MSGF_PLUS_RESULTS_COPIED = 25;
        protected const int PROGRESS_PCT_SEQUEST_RESULTS_COPIED = 40;
        protected const int PROGRESS_PCT_MZID_RESULTS_COPIED = 50;
        protected const int PROGRESS_PCT_INSTRUMENT_DATA_COPIED = 95;

        public const string WARNING_INSTRUMENT_DATA_MISSING = "WarningInstrumentDataMissing";
        #endregion

        #region Fields

        private bool _bIncludeInstrumentData;
        private bool _bIncludeSequestResults;
        private bool _bIncludeMzXMLFiles;
        private bool _bIncludeMSGFPlusResults;
        private bool _bIncludeMZidFiles;

        private string _outputResultsFolderPath;
        private MageRepoPkgrPipelines _mgr;

        private string _MSXmlGeneratorAppPath;

        #endregion

        #region Main Logic

        /// <summary>
        /// Primary entry point for running this tool
        /// </summary>
        /// <returns>CloseOutType enum representing completion status</returns>
        public override CloseOutType RunTool()
        {
            try
            {
                // Do the base class stuff
                var result = base.RunTool();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                // Store the RepoPkgr version info in the database
                if (!StoreToolVersionInfo())
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    m_message = "Error determining RepoPkgr version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var instrumentDataWarning = m_jobParams.GetJobParameter(WARNING_INSTRUMENT_DATA_MISSING, string.Empty);
                if (!string.IsNullOrEmpty(instrumentDataWarning))
                    m_EvalMessage = instrumentDataWarning;

                result = BuildRepoCache();
                return result;
            }
            catch (Exception ex)
            {
                m_message = "Error in RepoPkgr Plugin->RunTool";
                LogError(m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Find (or generate) necessary files and copy them to repository cache folder for upload
        /// </summary>
        /// <returns></returns>
        private CloseOutType BuildRepoCache()
        {
            SetOptions();
            SetOutputFolderPath();
            SetMagePipelineManager(_outputResultsFolderPath);

            // do operations for repository specified in job parameters
            var targetRepository = m_jobParams.GetJobParameter("Repository", "");
            var success = false;
            switch (targetRepository)
            {
                case "PeptideAtlas":
                    success = DoPeptideAtlasOperation();
                    break;
                    // FUTURE: code for other repositories to go here someday
            }

            return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
        }

        private void CopyFastaFiles()
        {
            var localOrgDBFolder = m_mgrParams.GetParam("orgdbdir");
            var targetFolderPath = Path.Combine(_outputResultsFolderPath, "Organism_Database");

            var lstGeneratedOrgDBNames = ExtractPackedJobParameterList(clsAnalysisResourcesRepoPkgr.FASTA_FILES_FOR_DATA_PACKAGE);

            foreach (var orgDbName in lstGeneratedOrgDBNames)
            {
                var sourceFilePath = Path.Combine(localOrgDBFolder, orgDbName);

                // Rename the target file if it was created using Protein collections (and used 3 or fewer protein collections)
                var targetFileName = UpdateOrgDBNameIfRequired(orgDbName);

                var destFilePath = Path.Combine(targetFolderPath, targetFileName);
                m_FileTools.CopyFile(sourceFilePath, destFilePath, true);
            }
        }

        /// <summary>
        /// Gather files for submission to PeptideAtlas repository
        /// and copy them to repo cache
        /// </summary>
        private bool DoPeptideAtlasOperation()
        {
            // Copy *.fasta files from organism db to appropriate cache subfolder
            // Files to copy are stored in job parameters named "Job123456_GeneratedFasta"
            CopyFastaFiles();
            m_progress = PROGRESS_PCT_FASTA_FILES_COPIED;
            m_StatusTools.UpdateAndWrite(m_progress);

            var dataPkgJobCountMatch = 0;

            if (_bIncludeMSGFPlusResults)
            {
                // Find any MSGFPlus jobs in data package and copy their first hits files to appropriate cache subfolder
                // Note that prior to November 2016 the filenames had _msgfdb_fht; they now have _msgfplus_fht
                _mgr.GetItemsToRepoPkg(
                    "DataPkgJobsQueryTemplate",
                    "MSGFPlus",
                    "*_msgfplus_fht.txt;*_msgfplus_fht_MSGF.txt;*_msgfdb_fht.txt;*_msgfdb_fht_MSGF.txt",
                    "MSGFPlus_Results", "Job");

                dataPkgJobCountMatch = _mgr.DataPackageItems.Rows.Count;
                var dataPkgFileCountMatch = _mgr.AssociatedFiles.Rows.Count;

                if (dataPkgJobCountMatch == 0)
                {
                    LogWarning("Did not find any MSGF+ jobs in data package " + _mgr.DataPkgId + "; auto-setting _bIncludeSequestResults to True");
                    _bIncludeSequestResults = true;
                }
                else
                {
                    if (dataPkgFileCountMatch == 0)
                        LogWarning("Found " + dataPkgJobCountMatch + " MSGF+ jobs in data package " + _mgr.DataPkgId + " but did not find any candidate files to copy");
                    else
                        LogMessage("Copied " + dataPkgFileCountMatch + " files for " + dataPkgJobCountMatch + " MSGF+ jobs in data package " + _mgr.DataPkgId);
                }
            }
            m_progress = PROGRESS_PCT_MSGF_PLUS_RESULTS_COPIED;
            m_StatusTools.UpdateAndWrite(m_progress);

            if (_bIncludeSequestResults)
            {
                // find any sequest jobs in data package and copy their first hits files to appropriate cache subfolder
                _mgr.GetItemsToRepoPkg("DataPkgJobsQueryTemplate", "SEQUEST", "*_fht.txt", "SEQUEST_Results", "Job");
                dataPkgJobCountMatch = _mgr.DataPackageItems.Rows.Count;
                var dataPkgFileCountMatch = _mgr.AssociatedFiles.Rows.Count;

                if (dataPkgJobCountMatch == 0)
                {
                    LogWarning("Did not find any SEQUEST jobs in data package " + _mgr.DataPkgId);
                }
                else
                {
                    if (dataPkgFileCountMatch == 0)
                        LogWarning("Found " + dataPkgJobCountMatch + " SEQUEST jobs in data package " + _mgr.DataPkgId + " but did not find any candidate files to copy");
                    else
                        LogMessage("Copied " + dataPkgFileCountMatch + " files for " + dataPkgJobCountMatch + " SEQUEST jobs in data package " + _mgr.DataPkgId);
                }
            }
            m_progress = PROGRESS_PCT_SEQUEST_RESULTS_COPIED;
            m_StatusTools.UpdateAndWrite(m_progress);

            if (_bIncludeMZidFiles)
            {
                // find any MSGFPlus jobs in data package and copy their MZID files to appropriate cache subfolder
                _mgr.GetItemsToRepoPkg("DataPkgJobsQueryTemplate", "MSGFPlus", "*_msgfplus.zip;*_msgfplus.mzid.gz", @"MSGFPlus_Results\MZID_Files", "Job");
                var zipFileCountConverted = FileUtils.ConvertZipsToGZips(Path.Combine(_outputResultsFolderPath, @"MSGFPlus_Results\MZID_Files"), m_WorkDir);

                if (zipFileCountConverted > 0)
                    LogMessage("Converted " + zipFileCountConverted + " _msgfplus.zip files to .mzid.gz files");
            }
            m_progress = PROGRESS_PCT_MZID_RESULTS_COPIED;
            m_StatusTools.UpdateAndWrite(m_progress);

            var success = RetrieveInstrumentData(out var datasetsProcessed);
            if (!success)
                return false;

            if (datasetsProcessed == 0)
            {
                if (dataPkgJobCountMatch == 0)
                {
                    m_message = "Data package " + _mgr.DataPkgId +
                                " does not have any analysis jobs associated with it; please add some MASIC or DeconTools jobs then reset this job";
                    LogError(m_message);
                    return false;
                }


                var msg = "Data package " + _mgr.DataPkgId + " has " + dataPkgJobCountMatch + " associated jobs, but no instrument data files were retrieved";
                LogWarning(msg);

            }
            m_progress = PROGRESS_PCT_INSTRUMENT_DATA_COPIED;
            m_StatusTools.UpdateAndWrite(m_progress);

            // todo Do some logging on the above pipeline runs using pipeline intermediate results (_mgr.DataPackageItems; _mgr.AssociatedFiles; _mgr.ManifestForCopy;)?

            return true;
        }

        /// <summary>
        /// SetCnStr the report option flags from job parameters
        /// </summary>
        private void SetOptions()
        {
            // New parameters:
            _bIncludeMSGFPlusResults = m_jobParams.GetJobParameter("IncludeMSGFResults", true);
            _bIncludeMZidFiles = _bIncludeMSGFPlusResults;
            _bIncludeSequestResults = m_jobParams.GetJobParameter("IncludeSequestResults", false);	// This will get auto-changed to True if no MSGF+ jobs are found in the data package
            _bIncludeInstrumentData = m_jobParams.GetJobParameter("IncludeInstrumentData", true);
            _bIncludeMzXMLFiles = m_jobParams.GetJobParameter("IncludeMzXMLFiles", true);

        }

        /// <summary>
        /// SetCnStr the path for the repo cache folder
        /// </summary>
        private void SetOutputFolderPath()
        {
            var resultsFolderName = m_jobParams.GetJobParameter(clsAnalysisResources.JOB_PARAM_OUTPUT_FOLDER_NAME, "");
            var outputRootFolderPath = m_jobParams.GetJobParameter("CacheFolderPath", "");
            _outputResultsFolderPath = Path.Combine(outputRootFolderPath, resultsFolderName);
        }

        /// <summary>
        /// Generate handler that provides pre-packaged Mage pipelines
        /// that do the heavy lifting tasks that get data package items,
        /// find associated files, and copy them to repo cache folders
        /// </summary>
        /// <returns>Pipeline handler objet</returns>
        private void SetMagePipelineManager(string outputFolderPath = "")
        {
            var qd = new QueryDefinitions();
            qd.SetCnStr(QueryDefinitions.TagName.Main, m_mgrParams.GetParam("connectionstring"));
            qd.SetCnStr(QueryDefinitions.TagName.Broker, m_mgrParams.GetParam("brokerconnectionstring"));
            _mgr = new MageRepoPkgrPipelines
            {
                QueryDefs = qd,
                DataPkgId = m_jobParams.GetJobParameter("DataPackageID", "")
            };
            if (!string.IsNullOrEmpty(outputFolderPath))
            {
                _mgr.OutputResultsFolderPath = outputFolderPath;
            }
        }

        /// <summary>
        /// Retrieves or creates the .MzXML file for this dataset
        /// </summary>
        /// <param name="objMSXmlCreator">MzXML Creator</param>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="objAnalysisResults">Analysis Results class</param>
        /// <param name="dctDatasetRawFilePaths">Dictionary with dataset names and dataset raw file paths</param>
        /// <param name="dctDatasetYearQuarter">Dictionary with dataset names and year/quarter information</param>
        /// <param name="dctDatasetRawDataTypes">Dictionary with dataset names and the raw data type of the instrument data file</param>
        /// <param name="strDatasetFilePathLocal">Output parameter: Path to the locally cached dataset file</param>
        /// <returns>The full path to the locally created MzXML file</returns>
        protected string CreateMzXMLFileIfMissing(
            clsMSXMLCreator objMSXmlCreator,
            string datasetName,
            clsAnalysisResults objAnalysisResults,
            Dictionary<string, string> dctDatasetRawFilePaths,
            Dictionary<string, string> dctDatasetYearQuarter,
            Dictionary<string, string> dctDatasetRawDataTypes,
          out string strDatasetFilePathLocal)
        {

            strDatasetFilePathLocal = string.Empty;

            try
            {

                // Look in m_WorkDir for the .mzXML or .mzML file for this dataset
                var candidateFileNames = new List<string>
                    {
                        datasetName + clsAnalysisResources.DOT_MZXML_EXTENSION,
                        datasetName + clsAnalysisResources.DOT_MZXML_EXTENSION + clsAnalysisResources.DOT_GZ_EXTENSION,
                        datasetName + clsAnalysisResources.DOT_MZML_EXTENSION + clsAnalysisResources.DOT_GZ_EXTENSION
                    };

                foreach (var candidateFile in candidateFileNames)
                {
                    var filePath = Path.Combine(m_WorkDir, candidateFile);

                    if (File.Exists(filePath))
                    {
                        return filePath;
                    }

                    // Look for a StoragePathInfo file
                    filePath += clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX;

                    if (File.Exists(filePath))
                    {
                        var strDestPath = string.Empty;
                        var retrieveSuccess = RetrieveStoragePathInfoTargetFile(filePath, objAnalysisResults, ref strDestPath);
                        if (retrieveSuccess)
                        {
                            return strDestPath;
                        }
                        break;
                    }
                }

                // Need to create the .mzXML file
                if (!dctDatasetRawFilePaths.ContainsKey(datasetName))
                {
                    m_message = "Dataset " + datasetName + " not found in job parameter " +
                        clsAnalysisResources.JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS + "; unable to create the missing .mzXML file";
                    LogError(m_message);
                    return string.Empty;
                }

                m_jobParams.AddResultFileToSkip("MSConvert_ConsoleOutput.txt");

                objMSXmlCreator.UpdateDatasetName(datasetName);

                // Make sure the dataset file is present in the working directory
                // Copy it locally if necessary

                var strDatasetFilePathRemote = dctDatasetRawFilePaths[datasetName];
                if (string.IsNullOrEmpty(strDatasetFilePathRemote))
                {
                    m_message = "Dataset " + datasetName + " has an empty instrument file path in dctDatasetRawFilePaths";
                    LogError(m_message);
                    return string.Empty;
                }

                var blnDatasetFileIsAFolder = Directory.Exists(strDatasetFilePathRemote);

                strDatasetFilePathLocal = Path.Combine(m_WorkDir, Path.GetFileName(strDatasetFilePathRemote));

                if (blnDatasetFileIsAFolder)
                {
                    // Confirm that the dataset directory exists in the working directory

                    if (!Directory.Exists(strDatasetFilePathLocal))
                    {
                        // Copy the dataset directory locally
                        objAnalysisResults.CopyDirectory(strDatasetFilePathRemote, strDatasetFilePathLocal, overwrite: true);
                    }

                }
                else
                {
                    // Confirm that the dataset file exists in the working directory
                    if (!File.Exists(strDatasetFilePathLocal))
                    {
                        // Copy the dataset file locally
                        objAnalysisResults.CopyFileWithRetry(strDatasetFilePathRemote, strDatasetFilePathLocal, overwrite: true);
                    }
                }

                if (!dctDatasetRawDataTypes.TryGetValue(datasetName, out var rawDataType))
                    rawDataType = clsAnalysisResources.RAW_DATA_TYPE_DOT_RAW_FILES;

                m_jobParams.AddAdditionalParameter(clsAnalysisJob.JOB_PARAMETERS_SECTION, "RawDataType", rawDataType);

                var success = objMSXmlCreator.CreateMZXMLFile();

                if (!success && string.IsNullOrEmpty(m_message))
                {
                    m_message = objMSXmlCreator.ErrorMessage;
                    if (string.IsNullOrEmpty(m_message))
                    {
                        m_message = "Unknown error creating the mzXML file for dataset " + datasetName;
                    }
                    else if (!m_message.Contains(datasetName))
                    {
                        m_message += "; dataset " + datasetName;
                    }
                }

                if (!success)
                    return string.Empty;

                var fiMzXmlFilePathLocal = new FileInfo(Path.Combine(m_WorkDir, datasetName + clsAnalysisResources.DOT_MZXML_EXTENSION));

                if (!fiMzXmlFilePathLocal.Exists)
                {
                    m_message = "MSXmlCreator did not create the .mzXML file for dataset " + datasetName;
                    return string.Empty;
                }

                // Copy the .mzXML file to the cache
                // Gzip it first before copying
                var fiMzXmlFileGZipped = new FileInfo(fiMzXmlFilePathLocal + clsAnalysisResources.DOT_GZ_EXTENSION);
                success = m_DotNetZipTools.GZipFile(fiMzXmlFilePathLocal.FullName, true);
                if (!success)
                {
                    m_message = "Error compressing .mzXML file " + fiMzXmlFilePathLocal.Name + " with GZip: ";
                    return string.Empty;
                }

                fiMzXmlFileGZipped.Refresh();
                if (!fiMzXmlFileGZipped.Exists)
                {
                    m_message = "Compressed .mzXML file not found: " + fiMzXmlFileGZipped.FullName;
                    LogError(m_message);
                    return string.Empty;
                }

                var strMSXmlGeneratorName = Path.GetFileNameWithoutExtension(_MSXmlGeneratorAppPath);

                if (!dctDatasetYearQuarter.TryGetValue(datasetName, out var strDatasetYearQuarter))
                {
                    strDatasetYearQuarter = string.Empty;
                }

                CopyMzXMLFileToServerCache(fiMzXmlFileGZipped.FullName, strDatasetYearQuarter, strMSXmlGeneratorName, purgeOldFilesIfNeeded: true);

                m_jobParams.AddResultFileToSkip(Path.GetFileName(fiMzXmlFilePathLocal.FullName + clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX));

                PRISM.ProgRunner.GarbageCollectNow();

                return fiMzXmlFileGZipped.FullName;
            }
            catch (Exception ex)
            {
                m_message = "Exception in CreateMzXMLFileIfMissing";
                LogError(m_message, ex);
                return string.Empty;
            }

        }

        private void DeleteFileIgnoreErrors(string filePath)
        {
            try
            {
                if (File.Exists(filePath))
                    File.Delete(filePath);
            }
            catch (Exception ex)
            {
                LogWarning("Unable to delete file " + filePath + ": " + ex.Message);
            }
        }

        protected bool ProcessDataset(
            clsAnalysisResults objAnalysisResults,
            clsMSXMLCreator objMSXmlCreator,
            string datasetName,
            Dictionary<string, string> dctDatasetRawFilePaths,
            Dictionary<string, string> dctDatasetYearQuarter,
            Dictionary<string, string> dctDatasetRawDataTypes)
        {

            var strDatasetFilePathLocal = string.Empty;

            try
            {

                var instrumentDataFolderPath = Path.Combine(_outputResultsFolderPath, "Instrument_Data");

                if (!dctDatasetRawDataTypes.TryGetValue(datasetName, out var rawDataType))
                    rawDataType = clsAnalysisResources.RAW_DATA_TYPE_DOT_RAW_FILES;

                if (rawDataType != clsAnalysisResources.RAW_DATA_TYPE_DOT_UIMF_FILES && _bIncludeMzXMLFiles)
                {

                    // Create the .mzXML or .mzML file if it is missing
                    var mzXmlFilePathLocal = CreateMzXMLFileIfMissing(objMSXmlCreator, datasetName, objAnalysisResults,
                                                                      dctDatasetRawFilePaths,
                                                                      dctDatasetYearQuarter,
                                                                      dctDatasetRawDataTypes,
                                                                      out strDatasetFilePathLocal);

                    if (string.IsNullOrEmpty(mzXmlFilePathLocal))
                    {
                        return false;
                    }

                    var fiMzXmlFileSource = new FileInfo(mzXmlFilePathLocal);

                    // Copy the .MzXml file to the final folder
                    var fiTargetFile = new FileInfo(Path.Combine(instrumentDataFolderPath, Path.GetFileName(mzXmlFilePathLocal)));

                    if (fiTargetFile.Exists && fiTargetFile.Length == fiMzXmlFileSource.Length)
                        LogDebug("Skipping .mzXML file since already present in the target folder: " + fiTargetFile.FullName);
                    else
                        m_FileTools.CopyFileUsingLocks(mzXmlFilePathLocal, fiTargetFile.FullName, true);

                    // Delete the local .mzXml file
                    DeleteFileIgnoreErrors(mzXmlFilePathLocal);
                }

                if (_bIncludeInstrumentData)
                {
                    // Copy the .raw file, either from the local working directory or from the remote dataset directory
                    var strDatasetFilePathSource = dctDatasetRawFilePaths[datasetName];
                    if (!string.IsNullOrEmpty(strDatasetFilePathLocal))
                    {
                        // Dataset was already copied locally; copy it from the local computer to the staging folder
                        strDatasetFilePathSource = strDatasetFilePathLocal;
                    }

                    if (strDatasetFilePathSource.StartsWith(clsAnalysisResources.MYEMSL_PATH_FLAG))
                    {
                        // The file was in MyEMSL and should have already been retrieved via clsAnalysisResourcesRepoPkgr
                        strDatasetFilePathSource = Path.Combine(m_WorkDir, Path.GetFileName(strDatasetFilePathSource));
                        strDatasetFilePathLocal = strDatasetFilePathSource;
                    }

                    var blnDatasetFileIsAFolder = Directory.Exists(strDatasetFilePathSource);

                    if (blnDatasetFileIsAFolder)
                    {
                        var diDatasetFolder = new DirectoryInfo(strDatasetFilePathSource);
                        var strDatasetFilePathTarget = Path.Combine(instrumentDataFolderPath, diDatasetFolder.Name);
                        m_FileTools.CopyDirectory(strDatasetFilePathSource, strDatasetFilePathTarget);
                    }
                    else
                    {
                        var fiDatasetFile = new FileInfo(strDatasetFilePathSource);
                        var fiTargetFile = new FileInfo(Path.Combine(instrumentDataFolderPath, fiDatasetFile.Name));

                        if (fiTargetFile.Exists && fiTargetFile.Length == fiDatasetFile.Length)
                            LogDebug("Skipping instrument file since already present in the target folder: " + fiTargetFile.FullName);
                        else
                            m_FileTools.CopyFileUsingLocks(strDatasetFilePathSource, fiTargetFile.FullName, true);
                    }

                    if (!string.IsNullOrEmpty(strDatasetFilePathLocal))
                    {
                        if (blnDatasetFileIsAFolder)
                        {
                            // Delete the local dataset directory
                            if (Directory.Exists(strDatasetFilePathLocal))
                            {
                                Directory.Delete(strDatasetFilePathLocal, true);
                            }
                        }
                        else
                        {
                            // Delete the local dataset file
                            DeleteFileIgnoreErrors(strDatasetFilePathLocal);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Error retrieving instrument data for " + datasetName, ex);
                return false;
            }

            return true;

        }

        private bool RetrieveInstrumentData(out int datasetsProcessed)
        {

            // Extract the packed parameters
            var dctDatasetRawFilePaths = ExtractPackedJobParameterDictionary(clsAnalysisResources.JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS);
            var dctDatasetYearQuarter = ExtractPackedJobParameterDictionary(clsAnalysisResourcesRepoPkgr.JOB_PARAM_DICTIONARY_DATASET_STORAGE_YEAR_QUARTER);
            var dctDatasetRawDataTypes = ExtractPackedJobParameterDictionary(clsAnalysisResources.JOB_PARAM_DICTIONARY_DATASET_RAW_DATA_TYPES);

            datasetsProcessed = 0;

            // The objAnalysisResults object is used to copy files to/from this computer
            var objAnalysisResults = new clsAnalysisResults(m_mgrParams, m_jobParams);

            _MSXmlGeneratorAppPath = GetMSXmlGeneratorAppPath();
            var objMSXmlCreator = new clsMSXMLCreator(_MSXmlGeneratorAppPath, m_WorkDir, m_Dataset, m_DebugLevel, m_jobParams);

            RegisterEvents(objMSXmlCreator);

            var successCount = 0;
            var errorCount = 0;

            if (dctDatasetRawFilePaths.Keys.Count == 0)
            {
                m_message = "Could not retrieve instrument data since dctDatasetRawFilePaths is empty";
                return false;
            }

            // Process each dataset
            foreach (var datasetName in dctDatasetRawFilePaths.Keys)
            {

                var success = ProcessDataset(objAnalysisResults, objMSXmlCreator, datasetName, dctDatasetRawFilePaths, dctDatasetYearQuarter, dctDatasetRawDataTypes);

                if (success)
                    successCount++;
                else
                    errorCount++;

                datasetsProcessed += 1;
                m_progress = ComputeIncrementalProgress(PROGRESS_PCT_MZID_RESULTS_COPIED, PROGRESS_PCT_INSTRUMENT_DATA_COPIED, datasetsProcessed, dctDatasetRawFilePaths.Count);
                m_StatusTools.UpdateAndWrite(m_progress);

            }

            if (successCount > 0)
            {
                if (errorCount == 0)
                    return true;

                m_message = "Could not retrieve instrument data for one or more datasets in Data package " + _mgr.DataPkgId + "; SuccessCount = " + successCount + "; FailCount = " + errorCount;
                return false;
            }

            m_message = "Could not retrieve instrument data for any of the datasets in Data package " + _mgr.DataPkgId + "; FailCount = " + errorCount;
            return false;

        }

        protected bool RetrieveStoragePathInfoTargetFile(string strStoragePathInfoFilePath, clsAnalysisResults objAnalysisResults, ref string strDestPath)
        {
            const bool IsFolder = false;
            return RetrieveStoragePathInfoTargetFile(strStoragePathInfoFilePath, objAnalysisResults, IsFolder, ref strDestPath);
        }

        protected bool RetrieveStoragePathInfoTargetFile(string strStoragePathInfoFilePath, clsAnalysisResults objAnalysisResults, bool IsFolder, ref string strDestPath)
        {
            var strSourceFilePath = string.Empty;

            try
            {
                strDestPath = string.Empty;

                if (!File.Exists(strStoragePathInfoFilePath))
                {
                    m_message = "StoragePathInfo file not found";
                    LogError(m_message + ": " + strStoragePathInfoFilePath);
                    return false;
                }

                using (var srInfoFile = new StreamReader(new FileStream(strStoragePathInfoFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    if (!srInfoFile.EndOfStream)
                    {
                        strSourceFilePath = srInfoFile.ReadLine();
                    }
                }

                if (string.IsNullOrEmpty(strSourceFilePath))
                {
                    m_message = "StoragePathInfo file was empty";
                    LogError(m_message + ": " + strStoragePathInfoFilePath);
                    return false;
                }

                strDestPath = Path.Combine(m_WorkDir, Path.GetFileName(strSourceFilePath));

                if (IsFolder)
                {
                    objAnalysisResults.CopyDirectory(strSourceFilePath, strDestPath, overwrite: true);
                }
                else
                {
                    objAnalysisResults.CopyFileWithRetry(strSourceFilePath, strDestPath, overwrite: true);
                }

            }
            catch (Exception ex)
            {
                m_message = "Error in RetrieveStoragePathInfoTargetFile";
                LogError(m_message, ex);
                return false;
            }

            return true;

        }

        private bool StoreToolVersionInfo()
        {
            var strToolVersionInfo = string.Empty;

            // Lookup the version of the AnalysisManagerRepoPkgr plugin
            if (!StoreToolVersionInfoForLoadedAssembly(ref strToolVersionInfo, "AnalysisManager_RepoPkgr_Plugin", includeRevision: false))
                return false;

            try
            {
                return SetStepTaskToolVersion(strToolVersionInfo, new List<FileInfo>(), saveToolVersionTextFile: false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }

        }

        /// <summary>
        /// Examines orgDbName to see if it is of the form ID_003878_0C3354F8.fasta
        /// If it is, queries the protein sequences database for the names of the protein collections used to generate the file
        /// </summary>
        /// <param name="orgDbName"></param>
        /// <returns>If three or fewer protein collections, returns an updated filename based on the protein collection names.  Otherwise, simply returns orgDbName</returns>
        private string UpdateOrgDBNameIfRequired(string orgDbName)
        {
            var reArchiveFileId = new Regex(@"ID_([0-9]+)_[A-Z0-9]+\.fasta", RegexOptions.Compiled);

            try
            {
                // ID_003878_0C3354F8.fasta
                var reMatch = reArchiveFileId.Match(orgDbName);

                if (!reMatch.Success)
                    return orgDbName;		// Likely used a legacy fasta file

                var fileID = int.Parse(reMatch.Groups[1].Value);

                var sqlQuery = "SELECT FileName FROM V_Archived_Output_File_Protein_Collections WHERE Archived_File_ID = " + fileID;
                var retryCount = 3;
                var proteinSeqsDBConnectionString = m_mgrParams.GetParam("fastacnstring");
                if (string.IsNullOrWhiteSpace(proteinSeqsDBConnectionString))
                {
                    LogError("Error in UpdateOrgDBNameIfRequired: manager parameter fastacnstring is not defined");
                    return orgDbName;
                }

                var lstProteinCollections = new List<string>();

                while (retryCount > 0)
                {
                    try
                    {
                        using (var connection = new SqlConnection(proteinSeqsDBConnectionString))
                        {
                            connection.Open();
                            using (var dbCommand = new SqlCommand(sqlQuery, connection))
                            {
                                var dbReader = dbCommand.ExecuteReader();
                                if (dbReader.HasRows)
                                {
                                    while (dbReader.Read())
                                    {
                                        lstProteinCollections.Add(dbReader.GetString(0));
                                    }
                                }
                            }
                        }

                        // Data successfully retrieved
                        // Exit the while loop
                        break;
                    }
                    catch (Exception ex)
                    {
                        retryCount -= 1;
                        LogError("Exception getting protein collection info from Protein Sequences database for Archived_File_ID = " + fileID, ex);

                        // Delay for 2 seconds before trying again
                        clsGlobal.IdleLoop(2);
                    }
                }

                if (lstProteinCollections.Count > 0 && lstProteinCollections.Count < 4)
                {
                    var orgDbNameNew = string.Join("_", lstProteinCollections) + ".fasta";
                    return orgDbNameNew;
                }

            }
            catch (Exception ex)
            {
                LogError("Exception in UpdateOrgDBNameIfRequired", ex);
            }

            return orgDbName;
        }

        #endregion

    }
}
