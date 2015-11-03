/*****************************************************************
** Written by Matthew Monroe for the US Department of Energy    **
** Pacific Northwest National Laboratory, Richland, WA          **
** Created 10/05/2015                                           **
**                                                              **
*****************************************************************/

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml.XPath;
using AnalysisManagerBase;

namespace AnalysisManagerQCARTPlugin
{
    public class clsAnalysisResourcesQCART : clsAnalysisResources
    {
        public const string JOB_PARAMETER_QCART_BASELINE_DATASET_NAMES = "QC-ART_Baseline_Dataset_Names";
        public const string JOB_PARAMETER_QCART_BASELINE_UNIQUE_KEY = "QC-ART_Baseline_Datasets_Unique_Key";
        public const string JOB_PARAMETER_QCART_EXISTING_BASELINE_RESULTS_FILENAME = "QC-ART_Existing_Baseline_Results_File";

        private string mProjectName;

        /// <summary>
        /// The unique key is a MD5 hash of the dataset names, appended with JobFirst_JobLast
        /// </summary>
        private string mBaselineUniqueKey;

        /// <summary>
        /// Keys are dataset names; values are MASIC job numbers
        /// </summary>
        private readonly Dictionary<string, int> mBaselineDatasets;

        /// <summary>
        /// Constructor
        /// </summary>
        public clsAnalysisResourcesQCART()
        {
            mProjectName = string.Empty;
            mBaselineDatasets = new Dictionary<string, int>();
            mBaselineUniqueKey = string.Empty;
        }

        public override IJobParams.CloseOutType GetResources()
        {

            var currentTask = "Initializing";
            mProjectName = string.Empty;
            mBaselineDatasets.Clear();

            try
            {
                // Retrieve the parameter file
                currentTask = "Retrieve the parameter file";
                var paramFileName = m_jobParams.GetParam("ParmFileName");
                var paramFileStoragePath = m_jobParams.GetParam("ParmFileStoragePath");

                if (!RetrieveFile(paramFileName, paramFileStoragePath))
                {
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                // Parse the parameter file to discover the baseline datasets
                currentTask = "Read the parameter file";

                var paramFilePath = Path.Combine(m_WorkingDir, paramFileName);
                if (!ParseQCARTParamFile(paramFilePath))
                {
                    if (string.IsNullOrWhiteSpace(m_message))
                        LogError("ParseQCARTParamFile returned false (unknown error)");

                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                // Look for an existing QC-ART baseline results file for the given set of datasets
                // If one is not found, we need to obtain the necessary files for the 
                // baseline datasets so that the QC-ART tool can compute a new baseline

                currentTask = "Get existing baseline results";
                bool criticalError;
                var baselineResultsFound = FindBaselineResults(paramFilePath, out criticalError);
                if (criticalError)
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;

                // Cache the current dataset and job info
                var udtCurrentDatasetAndJobInfo = GetCurrentDatasetAndJobInfo();

                currentTask = "Retrieve _Reportions.txt file for " + m_DatasetName;

                if (!RetrieveReporterIonsFile(udtCurrentDatasetAndJobInfo))
                {
                    return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                if (!baselineResultsFound)
                {
                    currentTask = "Retrieve _Reportions.txt files for the baseline datasets";
                    if (!RetrieveDataForBaselineDatasets(udtCurrentDatasetAndJobInfo))
                        return IJobParams.CloseOutType.CLOSEOUT_FAILED; ;
                }

                currentTask = "Process the MyEMSL download queue";
                if (!ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
                {
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }


                // Store some parameters that will be used by clsAnalysisToolRunnerQCART
                // Baseline Dataset Names
                StorePackedJobParameterList(mBaselineDatasets.Keys.ToList(), JOB_PARAMETER_QCART_BASELINE_DATASET_NAMES);

                // Baseline unique key
                m_jobParams.AddAdditionalParameter("JobParameters", JOB_PARAMETER_QCART_BASELINE_UNIQUE_KEY, mBaselineUniqueKey);

                return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;

            }
            catch (Exception ex)
            {
                LogError("Exception in GetResources; task = " + currentTask, ex);
                return IJobParams.CloseOutType.CLOSEOUT_FAILED;
            }

        }

        private bool ParseQCARTParamFile(string paramFilePath)
        {
            const string NO_BASELINE_INFO = "One or more baseline datasets not found in the QC-ART parameter file; " +
                                            "expected at <Parameters><BaselineList><BaselineDataset>";

            try
            {
                var paramFile = new XPathDocument(new FileStream(paramFilePath, FileMode.Open, FileAccess.Read, FileShare.Read));
                var contents = paramFile.CreateNavigator();

                var projectNode = contents.Select("/Parameters/Metadata/Project");
                if (!projectNode.MoveNext())
                {
                    LogError("Project node not found in the QC-ART parameter file; " +
                             "expected at <Parameters><Metadata><Project>");
                    return false;
                }

                mProjectName = projectNode.Current.Value;
                if (string.IsNullOrWhiteSpace(mProjectName))
                {
                    mProjectName = "Unknown";
                }

                var baselineDatasets = contents.Select("/parameters/BaselineList/BaselineDataset");
                if (baselineDatasets.Count == 0)
                {
                    LogError(NO_BASELINE_INFO);
                    return false;
                }

                while (baselineDatasets.MoveNext())
                {
                    if (baselineDatasets.Current.HasChildren)
                    {
                        var masicJob = baselineDatasets.Current.SelectChildren("MasicJob", string.Empty);
                        var datasetName = baselineDatasets.Current.SelectChildren("Dataset", string.Empty);

                        if (masicJob.MoveNext() && datasetName.MoveNext())
                        {
                            mBaselineDatasets.Add(datasetName.Current.Value, masicJob.Current.ValueAsInt);
                            continue;
                        }
                    }

                    LogError("Nodes MasicJob and Dataset not found beneath a BaselineDataset node " +
                             "in the QC-ART parameter file");
                    return false;
                }

                if (mBaselineDatasets.Count == 0)
                {
                    LogError(NO_BASELINE_INFO);
                    return false;
                }

                // Compute the MD5 Hash of the datasets and jobs in mBaselineDatasets
                var sbTextToHash = new StringBuilder();
                foreach (var item in mBaselineDatasets)
                {
                    sbTextToHash.Append(item.Key);
                    sbTextToHash.Append(item.Value);
                }

                // Hash contents of this stream
                var md5Hash = clsGlobal.ComputeStringHashMD5(sbTextToHash.ToString());

                mBaselineUniqueKey = md5Hash + "_" + mBaselineDatasets.Values.Min() + "_" + mBaselineDatasets.Values.Max();

                return true;
            }
            catch (Exception ex)
            {
                LogError("Exception in ParseQCARTParamFile", ex);
                return false;
            }

        }

        /// <summary>
        /// Look for existing baseline results
        /// </summary>
        /// <param name="paramFilePath">Parameter file path</param>
        /// <param name="criticalError">Output: true if a critical error occurred and the job should be aborted</param>
        /// <returns>True if existing results were found and successfully copied locally; otherwise false</returns>
        /// <remarks>Also uses mProjectName and mBaselineDatasets</remarks>
        private bool FindBaselineResults(string paramFilePath, out bool criticalError)
        {
            criticalError = false;
            var currentTask = "Initializing";

            try
            {
                string errorMessage;

                currentTask = "Finding parameter file";
                var fiParamFile = new FileInfo(paramFilePath);
                if (!fiParamFile.Exists)
                {
                    errorMessage = "Parameter file not found";
                    LogError(errorMessage, errorMessage + ": " + paramFilePath);
                    criticalError = true;
                    return false;
                }

                if (fiParamFile.Directory == null)
                {
                    errorMessage = "Parameter file directory is null";
                    LogError(errorMessage, errorMessage + ": " + paramFilePath);
                    criticalError = true;
                    return false;
                }

                currentTask = "Finding cache folder";
                var diCacheFolder = new DirectoryInfo(Path.Combine(fiParamFile.Directory.FullName, "Cache"));

                if (!diCacheFolder.Exists)
                {
                    // Create the folder (we'll use it later)
                    diCacheFolder.Create();
                    return false;
                }

                currentTask = "Finding project-specific cache folder";
                var diProjectFolder = new DirectoryInfo(Path.Combine(diCacheFolder.FullName, mProjectName));

                if (!diProjectFolder.Exists)
                {
                    // Create the folder (we'll use it later)
                    diProjectFolder.Create();
                    return false;
                }

                // Look for a QCART_Cache file in the Project folder                
                var baselineResultsFileName = "QCART_Cache_" + mBaselineUniqueKey + ".xml";
                currentTask = "Looking for " + baselineResultsFileName;

                var baselineResultsPath = Path.Combine(diProjectFolder.FullName, baselineResultsFileName);

                var fiBaselineResultsMetadata = new FileInfo(baselineResultsPath);

                if (!fiBaselineResultsMetadata.Exists)
                {
                    // Valid baseline results metadata file was not found
                    return false;
                }

                var retrieveCachedResults = true;
                if (fiBaselineResultsMetadata.Length == 0)
                {
                    // 0-byte file
                    // Another manager is processing the baseline results

                    currentTask = "Waiting for baseline results metadata to finalize: " + fiBaselineResultsMetadata.FullName;

                    retrieveCachedResults = WaitForExistingResults(fiBaselineResultsMetadata);
                }

                if (!retrieveCachedResults)
                {
                    // Valid baseline results metadata file was not found
                    return false;
                }

                currentTask = "Retrieving cached results";

                var success = RetrieveExistingBaselineResultFile(fiBaselineResultsMetadata, out criticalError);
                return success;

            }
            catch (Exception ex)
            {
                LogError("Exception in FindBaselineResults: " + currentTask, ex);
                criticalError = true;
                return false;
            }

        }

        /// <summary>
        /// Retrieve the MASIC _ReporterIons.txt file for each job in 
        /// </summary>
        /// <param name="udtCurrentDatasetAndJobInfo"></param>
        /// <returns>True if success; otherwise false</returns>
        private bool RetrieveDataForBaselineDatasets(udtDataPackageJobInfoType udtCurrentDatasetAndJobInfo)
        {

            foreach (var datasetJobInfo in mBaselineDatasets)
            {
                var baselineDatasetName = datasetJobInfo.Key;
                var baselineDatasetJob = datasetJobInfo.Value;
                udtDataPackageJobInfoType udtJobInfo;

                if (!LookupJobInfo(baselineDatasetJob, out udtJobInfo))
                {
                    if (string.IsNullOrWhiteSpace(m_message))
                    {
                        LogError("Unknown error retrieving the details for baseline job " + baselineDatasetJob);
                    }

                    return false;
                }

                if (!clsGlobal.IsMatch(udtJobInfo.Dataset, baselineDatasetName))
                {
                    var logMessage = "Mismatch in the QC-ART parameter file; " +
                                     "baseline job " + baselineDatasetJob + " is not dataset " + baselineDatasetName;

                    LogError(logMessage, logMessage + "; it is actually dataset " + udtJobInfo.Dataset);

                    return false;
                }

                OverrideCurrentDatasetAndJobInfo(udtJobInfo);

                RetrieveReporterIonsFile(udtJobInfo);
            }

            OverrideCurrentDatasetAndJobInfo(udtCurrentDatasetAndJobInfo);
            return true;

        }

        /// <summary>
        /// Parse the baseline results metadata file to determine the baseline results filename then copy it locally
        /// </summary>
        /// <param name="fiBaselineResultsMetadata"></param>
        /// <param name="criticalError"></param>
        /// <returns>True if the baseline results were successfully copied locally; otherwise false</returns>
        private bool RetrieveExistingBaselineResultFile(FileInfo fiBaselineResultsMetadata, out bool criticalError)
        {

            criticalError = false;

            try
            {
                if (fiBaselineResultsMetadata.Directory == null)
                {
                    var warningMessage = "QC-ART baseline results metadata file directory is null";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, warningMessage);
                    return false;
                }

                var paramFile = new XPathDocument(new FileStream(fiBaselineResultsMetadata.FullName, FileMode.Open, FileAccess.Read, FileShare.Read));
                var contents = paramFile.CreateNavigator();

                var projectNode = contents.Select("/Parameters/Results/BaselineDataFile");
                if (!projectNode.MoveNext())
                {
                    var warningMessage = "BaselineDataFile node not found in the QC-ART baseline results metadata file; " +
                                         "expected at <Parameters><Results><BaselineDataFile>";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, warningMessage);                                        
                    return false;
                }

                var baselineDataFileName = projectNode.Current.Value;
                if (string.IsNullOrWhiteSpace(baselineDataFileName))
                {
                    var warningMessage = "BaselineDataFile node is empty in the QC-ART baseline results metadata file";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, warningMessage);
                    return false;
                }

                var baselineResultsFilePath = Path.Combine(fiBaselineResultsMetadata.Directory.FullName, baselineDataFileName);

                var fiBaselineResultsFileSource = new FileInfo(baselineResultsFilePath);

                if (!fiBaselineResultsFileSource.Exists || fiBaselineResultsFileSource.Directory == null)
                {
                    var warningMessage = "QC-ART baseline data file not found: " + baselineResultsFilePath;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, warningMessage);

                    warningMessage = "Deleting invalid QC-ART baseline results metadata file: " + fiBaselineResultsMetadata.FullName;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, warningMessage);

                    fiBaselineResultsMetadata.Delete();
                    return false;
                }

                var success = CopyFileToWorkDir(fiBaselineResultsFileSource.Name, fiBaselineResultsFileSource.Directory.FullName, m_WorkingDir);

                if (!success)
                {
                    if (string.IsNullOrWhiteSpace(m_message))
                        LogError("Unknown error retrieving " + fiBaselineResultsFileSource.Name);

                    criticalError = true;
                    return false;
                }

                m_jobParams.AddAdditionalParameter("JobParameters",
                                                   JOB_PARAMETER_QCART_EXISTING_BASELINE_RESULTS_FILENAME,
                                                   fiBaselineResultsFileSource.Name);
                return true;

            }
            catch (Exception ex)
            {
                LogError("Exception in RetrieveExistingBaselineResultFile", ex);
                criticalError = true;
                return false;
            }
        }

        /// <summary>
        /// Wait for the QC-ART baseline results metadata file to become valid
        /// </summary>
        /// <param name="fiBaselineResults"></param>
        /// <returns>True if cached results are now ready to retrieve</returns>
        /// <remarks>Will wait for up to 60 minutes</remarks>
        private bool WaitForExistingResults(FileInfo fiBaselineResults)
        {
            const int MAX_WAITTIME_MINUTES = 60;

            var startTime = DateTime.UtcNow;
            var lastStatusUpdate = DateTime.UtcNow;

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO,
                                 "Waiting for existing QC-ART baseline results metadata file to be larger than 0 bytes: " +
                                 fiBaselineResults.FullName);

            while (fiBaselineResults.Exists && fiBaselineResults.Length == 0)
            {
                var minutesElapsed = DateTime.UtcNow.Subtract(startTime).TotalMinutes;

                if (DateTime.UtcNow.Subtract(lastStatusUpdate).TotalMinutes >= 1)
                {
                    var debugMessage = "Waiting for " + fiBaselineResults.Name + "; " + minutesElapsed.ToString("0.0") + " minutes elapsed";

                    m_StatusTools.CurrentOperation = debugMessage;
                    m_StatusTools.UpdateAndWrite(0);

                    if (m_DebugLevel >= 3)
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, debugMessage);

                    lastStatusUpdate = DateTime.UtcNow;
                }

                // Wait for 5 seconds
                System.Threading.Thread.Sleep(5000);

                if (minutesElapsed >= MAX_WAITTIME_MINUTES)
                {
                    // Waited too long
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO,
                                "QC-ART baseline results metadata file unchanged after " + MAX_WAITTIME_MINUTES + " minutes");
                    break;
                }

                fiBaselineResults.Refresh();
            }

            // Wait for 1 more second
            System.Threading.Thread.Sleep(1000);

            fiBaselineResults.Refresh();
            if (fiBaselineResults.Exists && fiBaselineResults.Length > 0)
            {
                // File exists and is > 0 bytes
                // Assumed to be valid
                return true;
            }

            if (fiBaselineResults.Exists)
            {
                // Stale file; delete it            
                fiBaselineResults.Delete();
            }

            return false;

        }

        /// <summary>
        /// Retrieve the MASIC _ReporterIons.txt file
        /// for the dataset and job in udtJobInfo
        /// </summary>
        /// <param name="udtJobInfo"></param>
        /// <returns></returns>
        private bool RetrieveReporterIonsFile(udtDataPackageJobInfoType udtJobInfo)
        {
            try
            {

                var targetDatasetName = m_jobParams.GetJobParameter("SourceJob2Dataset", string.Empty);
                var targetDatasetMasicJob = m_jobParams.GetJobParameter("SourceJob2", 0);

                if (string.IsNullOrWhiteSpace(targetDatasetName) || targetDatasetMasicJob == 0)
                {
                    var baseMessage = "Job parameters SourceJob2Dataset and SourceJob2 not found; populated via the [Special Processing] job parameter";
                    LogError(baseMessage, baseMessage + "; for example 'SourceJob:Auto{Tool = \"SMAQC_MSMS\"}, Job2:Auto{Tool = \"MASIC_Finnigan\"}'");
                    return false;
                }

                var inputFolderNameCached = m_jobParams.GetJobParameter("JobParameters", "inputFolderName", string.Empty);

                if (clsGlobal.IsMatch(udtJobInfo.Dataset, targetDatasetName) && m_JobNum == udtJobInfo.Job)
                {
                    // Retrieving the _ReporterIons.txt file for the dataset associated with this QC-ART job
                    var masicFolderPath = m_jobParams.GetJobParameter("SourceJob2FolderPath", string.Empty);

                    if (string.IsNullOrWhiteSpace(masicFolderPath))
                    {
                        var baseMessage = "Job parameter SourceJob2FolderPath not found; populated via the [Special Processing] job parameter";
                        LogError(baseMessage, baseMessage + "; for example 'SourceJob:Auto{Tool = \"SMAQC_MSMS\"}, Job2:Auto{Tool = \"MASIC_Finnigan\"}'");
                        return false;
                    }

                    // Override inputFolderName
                    m_jobParams.AddAdditionalParameter("JobParameters", "inputFolderName", Path.GetDirectoryName(masicFolderPath));
                }
                else
                {
                    // Baseline dataset
                    m_jobParams.AddAdditionalParameter("JobParameters", "inputFolderName", udtJobInfo.ResultsFolderName);
                }


                var success = FindAndRetrieveMiscFiles(m_DatasetName + "_ReporterIons.txt", false);
                if (!success)
                {
                    LogError("_ReporterIons.txt file not found for dataset " + udtJobInfo.Dataset + ", job " + udtJobInfo.Job);
                    return false;
                }

                // Restore the inputFolderName
                m_jobParams.AddAdditionalParameter("JobParameters", "inputFolderName", inputFolderNameCached);

                return true;

            }
            catch (Exception ex)
            {
                LogError("Exception in RetrieveReporterIonsFile", ex);
                return false;
            }

        }

    }
}
