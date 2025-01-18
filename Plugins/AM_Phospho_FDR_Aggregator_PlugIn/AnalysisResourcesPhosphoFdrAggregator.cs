using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using PRISM.Logging;

namespace AnalysisManagerPhospho_FDR_AggregatorPlugIn
{
    /// <summary>
    /// Retrieve resources for the Phospho FDR Aggregator plugin
    /// </summary>
    public class AnalysisResourcesPhosphoFdrAggregator : AnalysisResources
    {
        // Ignore Spelling: aggregator, nocopy, phospho, sequest, xxx_ascore

        /// <summary>
        /// Retrieve required files
        /// </summary>
        /// <returns>Closeout code</returns>
        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            // Lookup the file processing options, for example:
            // sequest:_syn.txt:nocopy,sequest:_fht.txt:nocopy,sequest:_dta.zip:nocopy,masic_finnigan:_ScanStatsEx.txt:nocopy
            // MSGFPlus:_msgfplus_syn.txt,MSGFPlus:_msgfplus_fht.txt,MSGFPlus:_dta.zip,masic_finnigan:_ScanStatsEx.txt

            var fileSpecList = mJobParams.GetParam("TargetJobFileList").Split(',').ToList();

            foreach (var fileSpec in fileSpecList.ToList())
            {
                var fileSpecTerms = fileSpec.Split(':').ToList();

                if (fileSpecTerms.Count <= 2 || fileSpecTerms[2].ToLower().Trim() != "copy")
                {
                    mJobParams.AddResultFileExtensionToSkip(fileSpecTerms[1]);
                }
            }

            LogMessage("Retrieving the param file");

            var paramFilesCopied = 0;

            if (!RetrieveAScoreParamFile("AScoreCIDParamFile", ref paramFilesCopied))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!RetrieveAScoreParamFile("AScoreETDParamFile", ref paramFilesCopied))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!RetrieveAScoreParamFile("AScoreHCDParamFile", ref paramFilesCopied))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (paramFilesCopied == 0)
            {
                mMessage = "One or more of these job parameters must define a valid AScore parameter file name: AScoreCIDParamFile, AScoreETDParamFile, or AScoreHCDParamFile";
                LogError(mMessage);
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            LogMessage("Retrieving input files");
            const bool callingMethodCanRegenerateMissingFile = false;

            // Retrieve the files for the jobs in the data package associated with this job
            if (!RetrieveAggregateFiles(
                    fileSpecList, DataPackageFileRetrievalModeConstants.Ascore,
                    callingMethodCanRegenerateMissingFile, out var dataPackageJobs))
            {
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Cache the data package info
            if (!CacheDataPackageInfo(dataPackageJobs))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private bool CacheDataPackageInfo(IReadOnlyDictionary<int, DataPackageJobInfo> dataPackageJobs)
        {
            try
            {
                var workDir = new DirectoryInfo(mWorkDir);

                var jobToDatasetMap = new Dictionary<string, string>();
                var jobToSettingsFileMap = new Dictionary<string, string>();
                var jobToolMap = new Dictionary<string, string>();

                // Find the Job* folders

                foreach (var subdirectory in workDir.GetDirectories("Job*"))
                {
                    var jobNumber = int.Parse(subdirectory.Name.Substring(3));
                    var jobInfo = dataPackageJobs[jobNumber];

                    jobToDatasetMap.Add(jobNumber.ToString(), jobInfo.Dataset);
                    jobToSettingsFileMap.Add(jobNumber.ToString(), jobInfo.SettingsFileName);
                    jobToolMap.Add(jobNumber.ToString(), jobInfo.Tool);
                }

                // Store the packed job parameters
                StorePackedJobParameterDictionary(jobToDatasetMap, JOB_PARAM_DICTIONARY_JOB_DATASET_MAP);
                StorePackedJobParameterDictionary(jobToSettingsFileMap, JOB_PARAM_DICTIONARY_JOB_SETTINGS_FILE_MAP);
                StorePackedJobParameterDictionary(jobToolMap, JOB_PARAM_DICTIONARY_JOB_TOOL_MAP);
            }
            catch (Exception ex)
            {
                mMessage = "Error in CacheDataPackageInfo";
                LogError("Error in CacheDataPackageInfo: " + ex.Message);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Retrieves the AScore parameter file stored in the given parameter name
        /// </summary>
        /// <param name="parameterName">AScoreCIDParamFile or AScoreETDParamFile or AScoreHCDParamFile</param>
        /// <param name="paramFilesCopied">Incremented if the parameter file is found and copied</param>
        private bool RetrieveAScoreParamFile(string parameterName, ref int paramFilesCopied)
        {
            var paramFileName = mJobParams.GetJobParameter(parameterName, string.Empty);

            if (string.IsNullOrWhiteSpace(paramFileName))
            {
                return true;
            }

            if (paramFileName.StartsWith("xxx_ascore_", StringComparison.OrdinalIgnoreCase) || paramFileName.StartsWith("xxx_undefined", StringComparison.OrdinalIgnoreCase))
            {
                // Dummy parameter file; ignore it
                // Update the job parameter to be an empty string so that this parameter is ignored in BuildInputFile
                mJobParams.SetParam(parameterName, string.Empty);
                return true;
            }

            var success = FileSearchTool.RetrieveFile(paramFileName, mJobParams.GetParam(JOB_PARAM_TRANSFER_DIRECTORY_PATH), 2, BaseLogger.LogLevels.DEBUG);

            if (!success)
            {
                // File not found in the transfer folder
                // Look in the AScore parameter folder on Gigasax, \\gigasax\DMS_Parameter_Files\AScore

                var paramFileFolder = mJobParams.GetJobParameter("ParamFileStoragePath", @"\\gigasax\DMS_Parameter_Files\AScore");
                success = FileSearchTool.RetrieveFile(paramFileName, paramFileFolder, 2);
            }

            if (success)
            {
                paramFilesCopied++;
            }

            return success;
        }
    }
}
