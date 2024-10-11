using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.StatusReporting;

namespace AnalysisManagerMzRefineryPlugIn
{
    /// <summary>
    /// Retrieve resources for the MzRefinery plugin
    /// </summary>
    public class AnalysisResourcesMzRefinery : AnalysisResources
    {
        // Ignore Spelling: dta

        /// <summary>
        /// Initialize options
        /// </summary>
        public override void Setup(string stepToolName, IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, MyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(stepToolName, mgrParams, jobParams, statusTools, myEMSLUtilities);
            SetOption(Global.AnalysisResourceOptions.OrgDbRequired, true);
        }

        /// <summary>
        /// Retrieves files necessary for running MzRefinery
        /// </summary>
        /// <returns>CloseOutType indicating success or failure</returns>
        public override CloseOutType GetResources()
        {
            var currentTask = "Initializing";

            try
            {
                currentTask = "Retrieve shared resources";

                // Retrieve shared resources, including the JobParameters file from the previous job step
                GetSharedResources();

                var mzRefParamFile = mJobParams.GetJobParameter("MzRefParamFile", string.Empty);

                if (string.IsNullOrEmpty(mzRefParamFile))
                {
                    LogError("MzRefParamFile parameter is empty");
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }

                if (mzRefParamFile.Equals("SkipAll", StringComparison.OrdinalIgnoreCase))
                {
                    // Do not perform any processing with MzRefinery
                    EvalMessage = "Skipping MzRefinery since job parameter MzRefParamFile is 'SkipAll'";
                    return CloseOutType.CLOSEOUT_SKIPPED_MZ_REFINERY;
                }

                currentTask = "Get input file(s)";

                // Typically only MsXMLGenerator is defined
                // However, for MSGFPlus_DeconMSn_MzRefinery jobs, we retrieve both a _dta.txt file (from DeconMSn) and a .mzML file (from MSConvert)

                var dtaGenerator = mJobParams.GetJobParameter("DtaGenerator", string.Empty);
                var msXmlGenerator = mJobParams.GetJobParameter("MSXMLGenerator", string.Empty);

                if (string.IsNullOrWhiteSpace(dtaGenerator) && string.IsNullOrWhiteSpace(msXmlGenerator))
                {
                    LogMessage("Job parameters must have DtaGenerator or MSXMLGenerator defined");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (!string.IsNullOrWhiteSpace(dtaGenerator))
                {
                    LogMessage("Running MzRefinery on a _dta.txt file");
                    var result = GetCDTAFile();

                    if (result != CloseOutType.CLOSEOUT_SUCCESS)
                    {
                        return result;
                    }
                }

                if (!string.IsNullOrWhiteSpace(msXmlGenerator))
                {
                    // Retrieve the .mzML file
                    var result = GetMsXmlFile();

                    if (result != CloseOutType.CLOSEOUT_SUCCESS)
                    {
                        return result;
                    }
                }

                // Retrieve the FASTA file
                var orgDbDirectoryPath = mMgrParams.GetParam("OrgDbDir");

                currentTask = "RetrieveOrgDB to " + orgDbDirectoryPath;

                if (!RetrieveOrgDB(orgDbDirectoryPath, out var resultCode))
                {
                    return resultCode;
                }

                // Retrieve the Mz Refinery parameter file
                currentTask = "Retrieve the Mz Refinery parameter file " + mzRefParamFile;

                const string paramFileStoragePathKeyName = Global.STEP_TOOL_PARAM_FILE_STORAGE_PATH_PREFIX + "Mz_Refinery";

                var mzRefineryParamFileStoragePath = mMgrParams.GetParam(paramFileStoragePathKeyName);

                if (string.IsNullOrWhiteSpace(mzRefineryParamFileStoragePath))
                {
                    mzRefineryParamFileStoragePath = @"\\gigasax\dms_parameter_Files\MzRefinery";
                    LogWarning("Parameter '" + paramFileStoragePathKeyName + "' is not defined " +
                        "(obtained using V_Pipeline_Step_Tool_Storage_Paths in the Broker DB); " +
                        "will assume: " + mzRefineryParamFileStoragePath);
                }

                if (!FileSearchTool.RetrieveFile(mzRefParamFile, mzRefineryParamFileStoragePath))
                {
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                // Look for existing MS-GF+ results in the transfer folder
                currentTask = "Find existing MS-GF+ results";

                if (!FindExistingMSGFPlusResults(mzRefParamFile))
                {
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                mMessage = "Error in GetResources: " + ex.Message;
                LogError(mMessage + "; task = " + currentTask + "; " + Global.GetExceptionStackTrace(ex));
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private CloseOutType GetCDTAFile()
        {
            // Retrieve the _DTA.txt file
            // Note that if the file was found in MyEMSL, RetrieveDtaFiles will auto-call ProcessMyEMSLDownloadQueue to download the file

            if (FileSearchTool.RetrieveDtaFiles())
            {
                mJobParams.AddResultFileToSkip(DatasetName + CDTA_ZIPPED_EXTENSION);
                mJobParams.AddResultFileToSkip(DatasetName + CDTA_EXTENSION);

                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            var sharedResultsFolders = mJobParams.GetParam(JOB_PARAM_SHARED_RESULTS_FOLDERS);

            if (string.IsNullOrEmpty(sharedResultsFolders))
            {
                mMessage = Global.AppendToComment(mMessage, "Job parameter SharedResultsFolders is empty");
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            if (sharedResultsFolders.Contains(","))
            {
                mMessage = Global.AppendToComment(mMessage, "shared results folders: " + sharedResultsFolders);
            }
            else
            {
                mMessage = Global.AppendToComment(mMessage, "shared results folder " + sharedResultsFolders);
            }

            // Errors were reported in method call, so just return
            return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
        }

        /// <summary>
        /// Check for existing MS-GF+ results in the transfer directory
        /// </summary>
        /// <remarks>Will return true even if existing results are not found</remarks>
        /// <returns>True if no errors, false if a problem</returns>
        private bool FindExistingMSGFPlusResults(string mzRefParamFileName)
        {
            var resultsFolderName = mJobParams.GetParam(JOB_PARAM_OUTPUT_FOLDER_NAME);
            var transferDirectoryPath = mJobParams.GetParam(JOB_PARAM_TRANSFER_DIRECTORY_PATH);

            if (string.IsNullOrWhiteSpace(resultsFolderName))
            {
                mMessage = "Results folder not defined (job parameter OutputFolderName)";
                LogError(mMessage);
                return false;
            }

            if (string.IsNullOrWhiteSpace(transferDirectoryPath))
            {
                mMessage = "Transfer folder not defined (job parameter transferDirectoryPath)";
                LogError(mMessage);
                return false;
            }

            var transferDirectory = new DirectoryInfo(Path.Combine(transferDirectoryPath, DatasetName, resultsFolderName));

            if (!transferDirectory.Exists)
            {
                // This is not an error -- it just means there are no existing MS-GF+ results to use
                return true;
            }

            // Look for the required files in the transfer directory
            var resultsFileName = DatasetName + AnalysisToolRunnerMzRefinery.MSGFPLUS_MZID_SUFFIX + ".gz";
            var msgfPlusResults = new FileInfo(Path.Combine(transferDirectory.FullName, resultsFileName));

            if (!msgfPlusResults.Exists)
            {
                // This is not an error -- it just means there are no existing MS-GF+ results to use
                return true;
            }

            var msgfPlusConsoleOutput = new FileInfo(Path.Combine(transferDirectory.FullName, "MSGFPlus_ConsoleOutput.txt"));

            if (!msgfPlusConsoleOutput.Exists)
            {
                // This is unusual; typically if the mzid.gz file exists there should be a ConsoleOutput file
                // However, this file isn't required so we'll still use the existing results
                LogWarning("Found " + msgfPlusResults.FullName + " but did not find " + msgfPlusConsoleOutput.Name);
            }

            var mzRefParamFile = new FileInfo(Path.Combine(transferDirectory.FullName, mzRefParamFileName));

            if (!mzRefParamFile.Exists)
            {
                // This is unusual; typically if the mzid.gz file exists there should be a MzRefinery parameter file
                LogWarning("Found " + msgfPlusResults.FullName + " but did not find " + mzRefParamFile.Name + "; will re-run MS-GF+");
                return true;
            }

            // Compare the remote parameter file and the local one to make sure they match

            // The MS-GF+ tool runner will auto-add these two parameters if not in the source file (see method GetMSGFPlusParameters)
            // They will thus likely be in the MzRefinery parameter file in the working directory but not in the remote file
            // Ignore these lines when checking if two files match
            var lineIgnoreRegExSpecs = new List<Regex> {
                new("^MinNumPeaksPerSpectrum=5", RegexOptions.Compiled),
                new("^AddFeatures=1", RegexOptions.Compiled),
                new("^NumThreads=", RegexOptions.Compiled)
            };

            if (!Global.TextFilesMatch(
                    mzRefParamFile.FullName,
                    Path.Combine(mWorkDir, mzRefParamFileName),
                    true,
                    lineIgnoreRegExSpecs))
            {
                LogMessage("MzRefinery parameter file in transfer folder does not match the official MzRefinery parameter file; will re-run MS-GF+");
                return true;
            }

            // Existing results found
            // Copy the MS-GF+ results locally
            var localFilePath = Path.Combine(mWorkDir, msgfPlusResults.Name);
            msgfPlusResults.CopyTo(localFilePath, true);

            GUnzipFile(localFilePath);

            if (msgfPlusConsoleOutput.Exists)
            {
                localFilePath = Path.Combine(mWorkDir, msgfPlusConsoleOutput.Name);
                msgfPlusConsoleOutput.CopyTo(localFilePath, true);
            }

            LogMessage("Found existing MS-GF+ results to use for MzRefinery");

            return true;
        }
    }
}
