using System;
using System.IO;
using System.Xml;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.StatusReporting;
using MyEMSLReader;

namespace AnalysisManagerDtaRefineryPlugIn
{
    /// <summary>
    /// Retrieve resources for the DTA Refinery plugin
    /// </summary>
    public class AnalysisResourcesDtaRefinery : AnalysisResources
    {
        // Ignore Spelling: centroided, xtandem

        internal const string XTANDEM_DEFAULT_INPUT_FILE = "xtandem_default_input.xml";
        internal const string XTANDEM_TAXONOMY_LIST_FILE = "xtandem_taxonomy_list.xml";
        internal const string DTA_REFINERY_INPUT_FILE = "DtaRefinery_input.xml";

        /// <summary>
        /// Initialize options
        /// </summary>
        public override void Setup(string stepToolName, IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, MyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(stepToolName, mgrParams, jobParams, statusTools, myEMSLUtilities);
            SetOption(Global.AnalysisResourceOptions.OrgDbRequired, true);
        }

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

            // Retrieve FASTA file
            var orgDbDirectoryPath = mMgrParams.GetParam("OrgDbDir");

            if (!RetrieveOrgDB(orgDbDirectoryPath, out var resultCode))
                return resultCode;

            LogMessage("Getting param file");

            // Retrieve param file
            var paramFileName = mJobParams.GetParam("ParamFileName");

            if (!RetrieveGeneratedParamFile(paramFileName))
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            const string paramFileStoragePathKeyName = Global.STEP_TOOL_PARAM_FILE_STORAGE_PATH_PREFIX + "DTA_Refinery";

            var dtaRefineryParamFileStoragePath = mMgrParams.GetParam(paramFileStoragePathKeyName);

            if (string.IsNullOrEmpty(dtaRefineryParamFileStoragePath))
            {
                dtaRefineryParamFileStoragePath = @"\\gigasax\dms_parameter_Files\DTARefinery";
                LogErrorToDatabase("Parameter '" + paramFileStoragePathKeyName +
                    "' is not defined (obtained using V_Pipeline_Step_Tool_Storage_Paths in the Broker DB); " +
                    "will assume: " + dtaRefineryParamFileStoragePath);
            }

            // Retrieve settings files aka default file that will have values overwritten by parameter file values
            // Stored in same location as parameter file
            if (!FileSearchTool.RetrieveFile(XTANDEM_DEFAULT_INPUT_FILE, dtaRefineryParamFileStoragePath))
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            if (!FileSearchTool.RetrieveFile(XTANDEM_TAXONOMY_LIST_FILE, dtaRefineryParamFileStoragePath))
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            if (!FileSearchTool.RetrieveFile(mJobParams.GetParam("DTARefineryXMLFile"), dtaRefineryParamFileStoragePath))
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // Retrieve the _DTA.txt file
            // Note that if the file was found in MyEMSL then RetrieveDtaFiles will auto-call ProcessMyEMSLDownloadQueue to download the file
            if (!FileSearchTool.RetrieveDtaFiles())
            {
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // Make sure the _DTA.txt file has parent ion lines with text: scan=x and cs=y
            var cdtaPath = Path.Combine(mWorkDir, DatasetName + "_dta.txt");
            const bool replaceSourceFile = true;
            const bool deleteSourceFileIfUpdated = true;

            if (!ValidateCDTAFileScanAndCSTags(cdtaPath, replaceSourceFile, deleteSourceFileIfUpdated, ""))
            {
                mMessage = "Error validating the _DTA.txt file";
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // If the _dta.txt file is over 2 GB in size, condense it
            if (!ValidateCDTAFileSize(mWorkDir, Path.GetFileName(cdtaPath)))
            {
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // Retrieve DeconMSn Log file and DeconMSn Profile File
            if (!RetrieveDeconMSnLogFiles())
            {
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            if (!ProcessMyEMSLDownloadQueue(mWorkDir, Downloader.DownloadLayout.FlatNoSubdirectories))
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // If this is a MSGFPlus script, make sure that the spectra are centroided
            // The ToolName job parameter holds the name of the job script we are executing
            var scriptName = mJobParams.GetParam("ToolName");

            if (scriptName.StartsWith("MSGFPlus", StringComparison.OrdinalIgnoreCase))
            {
                LogMessage(
                    "Validating that the _dta.txt file has centroided spectra (required by MS-GF+)");

                if (!ValidateCDTAFileIsCentroided(cdtaPath))
                {
                    // mMessage is already updated
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }
            }

            // Add all the extensions of the files to delete after run
            mJobParams.AddResultFileExtensionToSkip("_dta.zip");    // Zipped DTA
            mJobParams.AddResultFileExtensionToSkip("_dta.txt");    // Unzipped, concatenated DTA
            mJobParams.AddResultFileExtensionToSkip(".dta");        // DTA files
            mJobParams.AddResultFileExtensionToSkip(DatasetName + ".xml");

            mJobParams.AddResultFileToSkip(paramFileName);
            mJobParams.AddResultFileToSkip(Path.GetFileNameWithoutExtension(paramFileName) + "_ModDefs.txt");
            mJobParams.AddResultFileToSkip("Mass_Correction_Tags.txt");

            mJobParams.AddResultFileToKeep(DatasetName + "_dta.zip");

            // Set up run parameter file to reference spectra file, taxonomy file, and analysis parameter file
            var success = UpdateParameterFile(out var errorMessage);

            if (!success)
            {
                LogError(string.Format(
                    "AnalysisResourcesDtaRefinery.GetResources(), failed making input file: {0}", errorMessage));

                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private bool RetrieveDeconMSnLogFiles()
        {
            try
            {
                var deconMSnLogFileName = DatasetName + "_DeconMSn_log.txt";
                var sourceFolderPath = FileSearchTool.FindDataFile(deconMSnLogFileName);

                if (string.IsNullOrWhiteSpace(sourceFolderPath))
                {
                    // Could not find the file (error will have already been logged)
                    // We'll continue on, but log a warning
                    if (mDebugLevel >= 1)
                    {
                        LogWarning(
                            "Could not find the DeconMSn Log file named " + deconMSnLogFileName);
                    }
                    deconMSnLogFileName = string.Empty;
                }
                else
                {
                    if (!CopyFileToWorkDir(deconMSnLogFileName, sourceFolderPath, mWorkDir))
                    {
                        // Error copying file (error will have already been logged)
                        if (mDebugLevel >= 3)
                        {
                            LogError("CopyFileToWorkDir returned false for " + deconMSnLogFileName + " using directory " + sourceFolderPath);
                        }
                        // Ignore the error and continue
                    }
                }

                var deconMSnProfileFileName = DatasetName + "_profile.txt";
                sourceFolderPath = FileSearchTool.FindDataFile(deconMSnProfileFileName);

                if (string.IsNullOrWhiteSpace(sourceFolderPath))
                {
                    // Could not find the file (error will have already been logged)
                    // We'll continue on, but log a warning
                    if (mDebugLevel >= 1)
                    {
                        LogWarning(
                            "Could not find the DeconMSn Profile file named " + deconMSnProfileFileName);
                    }
                    deconMSnProfileFileName = string.Empty;
                }
                else
                {
                    if (!CopyFileToWorkDir(deconMSnProfileFileName, sourceFolderPath, mWorkDir))
                    {
                        // Error copying file (error will have already been logged)
                        if (mDebugLevel >= 3)
                        {
                            LogError("CopyFileToWorkDir returned false for " + deconMSnProfileFileName + " using directory " + sourceFolderPath);
                        }
                        // Ignore the error and continue
                    }
                }

                if (!ProcessMyEMSLDownloadQueue(mWorkDir, Downloader.DownloadLayout.FlatNoSubdirectories))
                {
                    return false;
                }

                if (!string.IsNullOrWhiteSpace(deconMSnLogFileName))
                {
                    if (!ValidateDeconMSnLogFile(Path.Combine(mWorkDir, deconMSnLogFileName)))
                    {
                        return false;
                    }
                }

                DeleteFileIfNoData(deconMSnLogFileName, "DeconMSn Log file");

                DeleteFileIfNoData(deconMSnProfileFileName, "DeconMSn Profile file");

                // Make sure the DeconMSn files are not stored in the DTARefinery results folder
                mJobParams.AddResultFileExtensionToSkip("_DeconMSn_log.txt");
                mJobParams.AddResultFileExtensionToSkip("_profile.txt");
            }
            catch (Exception ex)
            {
                LogError("Error in RetrieveDeconMSnLogFiles: " + ex.Message);
                return false;
            }

            return true;
        }

        private void DeleteFileIfNoData(string fileName, string fileDescription)
        {
            if (string.IsNullOrWhiteSpace(fileName))
                return;

            var filePathToCheck = Path.Combine(mWorkDir, fileName);

            if (!ValidateFileHasData(filePathToCheck, fileDescription, out _))
            {
                if (mDebugLevel >= 1)
                {
                    LogWarning(
                        fileDescription +
                        " does not have any tab-delimited lines that start with a number; file will be deleted so that DTARefinery can proceed without considering TIC or ion intensity");
                }

                File.Delete(filePathToCheck);
            }
        }

        private bool UpdateParameterFile(out string errorMessage)
        {
            var xtandemDefaultInput = Path.Combine(mWorkDir, XTANDEM_DEFAULT_INPUT_FILE);
            var xtandemTaxonomyList = Path.Combine(mWorkDir, XTANDEM_TAXONOMY_LIST_FILE);
            var paramFilePath = Path.Combine(mWorkDir, mJobParams.GetParam("DTARefineryXMLFile"));
            var dtaRefineryDirectory = Path.GetDirectoryName(mMgrParams.GetParam("DtaRefineryLoc"));

            errorMessage = string.Empty;

            try
            {
                var templateFile = new FileInfo(paramFilePath);

                if (!templateFile.Exists)
                {
                    errorMessage = "File not found: " + templateFile.FullName;
                    return false;
                }

                if (string.IsNullOrWhiteSpace(dtaRefineryDirectory))
                {
                    errorMessage = "Manager parameter DtaRefineryLoc is empty";
                    return false;
                }

                // Open the template XML file
                var template = new XmlDocument {
                    PreserveWhitespace = true
                };

                try
                {
                    template.Load(templateFile.FullName);
                }
                catch (Exception ex)
                {
                    errorMessage = "Error loading file " + templateFile.Name + ": " + ex.Message;
                    return false;
                }

                // Now override the values for xtandem parameters file
                try
                {
                    var root = template.DocumentElement;

                    var xtandemExePath = Path.Combine(dtaRefineryDirectory, @"aux_xtandem_module\tandem_5digit_precision.exe");

                    if (root != null)
                    {
                        var exeParam = root.SelectSingleNode("/allPars/xtandemPars/par[@label='xtandem exe file']");

                        if (exeParam != null)
                            exeParam.InnerXml = xtandemExePath;

                        var inputParam = root.SelectSingleNode("/allPars/xtandemPars/par[@label='default input']");

                        if (inputParam != null)
                            inputParam.InnerXml = xtandemDefaultInput;

                        var taxonomyListParam = root.SelectSingleNode("/allPars/xtandemPars/par[@label='taxonomy list']");

                        if (taxonomyListParam != null)
                            taxonomyListParam.InnerXml = xtandemTaxonomyList;
                    }
                }
                catch (Exception ex)
                {
                    errorMessage = "Error updating the MSInFile nodes: " + ex.Message;
                    return false;
                }

                // Write out the new file
                template.Save(paramFilePath);
            }
            catch (Exception ex)
            {
                errorMessage = "Error: " + ex.Message;
                return false;
            }

            return true;
        }

        private bool ValidateDeconMSnLogFile(string filePath)
        {
            var validator = new DeconMSnLogFileValidator();
            RegisterEvents(validator);

            var success = validator.ValidateDeconMSnLogFile(filePath);

            if (!success)
            {
                // The error will have already been logged
                return false;
            }

            if (validator.FileUpdated)
            {
                LogWarning("DeconMSnLogFileValidator.ValidateFile updated one or more rows " +
                           "in the DeconMSn_Log.txt file to replace values with intensities of 0 with 1");
            }

            return true;
        }
    }
}
