using System;
using System.IO;
using System.Xml;
using AnalysisManagerBase;
using MyEMSLReader;

namespace AnalysisManagerDtaRefineryPlugIn
{
    /// <summary>
    /// Retrieve resources for the DTA Refinery plugin
    /// </summary>
    public class clsAnalysisResourcesDtaRefinery : clsAnalysisResources
    {
        internal const string XTANDEM_DEFAULT_INPUT_FILE = "xtandem_default_input.xml";
        internal const string XTANDEM_TAXONOMY_LIST_FILE = "xtandem_taxonomy_list.xml";
        internal const string DTA_REFINERY_INPUT_FILE = "DtaRefinery_input.xml";

        /// <summary>
        /// Initialize options
        /// </summary>
        public override void Setup(string stepToolName, IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, clsMyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(stepToolName, mgrParams, jobParams, statusTools, myEMSLUtilities);
            SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, true);
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

            // Retrieve Fasta file
            var orgDbDirectoryPath = m_mgrParams.GetParam("orgdbdir");
            if (!RetrieveOrgDB(orgDbDirectoryPath, out var resultCode))
                return resultCode;

            // This will eventually be replaced by Ken Auberry dll call to make param file on the fly

            LogMessage("Getting param file");

            // Retrieve param file
            var strParamFileName = m_jobParams.GetParam("ParmFileName");

            if (!RetrieveGeneratedParamFile(strParamFileName))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var strParamFileStoragePathKeyName = clsGlobal.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX + "DTA_Refinery";

            var strDtaRefineryParmFileStoragePath = m_mgrParams.GetParam(strParamFileStoragePathKeyName);
            if (string.IsNullOrEmpty(strDtaRefineryParmFileStoragePath))
            {
                strDtaRefineryParmFileStoragePath = @"\\gigasax\dms_parameter_Files\DTARefinery";
                LogErrorToDatabase("Parameter '" + strParamFileStoragePathKeyName +
                    "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); " +
                    "will assume: " + strDtaRefineryParmFileStoragePath);
            }

            // Retrieve settings files aka default file that will have values overwritten by parameter file values
            // Stored in same location as parameter file
            if (!FileSearch.RetrieveFile(XTANDEM_DEFAULT_INPUT_FILE, strDtaRefineryParmFileStoragePath))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!FileSearch.RetrieveFile(XTANDEM_TAXONOMY_LIST_FILE, strDtaRefineryParmFileStoragePath))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!FileSearch.RetrieveFile(m_jobParams.GetParam("DTARefineryXMLFile"), strDtaRefineryParmFileStoragePath))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Retrieve the _DTA.txt file
            // Note that if the file was found in MyEMSL then RetrieveDtaFiles will auto-call ProcessMyEMSLDownloadQueue to download the file
            if (!FileSearch.RetrieveDtaFiles())
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Make sure the _DTA.txt file has parent ion lines with text: scan=x and cs=y
            var strCDTAPath = Path.Combine(m_WorkingDir, DatasetName + "_dta.txt");
            const bool blnReplaceSourceFile = true;
            const bool blnDeleteSourceFileIfUpdated = true;

            if (!ValidateCDTAFileScanAndCSTags(strCDTAPath, blnReplaceSourceFile, blnDeleteSourceFileIfUpdated, ""))
            {
                m_message = "Error validating the _DTA.txt file";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // If the _dta.txt file is over 2 GB in size, condense it
            if (!ValidateCDTAFileSize(m_WorkingDir, Path.GetFileName(strCDTAPath)))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Retrieve DeconMSn Log file and DeconMSn Profile File
            if (!RetrieveDeconMSnLogFiles())
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!ProcessMyEMSLDownloadQueue(m_WorkingDir, Downloader.DownloadFolderLayout.FlatNoSubfolders))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // If this is a MSGFPlus script, make sure that the spectra are centroided
            var toolName = m_jobParams.GetParam("ToolName");
            if (toolName.StartsWith("MSGFPlus", StringComparison.InvariantCultureIgnoreCase))
            {
                LogMessage(
                    "Validating that the _dta.txt file has centroided spectra (required by MSGF+)");
                if (!ValidateCDTAFileIsCentroided(strCDTAPath))
                {
                    // m_message is already updated
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            // Add all the extensions of the files to delete after run
            m_jobParams.AddResultFileExtensionToSkip("_dta.zip");    // Zipped DTA
            m_jobParams.AddResultFileExtensionToSkip("_dta.txt");    // Unzipped, concatenated DTA
            m_jobParams.AddResultFileExtensionToSkip(".dta");        // DTA files
            m_jobParams.AddResultFileExtensionToSkip(DatasetName + ".xml");

            m_jobParams.AddResultFileToSkip(strParamFileName);
            m_jobParams.AddResultFileToSkip(Path.GetFileNameWithoutExtension(strParamFileName) + "_ModDefs.txt");
            m_jobParams.AddResultFileToSkip("Mass_Correction_Tags.txt");

            m_jobParams.AddResultFileToKeep(DatasetName + "_dta.zip");

            // Set up run parameter file to reference spectra file, taxonomy file, and analysis parameter file
            string strErrorMessage;
            var success = UpdateParameterFile(out strErrorMessage);
            if (!success)
            {
                var msg = "clsAnalysisResourcesDtaRefinery.GetResources(), failed making input file: " + strErrorMessage;
                LogError(msg);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private bool RetrieveDeconMSnLogFiles()
        {
            try
            {
                var deconMSnLogFileName = DatasetName + "_DeconMSn_log.txt";
                var sourceFolderPath = FileSearch.FindDataFile(deconMSnLogFileName);

                if (string.IsNullOrWhiteSpace(sourceFolderPath))
                {
                    // Could not find the file (error will have already been logged)
                    // We'll continue on, but log a warning
                    if (m_DebugLevel >= 1)
                    {
                        LogWarning(
                            "Could not find the DeconMSn Log file named " + deconMSnLogFileName);
                    }
                    deconMSnLogFileName = string.Empty;
                }
                else
                {
                    if (!CopyFileToWorkDir(deconMSnLogFileName, sourceFolderPath, m_WorkingDir))
                    {
                        // Error copying file (error will have already been logged)
                        if (m_DebugLevel >= 3)
                        {
                            LogError("CopyFileToWorkDir returned False for " + deconMSnLogFileName + " using folder " + sourceFolderPath);
                        }
                        // Ignore the error and continue
                    }
                }

                var deconMSnProfileFileName = DatasetName + "_profile.txt";
                sourceFolderPath = FileSearch.FindDataFile(deconMSnProfileFileName);

                if (string.IsNullOrWhiteSpace(sourceFolderPath))
                {
                    // Could not find the file (error will have already been logged)
                    // We'll continue on, but log a warning
                    if (m_DebugLevel >= 1)
                    {
                        LogWarning(
                            "Could not find the DeconMSn Profile file named " + deconMSnProfileFileName);
                    }
                    deconMSnProfileFileName = string.Empty;
                }
                else
                {
                    if (!CopyFileToWorkDir(deconMSnProfileFileName, sourceFolderPath, m_WorkingDir))
                    {
                        // Error copying file (error will have already been logged)
                        if (m_DebugLevel >= 3)
                        {
                            LogError("CopyFileToWorkDir returned False for " + deconMSnProfileFileName + " using folder " + sourceFolderPath);
                        }
                        // Ignore the error and continue
                    }
                }

                if (!ProcessMyEMSLDownloadQueue(m_WorkingDir, Downloader.DownloadFolderLayout.FlatNoSubfolders))
                {
                    return false;
                }

                if (!string.IsNullOrWhiteSpace(deconMSnLogFileName))
                {
                    if (!ValidateDeconMSnLogFile(Path.Combine(m_WorkingDir, deconMSnLogFileName)))
                    {
                        return false;
                    }
                }

                DeleteFileIfNoData(deconMSnLogFileName, "DeconMSn Log file");

                DeleteFileIfNoData(deconMSnProfileFileName, "DeconMSn Profile file");

                // Make sure the DeconMSn files are not stored in the DTARefinery results folder
                m_jobParams.AddResultFileExtensionToSkip("_DeconMSn_log.txt");
                m_jobParams.AddResultFileExtensionToSkip("_profile.txt");
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

            var strFilePathToCheck = Path.Combine(m_WorkingDir, fileName);
            string strErrorMessage;
            if (!ValidateFileHasData(strFilePathToCheck, fileDescription, out strErrorMessage))
            {
                if (m_DebugLevel >= 1)
                {
                    LogWarning(
                        fileDescription +
                        " does not have any tab-delimited lines that start with a number; file will be deleted so that DTARefinery can proceed without considering TIC or ion intensity");
                }

                File.Delete(strFilePathToCheck);
            }
        }

        private bool UpdateParameterFile(out string strErrorMessage)
        {
            var XtandemDefaultInput = Path.Combine(m_WorkingDir, XTANDEM_DEFAULT_INPUT_FILE);
            var XtandemTaxonomyList = Path.Combine(m_WorkingDir, XTANDEM_TAXONOMY_LIST_FILE);
            var ParamFilePath = Path.Combine(m_WorkingDir, m_jobParams.GetParam("DTARefineryXMLFile"));
            var DtaRefineryDirectory = Path.GetDirectoryName(m_mgrParams.GetParam("dtarefineryloc"));

            strErrorMessage = string.Empty;

            try
            {
                var fiTemplateFile = new FileInfo(ParamFilePath);

                if (!fiTemplateFile.Exists)
                {
                    strErrorMessage = "File not found: " + fiTemplateFile.FullName;
                    return false;
                }

                if (string.IsNullOrWhiteSpace(DtaRefineryDirectory))
                {
                    strErrorMessage = "Manager parameter dtarefineryloc is empty";
                    return false;
                }

                // Open the template XML file
                var objTemplate = new XmlDocument {
                    PreserveWhitespace = true
                };

                try
                {
                    objTemplate.Load(fiTemplateFile.FullName);
                }
                catch (Exception ex)
                {
                    strErrorMessage = "Error loading file " + fiTemplateFile.Name + ": " + ex.Message;
                    return false;
                }

                // Now override the values for xtandem parameters file
                try
                {
                    var root = objTemplate.DocumentElement;

                    var XTandemExePath = Path.Combine(DtaRefineryDirectory, "aux_xtandem_module\\tandem_5digit_precision.exe");

                    if (root != null)
                    {
                        var exeParam = root.SelectSingleNode("/allPars/xtandemPars/par[@label='xtandem exe file']");
                        if (exeParam != null)
                            exeParam.InnerXml = XTandemExePath;

                        var inputParam = root.SelectSingleNode("/allPars/xtandemPars/par[@label='default input']");
                        if (inputParam != null)
                            inputParam.InnerXml = XtandemDefaultInput;

                        var taxonomyListParam = root.SelectSingleNode("/allPars/xtandemPars/par[@label='taxonomy list']");
                        if (taxonomyListParam != null)
                            taxonomyListParam.InnerXml = XtandemTaxonomyList;
                    }
                }
                catch (Exception ex)
                {
                    strErrorMessage = "Error updating the MSInFile nodes: " + ex.Message;
                    return false;
                }

                // Write out the new file
                objTemplate.Save(ParamFilePath);
            }
            catch (Exception ex)
            {
                strErrorMessage = "Error: " + ex.Message;
                return false;
            }

            return true;
        }

        private bool ValidateDeconMSnLogFile(string strFilePath)
        {
            var oValidator = new clsDeconMSnLogFileValidator();
            RegisterEvents(oValidator);

            var blnSuccess = oValidator.ValidateDeconMSnLogFile(strFilePath);
            if (!blnSuccess)
            {
                // The error will have already been logged
                return false;
            }

            if (oValidator.FileUpdated)
            {
                LogWarning("clsDeconMSnLogFileValidator.ValidateFile updated one or more rows " +
                           "in the DeconMSn_Log.txt file to replace values with intensities of 0 with 1");
            }

            return true;
        }
    }
}
