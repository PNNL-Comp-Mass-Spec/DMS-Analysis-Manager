/*****************************************************************
** Written by Matthew Monroe for the US Department of Energy    **
** Pacific Northwest National Laboratory, Richland, WA          **
** Created 05/24/2018                                           **
**                                                              **
*****************************************************************/

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml.Linq;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.OfflineJobs;
using PRISM;

namespace AnalysisManagerFormularityPlugin
{
    /// <summary>
    /// Retrieve resources for the Formularity plugin
    /// </summary>
    public class AnalysisResourcesFormularity : AnalysisResources
    {
        // Ignore Spelling: cia, Formularity

        public const string JOB_PARAM_FORMULARITY_CALIBRATION_PEAKS_FILE = "CalibrationPeaksFile";

        /// <summary>
        /// Job parameter used to track the input file for formularity
        /// </summary>
        public const string JOB_PARAM_FORMULARITY_DATASET_SCANS_FILE = "Formularity_DatasetScansFile";

        /// <summary>
        /// Retrieve required files
        /// </summary>
        /// <returns>Closeout code</returns>
        public override CloseOutType GetResources()
        {
            var currentTask = "Initializing";

            try
            {
                currentTask = "Retrieve shared resources";

                // Retrieve shared resources, including the JobParameters file from the previous job step
                var result = GetSharedResources();

                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                // Retrieve the parameter file
                currentTask = "Retrieve the parameter file";
                var paramFileName = mJobParams.GetParam(JOB_PARAM_PARAMETER_FILE);
                var paramFileStoragePath = mJobParams.GetParam("ParamFileStoragePath");

                if (!FileSearchTool.RetrieveFile(paramFileName, paramFileStoragePath))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Examine the parameter file
                // If calibration is enabled, retrieve the calibration peaks file
                var success = RetrieveCalibrationPeaksFile(paramFileStoragePath, paramFileName);

                if (!success)
                {
                    if (string.IsNullOrWhiteSpace(mMessage))
                    {
                        LogError("Error extracting calibration settings from the parameter file");
                    }
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }

                // Retrieve the database
                currentTask = "Retrieve the CIA database";
                var databaseFileName = mJobParams.GetParam("cia_db_name");

                if (string.IsNullOrWhiteSpace(databaseFileName))
                {
                    LogError("Parameter cia_db_name not found in the settings file");
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }

                var sourceDirectory = new DirectoryInfo(Path.Combine(paramFileStoragePath, "CIA_DB"));

                if (!sourceDirectory.Exists)
                {
                    LogError("CIA database directory not found: " + sourceDirectory.FullName);
                    return CloseOutType.CLOSEOUT_NO_FAS_FILES;
                }

                var fileSyncUtil = new FileSyncUtils(mFileTools);
                RegisterEvents(fileSyncUtil);

                var remoteCiaDbPath = Path.Combine(sourceDirectory.FullName, databaseFileName);
                var orgDbDirectory = mMgrParams.GetParam(MGR_PARAM_ORG_DB_DIR);

                if (string.IsNullOrWhiteSpace(orgDbDirectory))
                {
                    LogError(string.Format("Manager parameter {0} is not defined", MGR_PARAM_ORG_DB_DIR));
                    return CloseOutType.CLOSEOUT_NO_FAS_FILES;
                }

                const int recheckIntervalDays = 7;
                var ciaDbIsValid = fileSyncUtil.CopyFileToLocal(remoteCiaDbPath, orgDbDirectory, out var errorMessage, recheckIntervalDays);

                if (!ciaDbIsValid)
                {
                    if (string.IsNullOrEmpty(errorMessage))
                        LogError("Error copying remote CIA database locally");
                    else
                        LogError("Error copying remote CIA database locally: " + errorMessage);

                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // The June 2018 version of Formularity created a _VerifiedNoDuplicates file after it validated data loaded from the CIA database
                // The December 2018 version skips this validation

                /*

                // Also copy the _VerifiedNoDuplicates.txt file if it exists
                var noDuplicatesFile = Path.GetFileNameWithoutExtension(databaseFileName) + "_VerifiedNoDuplicates.txt";
                var remoteNoDuplicatesFile = new FileInfo(Path.Combine(sourceDirectory.FullName, noDuplicatesFile));
                var localNoDuplicatesFile = new FileInfo(Path.Combine(orgDbDirectory, noDuplicatesFile));

                if (!localNoDuplicatesFile.Exists && remoteNoDuplicatesFile.Exists)
                {
                    remoteNoDuplicatesFile.CopyTo(localNoDuplicatesFile.FullName, true);
                }

                */

                var rawDataTypeName = mJobParams.GetParam("rawDataType");

                // The ToolName job parameter holds the name of the job script we are executing
                var scriptName = mJobParams.GetParam("ToolName");

                switch (rawDataTypeName.ToLower())
                {
                    case RAW_DATA_TYPE_DOT_RAW_FILES:
                        // Processing a Thermo .raw file
                        // Retrieve the .tsv file created by the ThermoPeakDataExporter step
                        currentTask = "Retrieve the ThermoPeakDataExporter .tsv file";
                        var tsvFileName = DatasetName + ".tsv";

                        if (!FileSearchTool.FindAndRetrieveMiscFiles(tsvFileName, false))
                        {
                            // Errors should have already been logged
                            return CloseOutType.CLOSEOUT_FAILED;
                        }
                        mJobParams.AddResultFileToSkip(tsvFileName);
                        mJobParams.AddAdditionalParameter(
                            AnalysisJob.STEP_PARAMETERS_SECTION,
                            JOB_PARAM_FORMULARITY_DATASET_SCANS_FILE,
                            tsvFileName);

                        break;

                    case RAW_DATA_TYPE_BRUKER_FT_FOLDER:
                        // Processing a .D directory

                        if (scriptName.EndsWith("_Decon", StringComparison.OrdinalIgnoreCase))
                        {
                            // Formularity_Bruker_Decon
                            // The .D directory was processed by DeconTools to create a _peaks.txt file
                            // Retrieve the _peaks.zip file

                            currentTask = "Retrieve the DeconTools _peaks.zip file";
                            var peaksFileName = DatasetName + "_peaks.zip";

                            if (!FileSearchTool.FindAndRetrieveMiscFiles(peaksFileName, false))
                            {
                                // Errors should have already been logged
                                return CloseOutType.CLOSEOUT_FAILED;
                            }
                            mJobParams.AddResultFileToSkip(peaksFileName);
                            mJobParams.AddAdditionalParameter(
                                AnalysisJob.STEP_PARAMETERS_SECTION,
                                JOB_PARAM_FORMULARITY_DATASET_SCANS_FILE,
                                peaksFileName);

                            break;
                        }
                        else if (scriptName.Equals("Formularity_Bruker", StringComparison.OrdinalIgnoreCase))
                        {
                            // Retrieve the zip file that has the XML files from the Bruker_Data_Analysis step
                            currentTask = "Retrieve the Bruker_Data_Analysis _scans.zip file";
                            var scansFileName = DatasetName + "_scans.zip";

                            if (!FileSearchTool.FindAndRetrieveMiscFiles(scansFileName, false))
                            {
                                // Errors should have already been logged
                                return CloseOutType.CLOSEOUT_FAILED;
                            }
                            mJobParams.AddResultFileToSkip(scansFileName);
                            mJobParams.AddAdditionalParameter(
                                AnalysisJob.STEP_PARAMETERS_SECTION,
                                JOB_PARAM_FORMULARITY_DATASET_SCANS_FILE,
                                scansFileName);

                            break;
                        }
                        else
                        {
                            LogError("Unsupported pipeline script for Formularity: " + scriptName);
                            return CloseOutType.CLOSEOUT_FAILED;
                        }

                    default:
                        LogError("This tool is not compatible with datasets of type " + rawDataTypeName);
                        return CloseOutType.CLOSEOUT_FAILED;
                }

                currentTask = "Process the MyEMSL download queue";

                if (!ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in GetResources: " + ex.Message;
                LogError(mMessage + "; task = " + currentTask + "; " + Global.GetExceptionStackTrace(ex));

                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private bool RetrieveCalibrationPeaksFile(string paramFileStoragePath, string paramFileName)
        {
            try
            {
                var paramFilePath = Path.Combine(paramFileStoragePath, paramFileName);

                using var reader = new StreamReader(new FileStream(paramFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                // Note that XDocument supersedes XmlDocument and XPathDocument
                // XDocument can often be easier to use since XDocument is LINQ-based

                var doc = XDocument.Parse(reader.ReadToEnd());

                var calibrationSection = doc.Elements("DefaultParameters").Elements("InputFilesTab").Elements("Calibration").ToList();

                if (calibrationSection.Count == 0)
                {
                    LogError("Formularity parameter file does not have a Calibration section");
                    return false;
                }

                var regressionSetting = XMLUtils.GetXmlValue(calibrationSection, "Regression");

                if (string.IsNullOrWhiteSpace(regressionSetting))
                {
                    LogError("Regression setting is missing from the Calibration section of the parameter file");
                    return false;
                }

                // Validate that regressionSetting is a known value
                var allowedValues = new SortedSet<string>(StringComparer.Ordinal) {
                    "none",
                    "auto",
                    "linear",
                    "quadratic"
                };

                if (!allowedValues.Contains(regressionSetting))
                {
                    // Check for the word being correct, but not lowercase
                    foreach (var value in allowedValues)
                    {
                        if (string.Equals(value, regressionSetting, StringComparison.OrdinalIgnoreCase))
                        {
                            LogError("The Regression value in the Calibration section is {0}; it needs to be lowercase {1}", regressionSetting, value);
                            return false;
                        }
                    }

                    LogError("Invalid Regression value in the Calibration section: {0}; allowed values are {1}", regressionSetting, string.Join(", ", allowedValues));
                    return false;
                }

                if (regressionSetting.Equals("none", StringComparison.Ordinal))
                {
                    // Calibration is not enabled
                    LogDebug("Calibration is not enabled in Formularity parameter file " + paramFileName);
                    return true;
                }

                var calibrationPeaksFileName = XMLUtils.GetXmlValue(calibrationSection, "RefPeakFileName");

                if (string.IsNullOrWhiteSpace(calibrationPeaksFileName))
                {
                    LogError("Calibration is enabled in the Formularity parameter file, but RefPeakFileName is missing or empty");
                    return false;
                }

                var calibrationFilesDirPath = Path.Combine(paramFileStoragePath, "CalibrationFiles");
                var calibrationFilesDirectory = new DirectoryInfo(calibrationFilesDirPath);

                if (!calibrationFilesDirectory.Exists)
                {
                    LogError("Calibration files directory not found: " + calibrationFilesDirectory.FullName);
                    return false;
                }

                var calibrationPeaksFilePath = Path.Combine(calibrationFilesDirectory.FullName, calibrationPeaksFileName);
                var calibrationPeaksFile = new FileInfo(calibrationPeaksFilePath);

                if (!calibrationPeaksFile.Exists)
                {
                    LogError("Calibration peaks file not found: " + calibrationPeaksFile.FullName);
                    return false;
                }

                var localCalFilePath = Path.Combine(mWorkDir, calibrationPeaksFileName);
                calibrationPeaksFile.CopyTo(localCalFilePath, true);

                mJobParams.AddResultFileToSkip(calibrationPeaksFileName);
                mJobParams.AddAdditionalParameter(
                    AnalysisJob.STEP_PARAMETERS_SECTION,
                    JOB_PARAM_FORMULARITY_CALIBRATION_PEAKS_FILE,
                    calibrationPeaksFileName);

                return true;
            }
            catch (Exception ex)
            {
                LogError("Exception in RetrieveCalibrationPeaksFile", ex);
                return false;
            }
        }
    }
}
