//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2008, Battelle Memorial Institute
// Created 07/08/2008
//
//*********************************************************************************************************

using System.IO;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace DTASpectraFileGen
{
    /// <summary>
    /// Gets resources necessary for DTA creation
    /// </summary>
    public class DtaGenResources : AnalysisResources
    {
        /// <summary>
        /// Text used to track that existing DeconMSn results are being used
        /// </summary>
        public const string USING_EXISTING_DECONMSN_RESULTS = "Using_existing_DeconMSn_Results";

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

            var rawDataTypeName = mJobParams.GetJobParameter("RawDataType", "");
            var mgfInstrumentData = mJobParams.GetJobParameter("MGFInstrumentData", false);

            var zippedDTAFilePath = string.Empty;

            var dtaGeneratorType = DtaGenToolRunner.GetDTAGeneratorInfo(mJobParams, out var errorMessage);

            if (dtaGeneratorType == DtaGenToolRunner.DTAGeneratorConstants.Unknown)
            {
                if (string.IsNullOrEmpty(errorMessage))
                {
                    LogError("GetDTAGeneratorInfo reported an Unknown DTAGenerator type");
                }
                else
                {
                    LogError(errorMessage);
                }

                return CloseOutType.CLOSEOUT_NO_SETTINGS_FILE;
            }

            if (!GetParameterFiles(dtaGeneratorType))
            {
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            if (mgfInstrumentData)
            {
                var fileToFind = DatasetName + DOT_MGF_EXTENSION;

                if (!FileSearchTool.FindAndRetrieveMiscFiles(fileToFind, false))
                {
                    LogError("Instrument data not found: " + fileToFind);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mJobParams.AddResultFileExtensionToSkip(DOT_MGF_EXTENSION);
            }
            else
            {
                // Get input data file
                if (!FileSearchTool.RetrieveSpectra(rawDataTypeName))
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        LogError("Error retrieving instrument data file");
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                bool centroidDTAs;

                if (dtaGeneratorType == DtaGenToolRunner.DTAGeneratorConstants.DeconConsole)
                {
                    centroidDTAs = false;
                }
                else
                {
                    centroidDTAs = mJobParams.GetJobParameter("CentroidDTAs", false);
                }

                if (centroidDTAs)
                {
                    // Look for a DTA_Gen_1_26_ folder for this dataset
                    // If it exists, and if we can find a valid _dta.zip file, use that file instead of re-running DeconMSn (since DeconMSn can take some time to run)

                    var datasetID = mJobParams.GetJobParameter("DatasetID", 0);
                    var directoryNameToFind = "DTA_Gen_1_26_" + datasetID;
                    var fileToFind = DatasetName + CDTA_ZIPPED_EXTENSION;

                    var existingDtDirectory = DirectorySearchTool.FindValidDirectory(DatasetName,
                        fileToFind,
                        directoryNameToFind,
                        maxAttempts: 1,
                        logDirectoryNotFound: false,
                        retrievingInstrumentDataDir: false,
                        assumeUnpurged: false,
                        validDirectoryFound: out var validFolderFound,
                        directoryNotFoundMessage: out _,
                        myEmslFileIDsInBestPath: out _);

                    if (validFolderFound)
                    {
                        // Copy the file locally (or queue it for download from MyEMSL)

                        var fileCopiedOrQueued = CopyFileToWorkDir(fileToFind, existingDtDirectory, mWorkDir);

                        if (fileCopiedOrQueued)
                        {
                            zippedDTAFilePath = Path.Combine(mWorkDir, fileToFind);

                            mJobParams.AddAdditionalParameter(AnalysisJob.JOB_PARAMETERS_SECTION, USING_EXISTING_DECONMSN_RESULTS, "True");

                            LogMessage("Found pre-existing DeconMSn results; will not re-run DeconMSn if they are valid");

                            fileToFind = DatasetName + "_profile.txt";
                            CopyFileToWorkDir(fileToFind, existingDtDirectory, mWorkDir);

                            fileToFind = DatasetName + "_DeconMSn_log.txt";
                            CopyFileToWorkDir(fileToFind, existingDtDirectory, mWorkDir);
                        }
                    }
                }
            }

            if (!ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!string.IsNullOrEmpty(zippedDTAFilePath))
            {
                var zippedDtaFile = new FileInfo(zippedDTAFilePath);
                var tempZipFilePath = Path.Combine(mWorkDir, Path.GetFileNameWithoutExtension(zippedDtaFile.Name) + "_PreExisting.zip");

                zippedDtaFile.MoveTo(tempZipFilePath);

                LogMessage("Unzipping file " + Path.GetFileName(zippedDTAFilePath));

                if (UnzipFileStart(tempZipFilePath, mWorkDir, "DtaGenResources"))
                {
                    if (mDebugLevel >= 1)
                    {
                        LogMessage("Unzipped file " + Path.GetFileName(zippedDTAFilePath));
                    }

                    mJobParams.AddResultFileToSkip(Path.GetFileName(tempZipFilePath));
                }
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private bool GetParameterFiles(DtaGenToolRunner.DTAGeneratorConstants eDtaGeneratorType)
        {
            if (eDtaGeneratorType == DtaGenToolRunner.DTAGeneratorConstants.DeconConsole)
            {
                const string paramFileStoragePathKeyName = Global.STEP_TOOL_PARAM_FILE_STORAGE_PATH_PREFIX + "DTA_Gen";

                var paramFileStoragePath = mMgrParams.GetParam(paramFileStoragePathKeyName);

                if (string.IsNullOrEmpty(paramFileStoragePath))
                {
                    paramFileStoragePath = @"\\gigasax\DMS_Parameter_Files\DTA_Gen";
                    LogWarning(
                        "Parameter '" + paramFileStoragePathKeyName +
                        "' is not defined (obtained using V_Pipeline_Step_Tool_Storage_Paths in the Broker DB); will assume: " +
                        paramFileStoragePath);
                }

                var paramFileName = mJobParams.GetJobParameter("DtaGenerator", "DeconMSn_ParamFile", string.Empty);

                if (string.IsNullOrEmpty(paramFileName))
                {
                    LogError(AnalysisToolRunnerBase.NotifyMissingParameter(mJobParams, "DeconMSn_ParamFile"));
                    return false;
                }

                if (!FileSearchTool.RetrieveFile(paramFileName, paramFileStoragePath))
                {
                    return false;
                }
            }

            return true;
        }
    }
}
