//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2008, Battelle Memorial Institute
// Created 07/08/2008
//
//*********************************************************************************************************

using System.IO;
using AnalysisManagerBase;

namespace DTASpectraFileGen
{
    /// <summary>
    /// Gets resources necessary for DTA creation
    /// </summary>
    /// <remarks></remarks>
    public class clsDtaGenResources : clsAnalysisResources
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

            var strRawDataType = mJobParams.GetJobParameter("RawDataType", "");
            var blnMGFInstrumentData = mJobParams.GetJobParameter("MGFInstrumentData", false);

            var zippedDTAFilePath = string.Empty;

            var eDtaGeneratorType = clsDtaGenToolRunner.GetDTAGeneratorInfo(mJobParams, out var strErrorMessage);
            if (eDtaGeneratorType == clsDtaGenToolRunner.eDTAGeneratorConstants.Unknown)
            {
                if (string.IsNullOrEmpty(strErrorMessage))
                {
                    LogError("GetDTAGeneratorInfo reported an Unknown DTAGenerator type");
                }
                else
                {
                    LogError(strErrorMessage);
                }

                return CloseOutType.CLOSEOUT_NO_SETTINGS_FILE;
            }

            if (!GetParameterFiles(eDtaGeneratorType))
            {
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            if (blnMGFInstrumentData)
            {
                var strFileToFind = DatasetName + DOT_MGF_EXTENSION;
                if (!FileSearch.FindAndRetrieveMiscFiles(strFileToFind, false))
                {
                    LogError("Instrument data not found: " + strFileToFind);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mJobParams.AddResultFileExtensionToSkip(DOT_MGF_EXTENSION);
            }
            else
            {
                // Get input data file
                if (!FileSearch.RetrieveSpectra(strRawDataType))
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        LogError("Error retrieving instrument data file");
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                bool blnCentroidDTAs;
                if (eDtaGeneratorType == clsDtaGenToolRunner.eDTAGeneratorConstants.DeconConsole)
                {
                    blnCentroidDTAs = false;
                }
                else
                {
                    blnCentroidDTAs = mJobParams.GetJobParameter("CentroidDTAs", false);
                }

                if (blnCentroidDTAs)
                {
                    // Look for a DTA_Gen_1_26_ folder for this dataset
                    // If it exists, and if we can find a valid _dta.zip file, use that file instead of re-running DeconMSn (since DeconMSn can take some time to run)

                    var datasetID = mJobParams.GetJobParameter("DatasetID", 0);
                    var directoryNameToFind = "DTA_Gen_1_26_" + datasetID;
                    var fileToFind = DatasetName + CDTA_ZIPPED_EXTENSION;

                    var existingDtDirectory = DirectorySearch.FindValidFolder(DatasetName,
                        fileToFind,
                        directoryNameToFind,
                        maxAttempts: 1,
                        logDirectoryNotFound: false,
                        retrievingInstrumentDataDir: false,
                        assumeUnpurged: false,
                        validDirectoryFound: out var validFolderFound,
                        directoryNotFoundMessage: out _);

                    if (validFolderFound)
                    {
                        // Copy the file locally (or queue it for download from MyEMSL)

                        var blnFileCopiedOrQueued = CopyFileToWorkDir(fileToFind, existingDtDirectory, mWorkDir);

                        if (blnFileCopiedOrQueued)
                        {
                            zippedDTAFilePath = Path.Combine(mWorkDir, fileToFind);

                            mJobParams.AddAdditionalParameter(clsAnalysisJob.JOB_PARAMETERS_SECTION, USING_EXISTING_DECONMSN_RESULTS, "True");

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
                var fiZippedDtaFile = new FileInfo(zippedDTAFilePath);
                var tempZipFilePath = Path.Combine(mWorkDir, Path.GetFileNameWithoutExtension(fiZippedDtaFile.Name) + "_PreExisting.zip");

                fiZippedDtaFile.MoveTo(tempZipFilePath);

                LogMessage("Unzipping file " + Path.GetFileName(zippedDTAFilePath));

                if (UnzipFileStart(tempZipFilePath, mWorkDir, "clsDtaGenResources", false))
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

        private bool GetParameterFiles(clsDtaGenToolRunner.eDTAGeneratorConstants eDtaGeneratorType)
        {
            if (eDtaGeneratorType == clsDtaGenToolRunner.eDTAGeneratorConstants.DeconConsole)
            {
                var strParamFileStoragePathKeyName = clsGlobal.STEP_TOOL_PARAM_FILE_STORAGE_PATH_PREFIX + "DTA_Gen";

                var strParamFileStoragePath = mMgrParams.GetParam(strParamFileStoragePathKeyName);
                if (string.IsNullOrEmpty(strParamFileStoragePath))
                {
                    strParamFileStoragePath = @"\\gigasax\DMS_Parameter_Files\DTA_Gen";
                    LogWarning(
                        "Parameter '" + strParamFileStoragePathKeyName +
                        "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " +
                        strParamFileStoragePath);
                }

                var strParamFileName = mJobParams.GetJobParameter("DtaGenerator", "DeconMSn_ParamFile", string.Empty);

                if (string.IsNullOrEmpty(strParamFileName))
                {
                    LogError(clsAnalysisToolRunnerBase.NotifyMissingParameter(mJobParams, "DeconMSn_ParamFile"));
                    return false;
                }

                if (!FileSearch.RetrieveFile(strParamFileName, strParamFileStoragePath))
                {
                    return false;
                }
            }

            return true;
        }

    }
}
