//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
//
//*********************************************************************************************************

using System;
using System.IO;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using MyEMSLReader;
using PRISM;

namespace AnalysisManagerLCMSFeatureFinderPlugIn
{
    /// <summary>
    /// Retrieve resources for the LCMS Feature Finder plugin
    /// </summary>
    public class AnalysisResourcesLCMSFF : AnalysisResources
    {
        /// <summary>
        /// DeconTools _scans.csv file suffix
        /// </summary>
        public const string SCANS_FILE_SUFFIX = "_scans.csv";

        /// <summary>
        /// DeconTools _isos.csv file suffix
        /// </summary>
        public const string ISOS_FILE_SUFFIX = "_isos.csv";

        /// <summary>
        /// Retrieve required files
        /// </summary>
        /// <returns>Closeout code</returns>
        public override CloseOutType GetResources()
        {
            LogMessage("Retrieving required files");

            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            // Retrieve Decon2LS _scans.csv file for this dataset
            // The LCMSFeature Finder doesn't actually use the _scans.csv file, but we want to be sure it's present in the results folder
            var fileToGet = DatasetName + SCANS_FILE_SUFFIX;

            if (!FileSearchTool.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // Retrieve Decon2LS _isos.csv files for this dataset
            fileToGet = DatasetName + ISOS_FILE_SUFFIX;

            if (!FileSearchTool.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }
            mJobParams.AddResultFileToSkip(fileToGet);

            // Retrieve the LCMSFeatureFinder .Ini file specified for this job
            var lcmsFeatureFinderIniFileName = mJobParams.GetParam("LCMSFeatureFinderIniFile");

            if (string.IsNullOrEmpty(lcmsFeatureFinderIniFileName))
            {
                LogError("LCMSFeatureFinderIniFile not defined in the settings for this job; unable to continue");
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            const string paramFileStoragePathKeyName = Global.STEP_TOOL_PARAM_FILE_STORAGE_PATH_PREFIX + "LCMSFeatureFinder";
            var lcmsFeatureFinderIniFileStorageDirectory = mMgrParams.GetParam(paramFileStoragePathKeyName);

            if (string.IsNullOrEmpty(lcmsFeatureFinderIniFileStorageDirectory))
            {
                lcmsFeatureFinderIniFileStorageDirectory = @"\\gigasax\DMS_Parameter_Files\LCMSFeatureFinder";
                LogWarning(
                    "Parameter '" + paramFileStoragePathKeyName +
                    "' is not defined (obtained using V_Pipeline_Step_Tool_Storage_Paths in the Broker DB); will assume: " + lcmsFeatureFinderIniFileStorageDirectory);
            }

            if (!CopyFileToWorkDir(lcmsFeatureFinderIniFileName, lcmsFeatureFinderIniFileStorageDirectory, mWorkDir))
            {
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            var rawDataType = mJobParams.GetParam("RawDataType");

            if (rawDataType.ToLower() == RAW_DATA_TYPE_DOT_UIMF_FILES)
            {
                if (mDebugLevel >= 2)
                {
                    LogDebug("Retrieving .UIMF file");
                }

                // IMS data; need to get the .UIMF file
                if (!FileSearchTool.RetrieveSpectra(rawDataType))
                {
                    LogDebug("AnalysisResourcesDecon2ls.GetResources: Error occurred retrieving spectra.");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (mDebugLevel >= 1)
                {
                    LogDebug("Retrieved .UIMF file");
                }

                mJobParams.AddResultFileExtensionToSkip(DOT_UIMF_EXTENSION);
            }

            if (!ProcessMyEMSLDownloadQueue(mWorkDir, Downloader.DownloadLayout.FlatNoSubdirectories))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Could add an extension of a file to delete, like this:
            // mJobParams.AddResultFileExtensionToSkip(".dta")  'DTA files

            // Customize the LCMSFeatureFinder .Ini file to include the input file path and output folder path
            var success = UpdateFeatureFinderIniFile(lcmsFeatureFinderIniFileName);

            if (!success)
            {
                var message = "AnalysisResourcesLCMSFF.GetResources(), failed customizing .Ini file " + lcmsFeatureFinderIniFileName;

                if (string.IsNullOrEmpty(mMessage))
                {
                    mMessage = message;
                }
                LogError(message);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private string GetValue(string dataLine)
        {
            var value = string.Empty;

            if (!string.IsNullOrEmpty(dataLine))
            {
                var equalsIndex = dataLine.IndexOf('=');

                if (equalsIndex > 0 && equalsIndex < dataLine.Length - 1)
                {
                    value = dataLine.Substring(equalsIndex + 1);
                }
            }

            return value;
        }

        private bool UpdateFeatureFinderIniFile(string lcmsFeatureFinderIniFileName)
        {
            const string INPUT_FILENAME_KEY = "InputFileName";
            const string OUTPUT_DIRECTORY_KEY = "OutputDirectory";
            const string FILTER_FILE_NAME_KEY = "DeconToolsFilterFileName";

            // Read the source .Ini file and update the settings for InputFileName and OutputDirectory
            // In addition, look for an entry for DeconToolsFilterFileName;
            //  if present, verify that the file exists and copy it locally (so that it will be included in the results folder)

            var srcFilePath = Path.Combine(mWorkDir, lcmsFeatureFinderIniFileName);
            var targetFilePath = Path.Combine(mWorkDir, lcmsFeatureFinderIniFileName + "_new");
            var isosFilePath = Path.Combine(mWorkDir, DatasetName + ISOS_FILE_SUFFIX);

            var inputFileDefined = false;
            var outputDirectoryDefined = false;

            var result = true;

            try
            {
                // Create the output file (temporary name ending in "_new"; we'll swap the files later)
                using (var writer = new StreamWriter(new FileStream(targetFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    try
                    {
                        // Open the input file
                        using (var reader = new StreamReader(new FileStream(srcFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                        {
                            while (!reader.EndOfStream)
                            {
                                var dataLine = reader.ReadLine();

                                if (dataLine == null)
                                    continue;

                                var dataLineLCase = dataLine.ToLower().Trim();

                                if (dataLineLCase.StartsWith(INPUT_FILENAME_KEY.ToLower()))
                                {
                                    // Customize the input file name
                                    dataLine = INPUT_FILENAME_KEY + "=" + isosFilePath;
                                    inputFileDefined = true;
                                }

                                if (dataLineLCase.StartsWith(OUTPUT_DIRECTORY_KEY.ToLower()))
                                {
                                    // Customize the output directory name
                                    dataLine = OUTPUT_DIRECTORY_KEY + "=" + mWorkDir;
                                    outputDirectoryDefined = true;
                                }

                                if (dataLineLCase.StartsWith(FILTER_FILE_NAME_KEY.ToLower()))
                                {
                                    // Copy the file defined by DeconToolsFilterFileName= to the working directory

                                    var value = GetValue(dataLine);

                                    if (!string.IsNullOrEmpty(value))
                                    {
                                        var file = new FileInfo(value);

                                        if (!file.Exists)
                                        {
                                            mMessage = "Entry for " + FILTER_FILE_NAME_KEY + " in " + lcmsFeatureFinderIniFileName +
                                                        " points to an invalid file: " + value;
                                            LogError(mMessage);
                                            result = false;
                                            break;
                                        }

                                        // Copy the file locally
                                        var destinationPath = Path.Combine(mWorkDir, file.Name);
                                        file.CopyTo(destinationPath);
                                    }
                                }

                                writer.WriteLine(dataLine);
                            }
                        }

                        if (!inputFileDefined)
                        {
                            writer.WriteLine(INPUT_FILENAME_KEY + "=" + isosFilePath);
                        }

                        if (!outputDirectoryDefined)
                        {
                            writer.WriteLine(OUTPUT_DIRECTORY_KEY + "=" + mWorkDir);
                        }
                    }
                    catch (Exception ex)
                    {
                        LogError("AnalysisResourcesLCMSFF.UpdateFeatureFinderIniFile, Error opening the .Ini file to customize " +
                                 "(" + lcmsFeatureFinderIniFileName + "): " + ex.Message);
                        result = false;
                    }
                } // end using

                // Replace the original .Ini file with the new one
                AppUtils.GarbageCollectNow();

                // Delete the input file
                File.Delete(srcFilePath);

                AppUtils.GarbageCollectNow();

                // Rename the newly created output file to have the name of the input file
                File.Move(targetFilePath, srcFilePath);
            }
            catch (Exception ex)
            {
                LogError("AnalysisResourcesLCMSFF.UpdateFeatureFinderIniFile, Error opening the .Ini file to customize " +
                         "(" + lcmsFeatureFinderIniFileName + "): " + ex.Message);
                result = false;
            }

            return result;
        }
    }
}
