//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
//
// Created 07/20/2010
//
//*********************************************************************************************************

using AnalysisManagerBase;
using PHRPReader;
using System;
using System.Collections.Generic;
using System.IO;

namespace AnalysisManagerMSGFPlugin
{
    /// <summary>
    /// Retrieve resources for the MSGF plugin
    /// </summary>
    public class clsAnalysisResourcesMSGF : clsAnalysisResources
    {
        #region "Constants"

        /// <summary>
        /// ModDefs file suffix
        /// </summary>
        public const string PHRP_MOD_DEFS_SUFFIX = "_ModDefs.txt";

        #endregion

        #region "Module variables"

        // Keys are the original file name, values are the new name
        private Dictionary<string, string> mPendingFileRenames;

        #endregion

        #region "Methods"

        /// <summary>
        /// Gets all files needed by MSGF
        /// </summary>
        /// <returns>CloseOutType specifying results</returns>
        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            mPendingFileRenames = new Dictionary<string, string>();

            var scriptName = mJobParams.GetParam("ToolName");

            if (!scriptName.ToLower().StartsWith("MSGFPlus".ToLower()))
            {
                // Make sure the machine has enough free memory to run MSGF
                if (!ValidateFreeMemorySize("MSGFJavaMemorySize"))
                {
                    mMessage = "Not enough free memory to run MSGF";
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            // Get analysis results files
            result = GetInputFiles(mJobParams.GetParam("ResultType"));
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Retrieves input files needed for MSGF
        /// </summary>
        /// <param name="resultTypeName">String specifying type of analysis results input to extraction process</param>
        /// <returns>CloseOutType specifying results</returns>
        /// <remarks></remarks>
        private CloseOutType GetInputFiles(string resultTypeName)
        {
            bool onlyCopyFirstHitsAndSynopsisFiles;

            // Make sure the ResultType is valid
            var resultType = clsPHRPReader.GetPeptideHitResultType(resultTypeName);

            bool validToolType;
            if (resultType == clsPHRPReader.ePeptideHitResultType.Sequest ||
                resultType == clsPHRPReader.ePeptideHitResultType.XTandem ||
                resultType == clsPHRPReader.ePeptideHitResultType.Inspect ||
                resultType == clsPHRPReader.ePeptideHitResultType.MSGFPlus || // MS-GF+
                resultType == clsPHRPReader.ePeptideHitResultType.MODa ||
                resultType == clsPHRPReader.ePeptideHitResultType.MODPlus ||
                resultType == clsPHRPReader.ePeptideHitResultType.MSPathFinder)
            {
                validToolType = true;
            }
            else
            {
                LogError("Invalid tool result type in clsAnalysisResourcesMSGF.GetInputFiles (not supported by MSGF): " + resultType);
                validToolType = false;
            }

            if (!validToolType)
            {
                return (CloseOutType.CLOSEOUT_NO_OUT_FILES);
            }

            // Make sure the dataset type is valid
            var rawDataType = mJobParams.GetParam("RawDataType");
            var eRawDataType = GetRawDataType(rawDataType);
            var mgfInstrumentData = mJobParams.GetJobParameter("MGFInstrumentData", false);

            if (resultType == clsPHRPReader.ePeptideHitResultType.MSGFPlus)
            {
                // We do not need the mzXML file, the parameter file, or various other files if we are running MS-GF+ and running MSGF v6432 or later
                // Determine this by looking for job parameter MSGF_Version

                var msgfStepToolVersion = mJobParams.GetParam("MSGF_Version");

                if (string.IsNullOrWhiteSpace(msgfStepToolVersion))
                {
                    // Production version of MS-GF+; don't need the parameter file, ModSummary file, or mzXML file
                    onlyCopyFirstHitsAndSynopsisFiles = true;
                }
                else
                {
                    // Specific version of MSGF is defined
                    // Check whether the version is one of the known versions for the old MSGF
                    if (clsMSGFRunner.IsLegacyMSGFVersion(msgfStepToolVersion))
                    {
                        onlyCopyFirstHitsAndSynopsisFiles = false;
                    }
                    else
                    {
                        onlyCopyFirstHitsAndSynopsisFiles = true;
                    }
                }
            }
            else if (resultType == clsPHRPReader.ePeptideHitResultType.MODa |
                     resultType == clsPHRPReader.ePeptideHitResultType.MODPlus |
                     resultType == clsPHRPReader.ePeptideHitResultType.MSPathFinder)
            {
                // We do not need any raw data files for MODa, modPlus, or MSPathFinder
                onlyCopyFirstHitsAndSynopsisFiles = true;
            }
            else
            {
                // Not running MS-GF+ or running MS-GF+ but using legacy MSGF
                onlyCopyFirstHitsAndSynopsisFiles = false;

                if (!mgfInstrumentData)
                {
                    switch (eRawDataType)
                    {
                        case eRawDataTypeConstants.ThermoRawFile:
                        case eRawDataTypeConstants.mzML:
                        case eRawDataTypeConstants.mzXML:
                            break;
                        // This is a valid data type
                        default:
                            mMessage = "Dataset type " + rawDataType + " is not supported by MSGF";
                            LogDebug(
                                mMessage + "; must be one of the following: " + RAW_DATA_TYPE_DOT_RAW_FILES + ", " + RAW_DATA_TYPE_DOT_MZML_FILES +
                                ", " + RAW_DATA_TYPE_DOT_MZXML_FILES);
                            return CloseOutType.CLOSEOUT_FAILED;
                    }
                }
            }

            if (!onlyCopyFirstHitsAndSynopsisFiles)
            {
                // Get the SEQUEST, X!Tandem, Inspect, MS-GF+, MODa, MODPlus, or MSPathFinder parameter file
                fileToGet = mJobParams.GetParam("ParmFileName");
                if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, false))
                {
                    // Errors were reported in function call, so just return
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }
                mJobParams.AddResultFileToSkip(fileToGet);

                // Also copy the _ProteinMods.txt file
                fileToGet = clsPHRPReader.GetPHRPProteinModsFileName(eResultType, DatasetName);
                if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, false))
                {
                    // Ignore this error; we don't really need this file
                }
                else
                {
                    mJobParams.AddResultFileToKeep(fileToGet);
                }
            }

            // Get the PHRP _syn.txt file
            fileToGet = clsPHRPReader.GetPHRPSynopsisFileName(eResultType, DatasetName);
            if (!string.IsNullOrEmpty(fileToGet))
            {
                success = FileSearch.FindAndRetrievePHRPDataFile(ref fileToGet, "");
                if (!success)
                {
                    // Errors were reported in function call, so just return
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }
                synFilePath = Path.Combine(mWorkDir, fileToGet);
            }

            // Get the PHRP _fht.txt file
            fileToGet = clsPHRPReader.GetPHRPFirstHitsFileName(eResultType, DatasetName);
            if (!string.IsNullOrEmpty(fileToGet))
            {
                success = FileSearch.FindAndRetrievePHRPDataFile(ref fileToGet, synFilePath);
                if (!success)
                {
                    // Errors were reported in function call, so just return
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }
            }

            // Get the PHRP _ResultToSeqMap.txt file
            fileToGet = clsPHRPReader.GetPHRPFirstHitsFileName(eResultType, DatasetName);
            if (!string.IsNullOrEmpty(fileToGet))
            {
                success = FileSearch.FindAndRetrievePHRPDataFile(ref fileToGet, synFilePath);
                if (!success)
                {
                    // Errors were reported in function call, so just return
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }
            }

            // Get the PHRP _SeqToProteinMap.txt file
            fileToGet = clsPHRPReader.GetPHRPFirstHitsFileName(eResultType, DatasetName);
            if (!string.IsNullOrEmpty(fileToGet))
            {
                success = FileSearch.FindAndRetrievePHRPDataFile(ref fileToGet, synFilePath);
                if (!success)
                {
                    // Errors were reported in function call, so just return
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }
            }

            // Get the PHRP _PepToProtMapMTS.txt file
            fileToGet = clsPHRPReader.GetPHRPPepToProteinMapFileName(eResultType, DatasetName);
            if (!string.IsNullOrEmpty(fileToGet))
            {
                // We're passing a dummy syn file name to FileSearch.FindAndRetrievePHRPDataFile
                // because there are a few jobs that have file _msgfplus_fht.txt (created by the November 2016 version of the DataExtractor tool)
                // but also have file msgfdb_PepToProtMapMTS.txt (created by an older version of the MSGFPlus tool)
                success = FileSearch.FindAndRetrievePHRPDataFile(ref fileToGet, "Dataset_msgfdb.txt");
                if (!success)
                {
                    if (mJobParams.GetJobParameter("IgnorePeptideToProteinMapError", false))
                    {
                        LogWarning("Ignoring missing _PepToProtMapMTS.txt file since 'IgnorePeptideToProteinMapError' = True");
                    }
                    else if (mJobParams.GetJobParameter("SkipProteinMods", false))
                    {
                        LogWarning("Ignoring missing _PepToProtMapMTS.txt file since 'SkipProteinMods' = True");
                    }
                    else
                    {
                        // Errors were reported in function call, so just return
                        return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                    }
                }
            }

            success = ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories);
            if (!success)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            Int64 synFileSizeBytes = 0;
            var fiSynopsisFile = new FileInfo(synFilePath);
            if (fiSynopsisFile.Exists)
            {
                synFileSizeBytes = fiSynopsisFile.Length;
            }

            if (!onlyCopyFHTandSYNfiles)
            {
                // Get the ModSummary.txt file
                fileToGet = clsPHRPReader.GetPHRPModSummaryFileName(eResultType, DatasetName);
                success = FileSearch.FindAndRetrievePHRPDataFile(ref fileToGet, synFilePath);
                if (!success)
                {
                    // _ModSummary.txt file not found
                    // This will happen if the synopsis file is empty
                    // Try to copy the _ModDefs.txt file instead

                    if (synFileSizeBytes == 0)
                    {
                        // If the synopsis file is 0-bytes, the _ModSummary.txt file won't exist; that's OK
                        var targetFile = Path.Combine(mWorkDir, fileToGet);

                        var modDefsFile = Path.GetFileNameWithoutExtension(mJobParams.GetParam("ParmFileName")) + PHRP_MOD_DEFS_SUFFIX;

                        if (!FileSearch.FindAndRetrieveMiscFiles(modDefsFile, false))
                        {
                            // Rename the file to end in _ModSummary.txt
                            mPendingFileRenames.Add(modDefsFile, targetFile);
                        }
                        else
                        {
                            // Errors were reported in function call, so just return
                            return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                        }
                    }
                    else
                    {
                        // Errors were reported in function call, so just return
                        return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                    }
                }
            }

            // Copy the PHRP files so that the PHRPReader can determine the modified residues and extract the protein names
            // clsMSGFResultsSummarizer also uses these files

            fileToGet = clsPHRPReader.GetPHRPResultToSeqMapFileName(eResultType, DatasetName);
            if (!string.IsNullOrEmpty(fileToGet))
            {
                if (!FileSearch.FindAndRetrievePHRPDataFile(ref fileToGet, synFilePath))
                {
                    if (synFileSizeBytes == 0)
                    {
                        // If the synopsis file is 0-bytes, the _ResultToSeqMap.txt file won't exist
                        // That's OK; we'll create an empty file with just a header line
                        if (!CreateEmptyResultToSeqMapFile(fileToGet))
                        {
                            return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                        }
                    }
                    else
                    {
                        // Errors were reported in function call, so just return
                        return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                    }
                }
            }

            fileToGet = clsPHRPReader.GetPHRPSeqToProteinMapFileName(eResultType, DatasetName);
            if (!string.IsNullOrEmpty(fileToGet))
            {
                if (!FileSearch.FindAndRetrievePHRPDataFile(ref fileToGet, synFilePath))
                {
                    if (synFileSizeBytes == 0)
                    {
                        // If the synopsis file is 0-bytes, the _SeqToProteinMap.txt file won't exist
                        // That's OK; we'll create an empty file with just a header line
                        if (!CreateEmptySeqToProteinMapFile(fileToGet))
                        {
                            return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                        }
                    }
                    else
                    {
                        // Errors were reported in function call, so just return
                        return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                    }
                }
            }

            fileToGet = clsPHRPReader.GetPHRPSeqInfoFileName(eResultType, DatasetName);
            if (!string.IsNullOrEmpty(fileToGet))
            {
                if (FileSearch.FindAndRetrievePHRPDataFile(ref fileToGet, synFilePath))
                {
                }
                else
                {
                    LogWarning("SeqInfo file not found (" + fileToGet + "); modifications will be inferred using the ModSummary.txt file");
                }
            }

            if (mgfInstrumentData)
            {
                var fileToFind = DatasetName + DOT_MGF_EXTENSION;
                if (!FileSearch.FindAndRetrieveMiscFiles(fileToFind, false))
                {
                    mMessage = "Instrument data not found: " + fileToFind;
                    LogError("clsAnalysisResourcesMSGF.GetResources: " + mMessage);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mJobParams.AddResultFileExtensionToSkip(DOT_MGF_EXTENSION);

            }
            else if (!onlyCopyFHTandSYNfiles)
            {

                // See if a .mzXML file already exists for this dataset
                success = FileSearch.RetrieveMZXmlFile(false, out var mzXMLFilePath);

                // Make sure we don't move the .mzXML file into the results folder
                mJobParams.AddResultFileExtensionToSkip(DOT_MZXML_EXTENSION);

                if (success)
                {
                    // .mzXML file found and copied locally; no need to retrieve the .Raw file
                    if (mDebugLevel >= 1)
                    {
                        LogMessage("Existing .mzXML file found: " + mzXMLFilePath);
                    }

                    // Possibly unzip the .mzXML file
                    var fiMzXMLFile = new FileInfo(Path.Combine(mWorkDir, DatasetName + DOT_MZXML_EXTENSION + DOT_GZ_EXTENSION));
                    if (fiMzXMLFile.Exists)
                    {
                        mJobParams.AddResultFileExtensionToSkip(DOT_GZ_EXTENSION);

                        if (!GUnzipFile(fiMzXMLFile.FullName))
                        {
                            mMessage = "Error decompressing .mzXML.gz file";
                            return CloseOutType.CLOSEOUT_FAILED;
                        }
                    }
                }
                else
                {
                    // .mzXML file not found
                    // Retrieve the .Raw file so that we can make the .mzXML file prior to running MSGF
                    if (FileSearch.RetrieveSpectra(rawDataType))
                    {
                        mJobParams.AddResultFileExtensionToSkip(DOT_RAW_EXTENSION);
                        // Raw file
                        mJobParams.AddResultFileExtensionToSkip(DOT_MZXML_EXTENSION);
                        // mzXML file
                    }
                    else
                    {
                        LogError("clsAnalysisResourcesMSGF.GetResources: Error occurred retrieving spectra.");
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }
            }

            success = ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories);
            if (!success)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            foreach (var entry in mPendingFileRenames)
            {
                var sourceFile = new FileInfo(Path.Combine(mWorkDir, entry.Key));
                if (sourceFile.Exists)
                {
                    sourceFile.MoveTo(Path.Combine(mWorkDir, entry.Value));
                }
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private bool CreateEmptyResultToSeqMapFile(string resultToSeqMapFile)
        {
            try
            {
                var filePath = Path.Combine(mWorkDir, resultToSeqMapFile);
                using (var writer = new StreamWriter(new FileStream(filePath, FileMode.CreateNew, FileAccess.Write, FileShare.Read)))
                {
                    writer.WriteLine("Result_ID\tUnique_Seq_ID");
                }
            }
            catch (Exception ex)
            {
                var errorMessage = "Error creating empty ResultToSeqMap file: " + ex.Message;
                LogError(errorMessage);
                return false;
            }

            return true;
        }

        private bool CreateEmptySeqToProteinMapFile(string FileName)
        {
            try
            {
                var filePath = Path.Combine(mWorkDir, FileName);
                using (var writer = new StreamWriter(new FileStream(filePath, FileMode.CreateNew, FileAccess.Write, FileShare.Read)))
                {
                    writer.WriteLine("Unique_Seq_ID\tCleavage_State\tTerminus_State\tProtein_Name\tProtein_Expectation_Value_Log(e)\tProtein_Intensity_Log(I)");
                }
            }
            catch (Exception ex)
            {
                var Msg = "Error creating empty SeqToProteinMap file: " + ex.Message;
                LogError(Msg);
                return false;
            }

            return true;
        }

        #endregion
    }
}
