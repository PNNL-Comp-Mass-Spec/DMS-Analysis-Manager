//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
//
// Created 07/20/2010
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;

using AnalysisManagerBase;
using PHRPReader;
using System.IO;

namespace AnalysisManagerMSGFPlugin
{
    /// <summary>
    /// Manages retrieval of all files needed by MSGF
    /// </summary>
    public class clsAnalysisResourcesMSGF : clsAnalysisResources
    {
        #region "Constants"

        public const string PHRP_MOD_DEFS_SUFFIX = "_ModDefs.txt";

        #endregion

        #region "Module variables"

        // Keys are the original file name, values are the new name
        private Dictionary<string, string> m_PendingFileRenames;

        #endregion

        #region "Methods"

        /// <summary>
        /// Gets all files needed by MSGF
        /// </summary>
        /// <returns>CloseOutType specifying results</returns>
        /// <remarks></remarks>
        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            m_PendingFileRenames = new Dictionary<string, string>();

            var strScriptName = m_jobParams.GetParam("ToolName");

            if (!strScriptName.ToLower().StartsWith("MSGFPlus".ToLower()))
            {
                // Make sure the machine has enough free memory to run MSGF
                if (!ValidateFreeMemorySize("MSGFJavaMemorySize", "MSGF"))
                {
                    m_message = "Not enough free memory to run MSGF";
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            //Get analysis results files
            result = GetInputFiles(m_jobParams.GetParam("ResultType"));
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Retrieves input files needed for MSGF
        /// </summary>
        /// <param name="resultType">String specifying type of analysis results input to extraction process</param>
        /// <returns>CloseOutType specifying results</returns>
        /// <remarks></remarks>
        private CloseOutType GetInputFiles(string resultType)
        {
            string fileToGet;
            var strSynFilePath = string.Empty;

            bool blnSuccess;
            bool blnOnlyCopyFHTandSYNfiles;

            // Make sure the ResultType is valid
            var eResultType = clsPHRPReader.GetPeptideHitResultType(resultType);

            if (eResultType == clsPHRPReader.ePeptideHitResultType.Sequest ||
                eResultType == clsPHRPReader.ePeptideHitResultType.XTandem ||
                eResultType == clsPHRPReader.ePeptideHitResultType.Inspect ||
                eResultType == clsPHRPReader.ePeptideHitResultType.MSGFDB || // MSGF+
                eResultType == clsPHRPReader.ePeptideHitResultType.MODa ||
                eResultType == clsPHRPReader.ePeptideHitResultType.MODPlus ||
                eResultType == clsPHRPReader.ePeptideHitResultType.MSPathFinder)
            {
                blnSuccess = true;
            }
            else
            {
                LogError("Invalid tool result type (not supported by MSGF): " + resultType);
                blnSuccess = false;
            }

            if (!blnSuccess)
            {
                return (CloseOutType.CLOSEOUT_NO_OUT_FILES);
            }

            // Make sure the dataset type is valid
            var rawDataType = m_jobParams.GetParam("RawDataType");
            var eRawDataType = GetRawDataType(rawDataType);
            var blnMGFInstrumentData = m_jobParams.GetJobParameter("MGFInstrumentData", false);

            if (eResultType == clsPHRPReader.ePeptideHitResultType.MSGFDB)
            {
                // We do not need the mzXML file, the parameter file, or various other files if we are running MSGF+ and running MSGF v6432 or later
                // Determine this by looking for job parameter MSGF_Version

                var strMSGFStepToolVersion = m_jobParams.GetParam("MSGF_Version");

                if (string.IsNullOrWhiteSpace(strMSGFStepToolVersion))
                {
                    // Production version of MSGF+; don't need the parameter file, ModSummary file, or mzXML file
                    blnOnlyCopyFHTandSYNfiles = true;
                }
                else
                {
                    // Specific version of MSGF is defined
                    // Check whether the version is one of the known versions for the old MSGF
                    if (clsMSGFRunner.IsLegacyMSGFVersion(strMSGFStepToolVersion))
                    {
                        blnOnlyCopyFHTandSYNfiles = false;
                    }
                    else
                    {
                        blnOnlyCopyFHTandSYNfiles = true;
                    }
                }
            }
            else if (eResultType == clsPHRPReader.ePeptideHitResultType.MODa |
                     eResultType == clsPHRPReader.ePeptideHitResultType.MODPlus |
                     eResultType == clsPHRPReader.ePeptideHitResultType.MSPathFinder)
            {
                // We do not need any raw data files for MODa, modPlus, or MSPathFinder
                blnOnlyCopyFHTandSYNfiles = true;
            }
            else
            {
                // Not running MSGF+ or running MSGF+ but using legacy msgf
                blnOnlyCopyFHTandSYNfiles = false;

                if (!blnMGFInstrumentData)
                {
                    switch (eRawDataType)
                    {
                        case eRawDataTypeConstants.ThermoRawFile:
                        case eRawDataTypeConstants.mzML:
                        case eRawDataTypeConstants.mzXML:
                            break;
                        // This is a valid data type
                        default:
                            m_message = "Dataset type " + rawDataType + " is not supported by MSGF";
                            LogDebug(
                                m_message + "; must be one of the following: " + RAW_DATA_TYPE_DOT_RAW_FILES + ", " + RAW_DATA_TYPE_DOT_MZML_FILES +
                                ", " + RAW_DATA_TYPE_DOT_MZXML_FILES);
                            return CloseOutType.CLOSEOUT_FAILED;
                    }
                }
            }

            if (!blnOnlyCopyFHTandSYNfiles)
            {
                // Get the Sequest, X!Tandem, Inspect, MSGF+, MODa, MODPlus, or MSPathFinder parameter file
                fileToGet = m_jobParams.GetParam("ParmFileName");
                if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, false))
                {
                    //Errors were reported in function call, so just return
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }
                m_jobParams.AddResultFileToSkip(fileToGet);

                // Also copy the _ProteinMods.txt file
                fileToGet = clsPHRPReader.GetPHRPProteinModsFileName(eResultType, DatasetName);
                if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, false))
                {
                    // Ignore this error; we don't really need this file
                }
                else
                {
                    m_jobParams.AddResultFileToKeep(fileToGet);
                }
            }

            // Get the PHRP _syn.txt file
            fileToGet = clsPHRPReader.GetPHRPSynopsisFileName(eResultType, DatasetName);
            if (!string.IsNullOrEmpty(fileToGet))
            {
                blnSuccess = FileSearch.FindAndRetrievePHRPDataFile(ref fileToGet, "");
                if (!blnSuccess)
                {
                    //Errors were reported in function call, so just return
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }
                strSynFilePath = Path.Combine(m_WorkingDir, fileToGet);
            }

            // Get the PHRP _fht.txt file
            fileToGet = clsPHRPReader.GetPHRPFirstHitsFileName(eResultType, DatasetName);
            if (!string.IsNullOrEmpty(fileToGet))
            {
                blnSuccess = FileSearch.FindAndRetrievePHRPDataFile(ref fileToGet, strSynFilePath);
                if (!blnSuccess)
                {
                    //Errors were reported in function call, so just return
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }
            }

            // Get the PHRP _ResultToSeqMap.txt file
            fileToGet = clsPHRPReader.GetPHRPFirstHitsFileName(eResultType, DatasetName);
            if (!string.IsNullOrEmpty(fileToGet))
            {
                blnSuccess = FileSearch.FindAndRetrievePHRPDataFile(ref fileToGet, strSynFilePath);
                if (!blnSuccess)
                {
                    //Errors were reported in function call, so just return
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }
            }

            // Get the PHRP _SeqToProteinMap.txt file
            fileToGet = clsPHRPReader.GetPHRPFirstHitsFileName(eResultType, DatasetName);
            if (!string.IsNullOrEmpty(fileToGet))
            {
                blnSuccess = FileSearch.FindAndRetrievePHRPDataFile(ref fileToGet, strSynFilePath);
                if (!blnSuccess)
                {
                    //Errors were reported in function call, so just return
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
                blnSuccess = FileSearch.FindAndRetrievePHRPDataFile(ref fileToGet, "Dataset_msgfdb.txt");
                if (!blnSuccess)
                {
                    if (m_jobParams.GetJobParameter("IgnorePeptideToProteinMapError", false))
                    {
                        LogWarning("Ignoring missing _PepToProtMapMTS.txt file since 'IgnorePeptideToProteinMapError' = True");
                    }
                    else if (m_jobParams.GetJobParameter("SkipProteinMods", false))
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

            blnSuccess = ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders);
            if (!blnSuccess)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            Int64 synFileSizeBytes = 0;
            var fiSynopsisFile = new FileInfo(strSynFilePath);
            if (fiSynopsisFile.Exists)
            {
                synFileSizeBytes = fiSynopsisFile.Length;
            }

            if (!blnOnlyCopyFHTandSYNfiles)
            {
                // Get the ModSummary.txt file
                fileToGet = clsPHRPReader.GetPHRPModSummaryFileName(eResultType, DatasetName);
                blnSuccess = FileSearch.FindAndRetrievePHRPDataFile(ref fileToGet, strSynFilePath);
                if (!blnSuccess)
                {
                    // _ModSummary.txt file not found
                    // This will happen if the synopsis file is empty
                    // Try to copy the _ModDefs.txt file instead

                    if (synFileSizeBytes == 0)
                    {
                        // If the synopsis file is 0-bytes, then the _ModSummary.txt file won't exist; that's OK
                        var strTargetFile = Path.Combine(m_WorkingDir, fileToGet);

                        var strModDefsFile = Path.GetFileNameWithoutExtension(m_jobParams.GetParam("ParmFileName")) + PHRP_MOD_DEFS_SUFFIX;

                        if (!FileSearch.FindAndRetrieveMiscFiles(strModDefsFile, false))
                        {
                            // Rename the file to end in _ModSummary.txt
                            m_PendingFileRenames.Add(strModDefsFile, strTargetFile);
                        }
                        else
                        {
                            //Errors were reported in function call, so just return
                            return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                        }
                    }
                    else
                    {
                        //Errors were reported in function call, so just return
                        return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                    }
                }
            }

            // Copy the PHRP files so that the PHRPReader can determine the modified residues and extract the protein names
            // clsMSGFResultsSummarizer also uses these files

            fileToGet = clsPHRPReader.GetPHRPResultToSeqMapFileName(eResultType, DatasetName);
            if (!string.IsNullOrEmpty(fileToGet))
            {
                if (!FileSearch.FindAndRetrievePHRPDataFile(ref fileToGet, strSynFilePath))
                {
                    if (synFileSizeBytes == 0)
                    {
                        // If the synopsis file is 0-bytes, then the _ResultToSeqMap.txt file won't exist
                        // That's OK; we'll create an empty file with just a header line
                        if (!CreateEmptyResultToSeqMapFile(fileToGet))
                        {
                            return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                        }
                    }
                    else
                    {
                        //Errors were reported in function call, so just return
                        return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                    }
                }
            }

            fileToGet = clsPHRPReader.GetPHRPSeqToProteinMapFileName(eResultType, DatasetName);
            if (!string.IsNullOrEmpty(fileToGet))
            {
                if (!FileSearch.FindAndRetrievePHRPDataFile(ref fileToGet, strSynFilePath))
                {
                    if (synFileSizeBytes == 0)
                    {
                        // If the synopsis file is 0-bytes, then the _SeqToProteinMap.txt file won't exist
                        // That's OK; we'll create an empty file with just a header line
                        if (!CreateEmptySeqToProteinMapFile(fileToGet))
                        {
                            return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                        }
                    }
                    else
                    {
                        //Errors were reported in function call, so just return
                        return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                    }
                }
            }

            fileToGet = clsPHRPReader.GetPHRPSeqInfoFileName(eResultType, DatasetName);
            if (!string.IsNullOrEmpty(fileToGet))
            {
                if (FileSearch.FindAndRetrievePHRPDataFile(ref fileToGet, strSynFilePath))
                {
                }
                else
                {
                    LogWarning("SeqInfo file not found (" + fileToGet + "); modifications will be inferred using the ModSummary.txt file");
                }
            }

            if (blnMGFInstrumentData)
            {
                var strFileToFind = DatasetName + DOT_MGF_EXTENSION;
                if (!FileSearch.FindAndRetrieveMiscFiles(strFileToFind, false))
                {
                    m_message = "Instrument data not found: " + strFileToFind;
                    LogError("clsAnalysisResourcesMSGF.GetResources: " + m_message);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                m_jobParams.AddResultFileExtensionToSkip(DOT_MGF_EXTENSION);

            }
            else if (!blnOnlyCopyFHTandSYNfiles)
            {
                string strMzXMLFilePath;

                // See if a .mzXML file already exists for this dataset
                blnSuccess = FileSearch.RetrieveMZXmlFile(false, out strMzXMLFilePath);

                // Make sure we don't move the .mzXML file into the results folder
                m_jobParams.AddResultFileExtensionToSkip(DOT_MZXML_EXTENSION);

                if (blnSuccess)
                {
                    // .mzXML file found and copied locally; no need to retrieve the .Raw file
                    if (m_DebugLevel >= 1)
                    {
                        LogMessage("Existing .mzXML file found: " + strMzXMLFilePath);
                    }

                    // Possibly unzip the .mzXML file
                    var fiMzXMLFile = new FileInfo(Path.Combine(m_WorkingDir, DatasetName + DOT_MZXML_EXTENSION + DOT_GZ_EXTENSION));
                    if (fiMzXMLFile.Exists)
                    {
                        m_jobParams.AddResultFileExtensionToSkip(DOT_GZ_EXTENSION);

                        if (!GUnzipFile(fiMzXMLFile.FullName))
                        {
                            m_message = "Error decompressing .mzXML.gz file";
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
                        m_jobParams.AddResultFileExtensionToSkip(DOT_RAW_EXTENSION);
                        // Raw file
                        m_jobParams.AddResultFileExtensionToSkip(DOT_MZXML_EXTENSION);
                        // mzXML file
                    }
                    else
                    {
                        LogError("clsAnalysisResourcesMSGF.GetResources: Error occurred retrieving spectra.");
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }
            }

            blnSuccess = ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders);
            if (!blnSuccess)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            foreach (var entry in m_PendingFileRenames)
            {
                var sourceFile = new FileInfo(Path.Combine(m_WorkingDir, entry.Key));
                if (sourceFile.Exists)
                {
                    sourceFile.MoveTo(Path.Combine(m_WorkingDir, entry.Value));
                }
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private bool CreateEmptyResultToSeqMapFile(string fileName)
        {
            try
            {
                var strFilePath = Path.Combine(m_WorkingDir, fileName);
                using (var swOutfile = new StreamWriter(new FileStream(strFilePath, FileMode.CreateNew, FileAccess.Write, FileShare.Read)))
                {
                    swOutfile.WriteLine("Result_ID\tUnique_Seq_ID");
                }
            }
            catch (Exception ex)
            {
                var Msg = "Error creating empty ResultToSeqMap file: " + ex.Message;
                LogError(Msg);
                return false;
            }

            return true;
        }

        private bool CreateEmptySeqToProteinMapFile(string FileName)
        {
            try
            {
                var strFilePath = Path.Combine(m_WorkingDir, FileName);
                using (var swOutfile = new StreamWriter(new FileStream(strFilePath, FileMode.CreateNew, FileAccess.Write, FileShare.Read)))
                {
                    swOutfile.WriteLine("Unique_Seq_ID\tCleavage_State\tTerminus_State\tProtein_Name\tProtein_Expectation_Value_Log(e)\tProtein_Intensity_Log(I)");
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
