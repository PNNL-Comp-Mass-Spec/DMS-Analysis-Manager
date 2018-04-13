//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 09/22/2008
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase;
using AnalysisManagerMSGFDBPlugIn;
using PHRPReader;

namespace AnalysisManagerExtractionPlugin
{
    /// <summary>
    /// Manages retrieval of all files needed for data extraction
    /// </summary>
    /// <remarks></remarks>
    public class clsAnalysisResourcesExtraction : clsAnalysisResources
    {

        /// <summary>
        /// ModDefs file suffix
        /// </summary>
        public const string MOD_DEFS_FILE_SUFFIX = "_ModDefs.txt";

        /// <summary>
        /// Mass Correction Tags filename
        /// </summary>
        public const string MASS_CORRECTION_TAGS_FILENAME = "Mass_Correction_Tags.txt";

        protected bool mRetrieveOrganismDB;

        // Keys are the original file name, values are the new name
        protected Dictionary<string, string> m_PendingFileRenames;

        /// <summary>
        /// Initialize options
        /// </summary>
        public override void Setup(string stepToolName, IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, clsMyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(stepToolName, mgrParams, jobParams, statusTools, myEMSLUtilities);

            // Always retrieve the FASTA file because PHRP uses it
            // This includes for MSGF+ because it uses the order of the proteins in the
            // FASTA file to determine the protein to include in the FHT file

            SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, true);
        }

        /// <summary>
        /// Gets all files needed to perform data extraction
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

            // Set this to true for now
            // It will be changed to False if processing Inspect results and the _PepToProtMap.txt file is successfully retrieved
            mRetrieveOrganismDB = true;
            m_PendingFileRenames = new Dictionary<string, string>();

            var resultType = m_jobParams.GetParam("ResultType");

            // Get analysis results files
            if (GetInputFiles(resultType, out var createPepToProtMapFile) != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Get misc files
            if (RetrieveMiscFiles(resultType) != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
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

            if (mRetrieveOrganismDB)
            {
                var skipProteinMods = m_jobParams.GetJobParameter("SkipProteinMods", false);
                if (!skipProteinMods || createPepToProtMapFile)
                {
                    // Retrieve the Fasta file; required to create the _ProteinMods.txt file
                    if (!RetrieveOrgDB(m_mgrParams.GetParam("orgdbdir")))
                    {
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Retrieves input files (ie, .out files) needed for extraction
        /// </summary>
        /// <param name="resultType">String specifying type of analysis results input to extraction process</param>
        /// <param name="createPepToProtMapFile"></param>
        /// <returns>CloseOutType specifying results</returns>
        /// <remarks></remarks>
        private CloseOutType GetInputFiles(string resultType, out bool createPepToProtMapFile)
        {
            createPepToProtMapFile = false;

            try
            {
                var inputFolderName = m_jobParams.GetParam("inputFolderName");
                if (string.IsNullOrWhiteSpace(inputFolderName))
                {
                    LogError("Input_Folder is not defined for this job step (job parameter inputFolderName); cannot retrieve input files");
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                CloseOutType result;
                switch (resultType)
                {
                    case RESULT_TYPE_SEQUEST:
                        result = GetSequestFiles();
                        break;

                    case RESULT_TYPE_XTANDEM:
                        result = GetXTandemFiles();
                        break;

                    case RESULT_TYPE_INSPECT:
                        result = GetInspectFiles();
                        break;

                    case RESULT_TYPE_MSGFPLUS:
                        result = GetMSGFPlusFiles(out createPepToProtMapFile);
                        break;

                    case RESULT_TYPE_MSALIGN:
                        result = GetMSAlignFiles();
                        break;

                    case RESULT_TYPE_MODA:
                        result = GetMODaFiles();
                        break;

                    case RESULT_TYPE_MODPLUS:
                        result = GetMODPlusFiles();
                        break;

                    case RESULT_TYPE_MSPATHFINDER:
                        result = GetMSPathFinderFiles();
                        m_jobParams.AddResultFileExtensionToSkip(".tsv");
                        break;

                    default:
                        LogError("Invalid tool result type: " + resultType);
                        return CloseOutType.CLOSEOUT_FAILED;
                }

                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                RetrieveToolVersionFile(resultType);
            }
            catch (Exception ex)
            {
                LogError("Error retrieving input files", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType GetSequestFiles()
        {
            // Get the concatenated .out file
            if (!FileSearch.RetrieveOutFiles(false))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_NO_OUT_FILES;
            }

            // Note that we'll obtain the SEQUEST parameter file in RetrieveMiscFiles

            // Add all the extensions of the files to delete after run
            m_jobParams.AddResultFileExtensionToSkip("_dta.zip");    // Zipped DTA
            m_jobParams.AddResultFileExtensionToSkip("_dta.txt");    // Unzipped, concatenated DTA
            m_jobParams.AddResultFileExtensionToSkip("_out.zip");    // Zipped OUT
            m_jobParams.AddResultFileExtensionToSkip("_out.txt");    // Unzipped, concatenated OUT
            m_jobParams.AddResultFileExtensionToSkip(".dta");        // DTA files
            m_jobParams.AddResultFileExtensionToSkip(".out");        // DTA files

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType GetXTandemFiles()
        {
            var fileToGet = DatasetName + "_xt.zip";
            if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, true))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_NO_XT_FILES;
            }
            m_jobParams.AddResultFileToSkip(fileToGet);

            // Manually adding this file to FilesToDelete; we don't want the unzipped .xml file to be copied to the server
            m_jobParams.AddResultFileToSkip(DatasetName + "_xt.xml");

            // Note that we'll obtain the X!Tandem parameter file in RetrieveMiscFiles

            // However, we need to obtain the "input.xml" file and "default_input.xml" files now
            fileToGet = "input.xml";
            if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }
            m_jobParams.AddResultFileToSkip(fileToGet);

            if (!CopyFileToWorkDir("default_input.xml", m_jobParams.GetParam("ParmFileStoragePath"), m_WorkingDir))
            {
                LogError("Failed retrieving default_input.xml file");
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType GetInspectFiles()
        {
            // Get the zipped Inspect results files

            // This file contains the p-value filtered results
            var fileToGet = DatasetName + "_inspect.zip";
            if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_NO_INSP_FILES;
            }
            m_jobParams.AddResultFileToSkip(fileToGet);

            // This file contains top hit for each scan (no filters)
            fileToGet = DatasetName + "_inspect_fht.zip";
            if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_NO_INSP_FILES;
            }
            m_jobParams.AddResultFileToSkip(fileToGet);

            // Get the peptide to protein mapping file
            fileToGet = DatasetName + "_inspect_PepToProtMap.txt";
            if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                // Errors were reported in function call

                // See if IgnorePeptideToProteinMapError=True
                if (m_jobParams.GetJobParameter("IgnorePeptideToProteinMapError", false))
                {
                    LogWarning(
                        "Ignoring missing _PepToProtMap.txt file since 'IgnorePeptideToProteinMapError' = True");
                }
                else
                {
                    return CloseOutType.CLOSEOUT_NO_INSP_FILES;
                }
            }
            else
            {
                // The OrgDB (aka fasta file) is not required
                mRetrieveOrganismDB = false;
            }
            m_jobParams.AddResultFileToSkip(fileToGet);

            // Note that we'll obtain the Inspect parameter file in RetrieveMiscFiles

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType GetMODaFiles()
        {
            var fileToGet = DatasetName + "_moda.zip";
            if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, true))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }
            m_jobParams.AddResultFileToSkip(fileToGet);
            m_jobParams.AddResultFileExtensionToSkip("_moda.txt");

            fileToGet = DatasetName + "_mgf_IndexToScanMap.txt";
            if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }
            m_jobParams.AddResultFileToSkip(fileToGet);

            // Note that we'll obtain the MODa parameter file in RetrieveMiscFiles

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType GetMODPlusFiles()
        {
            var fileToGet = DatasetName + "_modp.zip";
            if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, true))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }
            m_jobParams.AddResultFileToSkip(fileToGet);
            m_jobParams.AddResultFileExtensionToSkip("_modp.txt");

            // Delete the MSConvert_ConsoleOutput.txt and MODPlus_ConsoleOutput files that were in the zip file; we don't need them

            var diWorkDir = new DirectoryInfo(m_WorkingDir);
            var filesToDelete = new List<FileInfo>();

            filesToDelete.AddRange(diWorkDir.GetFiles("MODPlus_ConsoleOutput_Part*.txt"));
            filesToDelete.AddRange(diWorkDir.GetFiles("MSConvert_ConsoleOutput.txt"));
            filesToDelete.AddRange(diWorkDir.GetFiles("TDA_Plus_ConsoleOutput.txt"));

            foreach (var fiFile in filesToDelete)
            {
                fiFile.Delete();
            }

            // Note that we'll obtain the MODPlus parameter file in RetrieveMiscFiles

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType GetMSGFPlusFiles(out bool createPepToProtMapFile)
        {
            var currentStep = "Initializing";
            createPepToProtMapFile = false;

            var splitFastaEnabled = m_jobParams.GetJobParameter("SplitFasta", false);

            var numberOfClonedSteps = 1;

            try
            {
                string suffixToAdd;
                if (splitFastaEnabled)
                {
                    numberOfClonedSteps = m_jobParams.GetJobParameter("NumberOfClonedSteps", 0);
                    if (numberOfClonedSteps == 0)
                    {
                        LogError("Settings file is missing parameter NumberOfClonedSteps; cannot retrieve MSGFPlus results");
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    suffixToAdd = "_Part1";
                }
                else
                {
                    suffixToAdd = string.Empty;
                }

                currentStep = "Determining results file type based on the results file name";
                var useLegacyMSGFDB = false;

                // Look for file DatasetName_msgfplus.mzid.gz
                // or for split fasta, DatasetName_msgfplus_Part1.mzid.gz

                var fileToFind = DatasetName + "_msgfplus" + suffixToAdd + ".mzid.gz";
                var sourceDir = FileSearch.FindDataFile(fileToFind, true, false);
                string mzidSuffix;
                if (!string.IsNullOrEmpty(sourceDir))
                {
                    // Running MSGF+ with gzipped results
                    mzidSuffix = ".mzid.gz";
                }
                else
                {
                    // File not found; look for DatasetName_msgfdb.mzid.gz or DatasetName_msgfdb_Part1.mzid.gz
                    var fileToGetAlternative = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(fileToFind, DatasetName + "_msgfdb.txt");
                    var mzidSourceDirAlt = FileSearch.FindDataFile(fileToGetAlternative, true, false);

                    if (!string.IsNullOrEmpty(mzidSourceDirAlt))
                    {
                        // Running MSGF+ with gzipped results
                        mzidSuffix = ".mzid.gz";
                        sourceDir = mzidSourceDirAlt;
                    }
                    else
                    {
                        // File not found; look for a .zip file
                        var zipSourceDir = FileSearch.FindDataFile(DatasetName + "_msgfplus" + suffixToAdd + ".zip", true, false);
                        if (!string.IsNullOrEmpty(zipSourceDir))
                        {
                            // Running MSGF+ with zipped results
                            mzidSuffix = ".zip";
                            sourceDir = zipSourceDir;
                        }
                        else
                        {
                            // File not found; try _msgfdb
                            var zipSourceDirAlt = FileSearch.FindDataFile(DatasetName + "_msgfdb" + suffixToAdd + ".zip", true, false);
                            if (!string.IsNullOrEmpty(zipSourceDirAlt))
                            {
                                // File Found
                                useLegacyMSGFDB = true;
                                mzidSuffix = ".zip";
                                sourceDir = zipSourceDirAlt;
                            }
                            else
                            {
                                // File not found; log a warning
                                LogWarning(
                                    "Could not find the _msgfplus.mzid.gz, _msgfplus.zip file, or the _msgfdb.zip file; assuming we're running MSGF+");
                                mzidSuffix = ".mzid.gz";
                            }
                        }
                    }
                }

                for (var iteration = 1; iteration <= numberOfClonedSteps; iteration++)
                {
                    var skipMSGFResultsZipFileCopy = false;
                    string baseName;

                    if (splitFastaEnabled)
                    {
                        suffixToAdd = "_Part" + iteration;
                    }
                    else
                    {
                        suffixToAdd = string.Empty;
                    }

                    if (useLegacyMSGFDB)
                    {
                        baseName = DatasetName + "_msgfdb";

                        if (splitFastaEnabled)
                        {
                            LogError("GetMSGFPlusFiles does not support SplitFasta mode for legacy MSGF-DB results");
                            return CloseOutType.CLOSEOUT_FAILED;
                        }
                    }
                    else
                    {
                        baseName = DatasetName + "_msgfplus" + suffixToAdd;

                        var tsvFile = baseName + ".tsv";
                        currentStep = "Retrieving " + tsvFile;

                        var tsvSourceDir = FileSearch.FindDataFile(tsvFile, false, false);
                        if (string.IsNullOrEmpty(tsvSourceDir))
                        {
                            var fileToGetAlternative = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(tsvFile, DatasetName + "_msgfdb.txt");
                            var tsvSourceDirAlt = FileSearch.FindDataFile(fileToGetAlternative, false, false);
                            if (!string.IsNullOrEmpty(tsvSourceDirAlt))
                            {
                                tsvFile = fileToGetAlternative;
                                tsvSourceDir = tsvSourceDirAlt;
                            }
                        }

                        if (!string.IsNullOrEmpty(tsvSourceDir))
                        {
                            if (!tsvSourceDir.StartsWith(MYEMSL_PATH_FLAG))
                            {
                                // Examine the date of the TSV file
                                // If less than 4 hours old, retrieve it; otherwise, grab the _msgfplus.mzid.gz file and re-generate the .tsv file

                                var fiTSVFile = new FileInfo(Path.Combine(tsvSourceDir, tsvFile));
                                if (DateTime.UtcNow.Subtract(fiTSVFile.LastWriteTimeUtc).TotalHours < 4)
                                {
                                    // File is recent; grab it
                                    if (!CopyFileToWorkDir(tsvFile, tsvSourceDir, m_WorkingDir))
                                    {
                                        // File copy failed; that's OK; we'll grab the _msgfplus.mzid.gz file
                                    }
                                    else
                                    {
                                        skipMSGFResultsZipFileCopy = true;
                                        m_jobParams.AddResultFileToSkip(tsvFile);
                                    }
                                }

                                m_jobParams.AddServerFileToDelete(fiTSVFile.FullName);
                            }
                        }
                    }

                    if (!skipMSGFResultsZipFileCopy)
                    {
                        var mzidFile = baseName + mzidSuffix;
                        currentStep = "Retrieving " + mzidFile;

                        if (!FileSearch.FindAndRetrieveMiscFiles(mzidFile, unzip: true, searchArchivedDatasetFolder: true, logFileNotFound: true))
                        {
                            // Errors were reported in function call, so just return
                            return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                        }
                        m_jobParams.AddResultFileToSkip(mzidFile);

                        // Also retrieve the ConsoleOutput file; the command line used to call the MzidToTsvConverter.exe will be appended to this file
                        // This file is not critical, so pass false to logFileNotFound
                        FileSearch.FindAndRetrieveMiscFiles(MSGFPlusUtils.MSGFPLUS_CONSOLE_OUTPUT_FILE, unzip: false,
                                                            searchArchivedDatasetFolder: true, logFileNotFound: false);

                    }

                    // Manually add several files to skip
                    if (splitFastaEnabled)
                    {
                        m_jobParams.AddResultFileToSkip(DatasetName + "_msgfplus_Part" + iteration + ".txt");
                        m_jobParams.AddResultFileToSkip(DatasetName + "_msgfplus_Part" + iteration + ".mzid");
                        m_jobParams.AddResultFileToSkip(DatasetName + "_msgfplus_Part" + iteration + ".tsv");
                        m_jobParams.AddResultFileToSkip(DatasetName + "_msgfdb_Part" + iteration + ".txt");
                        m_jobParams.AddResultFileToSkip(DatasetName + "_msgfdb_Part" + iteration + ".tsv");
                    }
                    else
                    {
                        m_jobParams.AddResultFileToSkip(DatasetName + "_msgfplus.txt");
                        m_jobParams.AddResultFileToSkip(DatasetName + "_msgfplus.mzid");
                        m_jobParams.AddResultFileToSkip(DatasetName + "_msgfplus.tsv");
                        m_jobParams.AddResultFileToSkip(DatasetName + "_msgfdb.txt");
                        m_jobParams.AddResultFileToSkip(DatasetName + "_msgfdb.tsv");
                    }

                    // Get the peptide to protein mapping file
                    var pepToProtMapFile = baseName + "_PepToProtMap.txt";
                    currentStep = "Retrieving " + pepToProtMapFile;

                    if (!FileSearch.FindAndRetrievePHRPDataFile(ref pepToProtMapFile, synopsisFileName: "", addToResultFileSkipList: true, logFileNotFound: false))
                    {
                        // Errors were reported in function call

                        // See if IgnorePeptideToProteinMapError=True
                        if (m_jobParams.GetJobParameter("IgnorePeptideToProteinMapError", false))
                        {
                            LogWarning(
                                "Ignoring missing _PepToProtMap.txt file since 'IgnorePeptideToProteinMapError' = True");
                        }
                        else if (m_jobParams.GetJobParameter("SkipProteinMods", false))
                        {
                            LogWarning(
                                "Ignoring missing _PepToProtMap.txt file since 'SkipProteinMods' = True");
                        }
                        else
                        {
                            if (useLegacyMSGFDB)
                            {
                                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                            }

                            // This class will auto-create the PepToProtMap.txt file after the fasta file is retrieved
                            createPepToProtMapFile = true;
                        }
                    }
                    else
                    {
                        if (splitFastaEnabled && !string.IsNullOrWhiteSpace(sourceDir))
                        {
                            var fiPepToProtMapFile = new FileInfo(Path.Combine(sourceDir, pepToProtMapFile));
                            m_jobParams.AddServerFileToDelete(fiPepToProtMapFile.FullName);
                        }
                    }

                    if (splitFastaEnabled)
                    {
                        // Retrieve the _ConsoleOutput file

                        var consoleOutputFile = "MSGFPlus_ConsoleOutput" + suffixToAdd + ".txt";
                        currentStep = "Retrieving " + consoleOutputFile;

                        if (!FileSearch.FindAndRetrieveMiscFiles(consoleOutputFile, unzip: false, searchArchivedDatasetFolder: true, logFileNotFound: false))
                        {
                            // This is not an important error; ignore it
                        }

                        m_jobParams.AddResultFileToSkip(consoleOutputFile);
                    }

                }
            }
            catch (Exception ex)
            {
                LogError("Error in GetMSGFPlusFiles at step " + currentStep, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Note that we'll obtain the MSGF-DB parameter file in RetrieveMiscFiles

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType GetMSAlignFiles()
        {
            var fileToGet = DatasetName + "_MSAlign_ResultTable.txt";
            if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }
            m_jobParams.AddResultFileToSkip(fileToGet);

            // Note that we'll obtain the MSAlign parameter file in RetrieveMiscFiles

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType GetMSPathFinderFiles()
        {
            var fileToGet = DatasetName + "_IcTsv.zip";

            if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, true))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            m_jobParams.AddResultFileToSkip(fileToGet);
            m_jobParams.AddResultFileExtensionToSkip(".tsv");

            // Note that we'll obtain the MSPathFinder parameter file in RetrieveMiscFiles

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        // Deprecated function
        //
        ///// <summary>
        ///// Copies the default Mass Correction Tags file to the working directory
        ///// </summary>
        ///// <returns>True if success, otherwise false</returns>
        ///// <remarks></remarks>
        //protected bool RetrieveDefaultMassCorrectionTagsFile()
        //{
        //    try
        //    {
        //        var paramFileStoragePath = m_jobParams.GetParam("ParmFileStoragePath");
        //        var ioFolderInfo = new DirectoryInfo(paramFileStoragePath).Parent;
        //
        //        var ioSubfolders = ioFolderInfo.GetDirectories("MassCorrectionTags");
        //
        //        if (ioSubfolders.Length == 0)
        //        {
        //            m_message = "MassCorrectionTags folder not found at " + ioFolderInfo.FullName;
        //            LogError(m_message);
        //            return false;
        //        }
        //
        //        var ioFiles = ioSubfolders[0].GetFiles(MASS_CORRECTION_TAGS_FILENAME);
        //        if (ioFiles.Length == 0)
        //        {
        //            m_message = MASS_CORRECTION_TAGS_FILENAME + " file not found at " + ioSubfolders[0].FullName;
        //            LogError(m_message);
        //            return false;
        //        }
        //
        //        if (m_DebugLevel >= 1)
        //        {
        //            LogError("Retrieving default Mass Correction Tags file from " + ioFiles[0].FullName);
        //        }
        //
        //        ioFiles[0].CopyTo(Path.Combine(m_WorkingDir, ioFiles[0].Name));
        //    }
        //    catch (Exception ex)
        //    {
        //        m_message = "Error retrieving " + MASS_CORRECTION_TAGS_FILENAME;
        //        LogError(m_message + ": " + ex.Message);
        //        return false;
        //    }
        //
        //    return true;
        //}

        /// <summary>
        /// Retrieves misc files (i.e., ModDefs) needed for extraction
        /// </summary>
        /// <returns>CloseOutType specifying results</returns>
        /// <remarks></remarks>
        protected internal CloseOutType RetrieveMiscFiles(string resultType)
        {
            var paramFileName = m_jobParams.GetParam("ParmFileName");
            var modDefsFilename = Path.GetFileNameWithoutExtension(paramFileName) + MOD_DEFS_FILE_SUFFIX;

            try
            {
                // Call RetrieveGeneratedParamFile() now to re-create the parameter file, retrieve the _ModDefs.txt file,
                //   and retrieve the MassCorrectionTags.txt file
                // Although the ModDefs file should have been created when SEQUEST, X!Tandem, Inspect, MSGF+, or MSAlign ran,
                //   we re-generate it here just in case T_Param_File_Mass_Mods had missing information
                // Furthermore, we need the search engine parameter file for the PHRPReader

                // Note that the _ModDefs.txt file is obtained using this query:
                //  SELECT Local_Symbol, Monoisotopic_Mass_Correction, Residue_Symbol, Mod_Type_Symbol, Mass_Correction_Tag
                //  FROM V_Param_File_Mass_Mod_Info
                //  WHERE Param_File_Name = 'ParamFileName'

                var success = RetrieveGeneratedParamFile(paramFileName);

                if (!success)
                {
                    LogError("Error retrieving parameter file and ModDefs.txt file");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Confirm that the file was actually created
                var fiModDefsFile = new FileInfo(Path.Combine(m_WorkingDir, modDefsFilename));

                if (!fiModDefsFile.Exists && resultType == RESULT_TYPE_MSPATHFINDER)
                {
                    // MSPathFinder should have already created the ModDefs file during the previous step
                    // Retrieve it from the transfer directory now
                    FileSearch.FindAndRetrieveMiscFiles(modDefsFilename, false);
                    fiModDefsFile.Refresh();
                }

                if (resultType == RESULT_TYPE_XTANDEM)
                {
                    // Retrieve the taxonomy.xml file (PHRPReader uses for it)
                    FileSearch.FindAndRetrieveMiscFiles("taxonomy.xml", false);
                }

                if (!fiModDefsFile.Exists && resultType != RESULT_TYPE_MSALIGN)
                {
                    m_message = "Unable to create the ModDefs.txt file; update T_Param_File_Mass_Mods";
                    LogWarning("Unable to create the ModDefs.txt file; " +
                               "define the modifications in table T_Param_File_Mass_Mods for parameter file " + paramFileName);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                m_jobParams.AddResultFileToSkip(paramFileName);
                m_jobParams.AddResultFileToSkip(MASS_CORRECTION_TAGS_FILENAME);

                var logModFilesFileNotFound = (resultType == RESULT_TYPE_MSALIGN);

                // Check whether the newly generated ModDefs file matches the existing one
                // If it doesn't match, or if the existing one is missing, we need to keep the file
                // Otherwise, we can skip it
                var remoteModDefsDirectory = FileSearch.FindDataFile(modDefsFilename, searchArchivedDatasetFolder: false, logFileNotFound: logModFilesFileNotFound);
                if (string.IsNullOrEmpty(remoteModDefsDirectory))
                {
                    // ModDefs file not found on the server
                    if (fiModDefsFile.Length == 0)
                    {
                        // File is empty; no point in keeping it
                        m_jobParams.AddResultFileToSkip(modDefsFilename);
                    }
                }
                else if (remoteModDefsDirectory.ToLower().StartsWith(@"\\proto"))
                {
                    if (clsGlobal.FilesMatch(fiModDefsFile.FullName, Path.Combine(remoteModDefsDirectory, modDefsFilename)))
                    {
                        m_jobParams.AddResultFileToSkip(modDefsFilename);
                    }
                }

            }
            catch (Exception ex)
            {
                LogError("Error retrieving miscellaneous files", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        protected bool RetrieveToolVersionFile(string resultTypeName)
        {
            bool success;

            try
            {
                // Make sure the ResultType is valid
                var resultType = clsPHRPReader.GetPeptideHitResultType(resultTypeName);

                var toolVersionFile = clsPHRPReader.GetToolVersionInfoFilename(resultType);
                var toolVersionFileNewName = string.Empty;

                var toolNameForScript = m_jobParams.GetJobParameter("ToolName", string.Empty);
                if (resultType == clsPHRPReader.ePeptideHitResultType.MSGFDB && toolNameForScript == "MSGFPlus_IMS")
                {
                    // PeptideListToXML expects the ToolVersion file to be named "Tool_Version_Info_MSGFPlus.txt"
                    // However, this is the MSGFPlus_IMS script, so the file is currently "Tool_Version_Info_MSGFPlus_IMS.txt"
                    // We'll copy the current file locally, then rename it to the expected name
                    toolVersionFileNewName = string.Copy(toolVersionFile);
                    toolVersionFile = "Tool_Version_Info_MSGFPlus_IMS.txt";
                }

                success = FileSearch.FindAndRetrieveMiscFiles(toolVersionFile, false, false);

                if (success && !string.IsNullOrEmpty(toolVersionFileNewName))
                {
                    m_PendingFileRenames.Add(toolVersionFile, toolVersionFileNewName);

                    toolVersionFile = toolVersionFileNewName;
                }
                else if (!success)
                {
                    if (toolVersionFile.ToLower().Contains("msgfplus"))
                    {
                        var toolVersionFileLegacy = "Tool_Version_Info_MSGFDB.txt";
                        success = FileSearch.FindAndRetrieveMiscFiles(toolVersionFileLegacy, false, false);
                        if (success)
                        {
                            // Rename the Tool_Version file to the expected name (Tool_Version_Info_MSGFPlus.txt)
                            m_PendingFileRenames.Add(toolVersionFileLegacy, toolVersionFile);
                            m_jobParams.AddResultFileToSkip(toolVersionFileLegacy);
                        }
                    }
                }

                m_jobParams.AddResultFileToSkip(toolVersionFile);
            }
            catch (Exception ex)
            {
                LogError("Error in RetrieveToolVersionFile: " + ex.Message);
                return false;
            }

            return success;
        }

    }
}
