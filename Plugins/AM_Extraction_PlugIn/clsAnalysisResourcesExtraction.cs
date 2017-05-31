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
using System.Threading;
using AnalysisManagerBase;
using PHRPReader;

namespace AnalysisManagerExtractionPlugin
{
    /// <summary>
    /// Manages retrieval of all files needed for data extraction
    /// </summary>
    /// <remarks></remarks>
    public class clsAnalysisResourcesExtraction : clsAnalysisResources
    {
        #region "Constants"

        public const string MOD_DEFS_FILE_SUFFIX = "_ModDefs.txt";
        public const string MASS_CORRECTION_TAGS_FILENAME = "Mass_Correction_Tags.txt";

        #endregion

        #region "Module variables"

        protected bool mRetrieveOrganismDB;

        // Keys are the original file name, values are the new name
        protected Dictionary<string, string> m_PendingFileRenames;

        #endregion

        #region "Methods"

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

            string strResultType = m_jobParams.GetParam("ResultType");
            var createPepToProtMapFile = false;

            //Get analysis results files
            if (GetInputFiles(strResultType, out createPepToProtMapFile) != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            //Get misc files
            if (RetrieveMiscFiles(strResultType) != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!base.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            foreach (var entry in m_PendingFileRenames)
            {
                FileInfo sourceFile = new FileInfo(Path.Combine(m_WorkingDir, entry.Key));
                if (sourceFile.Exists)
                {
                    sourceFile.MoveTo(Path.Combine(m_WorkingDir, entry.Value));
                }
            }

            if (mRetrieveOrganismDB)
            {
                var blnSkipProteinMods = m_jobParams.GetJobParameter("SkipProteinMods", false);
                if (!blnSkipProteinMods || createPepToProtMapFile)
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
        /// <param name="strResultType">String specifying type of analysis results input to extraction process</param>
        /// <returns>CloseOutType specifying results</returns>
        /// <remarks></remarks>
        private CloseOutType GetInputFiles(string strResultType, out bool createPepToProtMapFile)
        {
            createPepToProtMapFile = false;

            try
            {
                CloseOutType eResult;
                switch (strResultType)
                {
                    case RESULT_TYPE_SEQUEST:
                        eResult = GetSequestFiles();

                        break;
                    case RESULT_TYPE_XTANDEM:
                        eResult = GetXTandemFiles();

                        break;
                    case RESULT_TYPE_INSPECT:
                        eResult = GetInspectFiles();

                        break;
                    case RESULT_TYPE_MSGFPLUS:
                        eResult = GetMSGFPlusFiles(out createPepToProtMapFile);

                        break;
                    case RESULT_TYPE_MSALIGN:
                        eResult = GetMSAlignFiles();

                        break;
                    case RESULT_TYPE_MODA:
                        eResult = GetMODaFiles();

                        break;
                    case RESULT_TYPE_MODPLUS:
                        eResult = GetMODPlusFiles();

                        break;
                    case RESULT_TYPE_MSPATHFINDER:
                        eResult = GetMSPathFinderFiles();

                        m_jobParams.AddResultFileExtensionToSkip(".tsv");

                        break;
                    default:
                        LogError("Invalid tool result type: " + strResultType);
                        return CloseOutType.CLOSEOUT_FAILED;
                }

                if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return eResult;
                }

                RetrieveToolVersionFile(strResultType);
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
            //Get the concatenated .out file
            if (!FileSearch.RetrieveOutFiles(false))
            {
                //Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_NO_OUT_FILES;
            }

            // Note that we'll obtain the Sequest parameter file in RetrieveMiscFiles

            //Add all the extensions of the files to delete after run
            m_jobParams.AddResultFileExtensionToSkip("_dta.zip");    //Zipped DTA
            m_jobParams.AddResultFileExtensionToSkip("_dta.txt");    //Unzipped, concatenated DTA
            m_jobParams.AddResultFileExtensionToSkip("_out.zip");    //Zipped OUT
            m_jobParams.AddResultFileExtensionToSkip("_out.txt");    //Unzipped, concatenated OUT
            m_jobParams.AddResultFileExtensionToSkip(".dta");        //DTA files
            m_jobParams.AddResultFileExtensionToSkip(".out");        //DTA files

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType GetXTandemFiles()
        {
            string fileToGet = null;

            fileToGet = DatasetName + "_xt.zip";
            if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, true))
            {
                //Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_NO_XT_FILES;
            }
            m_jobParams.AddResultFileToSkip(fileToGet);

            //Manually adding this file to FilesToDelete; we don't want the unzipped .xml file to be copied to the server
            m_jobParams.AddResultFileToSkip(DatasetName + "_xt.xml");

            // Note that we'll obtain the X!Tandem parameter file in RetrieveMiscFiles

            // However, we need to obtain the "input.xml" file and "default_input.xml" files now
            fileToGet = "input.xml";
            if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                //Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }
            m_jobParams.AddResultFileToSkip(fileToGet);

            if (!CopyFileToWorkDir("default_input.xml", m_jobParams.GetParam("ParmFileStoragePath"), m_WorkingDir))
            {
                const string Msg = "Failed retrieving default_input.xml file.";
                LogError(Msg);
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType GetInspectFiles()
        {
            string fileToGet = null;

            // Get the zipped Inspect results files

            // This file contains the p-value filtered results
            fileToGet = DatasetName + "_inspect.zip";
            if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                //Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_NO_INSP_FILES;
            }
            m_jobParams.AddResultFileToSkip(fileToGet);

            // This file contains top hit for each scan (no filters)
            fileToGet = DatasetName + "_inspect_fht.zip";
            if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                //Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_NO_INSP_FILES;
            }
            m_jobParams.AddResultFileToSkip(fileToGet);

            // Get the peptide to protein mapping file
            fileToGet = DatasetName + "_inspect_PepToProtMap.txt";
            if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                //Errors were reported in function call

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
            string fileToGet = null;

            fileToGet = DatasetName + "_moda.zip";
            if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, true))
            {
                //Errors were reported in function call, so just return
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
            string fileToGet = null;

            fileToGet = DatasetName + "_modp.zip";
            if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, true))
            {
                //Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }
            m_jobParams.AddResultFileToSkip(fileToGet);
            m_jobParams.AddResultFileExtensionToSkip("_modp.txt");

            Thread.Sleep(100);

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

            bool blnUseLegacyMSGFDB = false;
            var splitFastaEnabled = m_jobParams.GetJobParameter("SplitFasta", false);
            string suffixToAdd = null;
            string mzidSuffix = null;

            var numberOfClonedSteps = 1;

            try
            {
                if (splitFastaEnabled)
                {
                    numberOfClonedSteps = m_jobParams.GetJobParameter("NumberOfClonedSteps", 0);
                    if (numberOfClonedSteps == 0)
                    {
                        LogError(
                            "Settings file is missing parameter NumberOfClonedSteps; cannot retrieve MSGFPlus results");
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    suffixToAdd = "_Part1";
                }
                else
                {
                    suffixToAdd = string.Empty;
                }

                string SourceFolderPath = null;
                currentStep = "Determining results file type based on the results file name";
                blnUseLegacyMSGFDB = false;

                var fileToFind = DatasetName + "_msgfplus" + suffixToAdd + ".mzid.gz";
                SourceFolderPath = FileSearch.FindDataFile(fileToFind, true, false);
                if (!string.IsNullOrEmpty(SourceFolderPath))
                {
                    // Running MSGF+ with gzipped results
                    mzidSuffix = ".mzid.gz";
                }
                else
                {
                    // File not found; look for _msgfdb.mzid.gz
                    var fileToGetAlternative = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(fileToFind, "Dataset_msgfdb.txt");
                    SourceFolderPath = FileSearch.FindDataFile(fileToGetAlternative, true, false);

                    if (!string.IsNullOrEmpty(SourceFolderPath))
                    {
                        // Running MSGF+ with gzipped results
                        mzidSuffix = ".mzid.gz";
                    }
                    else
                    {
                        // File not found; look for a .zip file
                        SourceFolderPath = FileSearch.FindDataFile(DatasetName + "_msgfplus" + suffixToAdd + ".zip", true, false);
                        if (!string.IsNullOrEmpty(SourceFolderPath))
                        {
                            // Running MSGF+ with zipped results
                            mzidSuffix = ".zip";
                        }
                        else
                        {
                            // File not found; try _msgfdb
                            SourceFolderPath = FileSearch.FindDataFile(DatasetName + "_msgfdb" + suffixToAdd + ".zip", true, false);
                            if (!string.IsNullOrEmpty(SourceFolderPath))
                            {
                                // File Found
                                blnUseLegacyMSGFDB = true;
                                mzidSuffix = ".zip";
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

                // ReSharper disable once UseImplicitlyTypedVariableEvident

                for (int iteration = 1; iteration <= numberOfClonedSteps; iteration++)
                {
                    var blnSkipMSGFResultsZipFileCopy = false;
                    string fileToGet = null;
                    string strBaseName = null;

                    if (splitFastaEnabled)
                    {
                        suffixToAdd = "_Part" + iteration;
                    }
                    else
                    {
                        suffixToAdd = string.Empty;
                    }

                    if (blnUseLegacyMSGFDB)
                    {
                        strBaseName = DatasetName + "_msgfdb";

                        if (splitFastaEnabled)
                        {
                            LogError("GetMSGFPlusFiles does not support SplitFasta mode for legacy MSGF-DB results");
                            return CloseOutType.CLOSEOUT_FAILED;
                        }
                    }
                    else
                    {
                        strBaseName = DatasetName + "_msgfplus" + suffixToAdd;

                        fileToGet = DatasetName + "_msgfplus" + suffixToAdd + ".tsv";
                        currentStep = "Retrieving " + fileToGet;

                        SourceFolderPath = FileSearch.FindDataFile(fileToGet, false, false);
                        if (string.IsNullOrEmpty(SourceFolderPath))
                        {
                            var fileToGetAlternative = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(fileToGet, "Dataset_msgfdb.txt");
                            SourceFolderPath = FileSearch.FindDataFile(fileToGetAlternative, false, false);
                            if (!string.IsNullOrEmpty(SourceFolderPath))
                            {
                                fileToGet = fileToGetAlternative;
                            }
                        }

                        if (!string.IsNullOrEmpty(SourceFolderPath))
                        {
                            if (!SourceFolderPath.StartsWith(MYEMSL_PATH_FLAG))
                            {
                                // Examine the date of the TSV file
                                // If less than 4 hours old, retrieve it; otherwise, grab the _msgfplus.mzid.gz file and re-generate the .tsv file

                                var fiTSVFile = new FileInfo(Path.Combine(SourceFolderPath, fileToGet));
                                if (System.DateTime.UtcNow.Subtract(fiTSVFile.LastWriteTimeUtc).TotalHours < 4)
                                {
                                    // File is recent; grab it
                                    if (!CopyFileToWorkDir(fileToGet, SourceFolderPath, m_WorkingDir))
                                    {
                                        // File copy failed; that's OK; we'll grab the _msgfplus.mzid.gz file
                                    }
                                    else
                                    {
                                        blnSkipMSGFResultsZipFileCopy = true;
                                        m_jobParams.AddResultFileToSkip(fileToGet);
                                    }
                                }

                                m_jobParams.AddServerFileToDelete(fiTSVFile.FullName);
                            }
                        }
                    }

                    if (!blnSkipMSGFResultsZipFileCopy)
                    {
                        fileToGet = strBaseName + mzidSuffix;
                        currentStep = "Retrieving " + fileToGet;

                        if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, true))
                        {
                            //Errors were reported in function call, so just return
                            return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                        }
                        m_jobParams.AddResultFileToSkip(fileToGet);
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
                    fileToGet = DatasetName + "_msgfplus" + suffixToAdd + "_PepToProtMap.txt";
                    currentStep = "Retrieving " + fileToGet;

                    if (!FileSearch.FindAndRetrievePHRPDataFile(ref fileToGet, ""))
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
                            if (blnUseLegacyMSGFDB)
                            {
                                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                            }
                            else
                            {
                                // This class will auto-create the PepToProtMap.txt file after the fasta file is retrieved
                                createPepToProtMapFile = true;
                            }
                        }
                    }
                    else
                    {
                        if (splitFastaEnabled)
                        {
                            var fiPepToProtMapFile = new FileInfo(Path.Combine(SourceFolderPath, fileToGet));
                            m_jobParams.AddServerFileToDelete(fiPepToProtMapFile.FullName);
                        }
                    }

                    if (splitFastaEnabled)
                    {
                        // Retrieve the _ConsoleOutput file

                        fileToGet = "MSGFPlus_ConsoleOutput" + suffixToAdd + ".txt";
                        currentStep = "Retrieving " + fileToGet;

                        if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, false))
                        {
                            // This is not an important error; ignore it
                        }
                    }

                    m_jobParams.AddResultFileToSkip(fileToGet);
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
            string fileToGet = null;

            fileToGet = DatasetName + "_MSAlign_ResultTable.txt";
            if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                //Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }
            m_jobParams.AddResultFileToSkip(fileToGet);

            // Note that we'll obtain the MSAlign parameter file in RetrieveMiscFiles

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType GetMSPathFinderFiles()
        {
            string fileToGet = null;

            fileToGet = DatasetName + "_IcTsv.zip";

            if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, true))
            {
                //Errors were reported in function call, so just return
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
        //        var strParamFileStoragePath = m_jobParams.GetParam("ParmFileStoragePath");
        //        var ioFolderInfo = new DirectoryInfo(strParamFileStoragePath).Parent;
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
        //            LogError(
        //                "Retrieving default Mass Correction Tags file from " + ioFiles[0].FullName);
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
        protected internal CloseOutType RetrieveMiscFiles(string ResultType)
        {
            string strParamFileName = m_jobParams.GetParam("ParmFileName");
            string ModDefsFilename = Path.GetFileNameWithoutExtension(strParamFileName) + MOD_DEFS_FILE_SUFFIX;

            bool blnSuccess = false;

            try
            {
                // Call RetrieveGeneratedParamFile() now to re-create the parameter file, retrieve the _ModDefs.txt file, 
                //   and retrieve the MassCorrectionTags.txt file
                // Although the ModDefs file should have been created when Sequest, X!Tandem, Inspect, MSGFDB, or MSAlign ran, 
                //   we re-generate it here just in case T_Param_File_Mass_Mods had missing information
                // Furthermore, we need the search engine parameter file for the PHRPReader

                // Note that the _ModDefs.txt file is obtained using this query:
                //  SELECT Local_Symbol, Monoisotopic_Mass_Correction, Residue_Symbol, Mod_Type_Symbol, Mass_Correction_Tag
                //  FROM V_Param_File_Mass_Mod_Info
                //  WHERE Param_File_Name = 'ParamFileName'

                blnSuccess = RetrieveGeneratedParamFile(strParamFileName);

                if (!blnSuccess)
                {
                    LogError("Error retrieving parameter file and ModDefs.txt file");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Confirm that the file was actually created
                var fiModDefsFile = new FileInfo(Path.Combine(m_WorkingDir, ModDefsFilename));

                if (!fiModDefsFile.Exists && ResultType == RESULT_TYPE_MSPATHFINDER)
                {
                    // MSPathFinder should have already created the ModDefs file during the previous step
                    // Retrieve it from the transfer folder now
                    FileSearch.FindAndRetrieveMiscFiles(ModDefsFilename, false);
                    fiModDefsFile.Refresh();
                }

                if (ResultType == RESULT_TYPE_XTANDEM)
                {
                    // Retrieve the taxonomy.xml file (PHRPReader looks for it)
                    FileSearch.FindAndRetrieveMiscFiles("taxonomy.xml", false);
                }

                if (!fiModDefsFile.Exists && ResultType != RESULT_TYPE_MSALIGN)
                {
                    m_message = "Unable to create the ModDefs.txt file; update T_Param_File_Mass_Mods";
                    LogWarning(
                        "Unable to create the ModDefs.txt file; define the modifications in table T_Param_File_Mass_Mods for parameter file " +
                        strParamFileName);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                m_jobParams.AddResultFileToSkip(strParamFileName);
                m_jobParams.AddResultFileToSkip(MASS_CORRECTION_TAGS_FILENAME);

                var logModFilesFileNotFound = (ResultType == RESULT_TYPE_MSALIGN);

                // Check whether the newly generated ModDefs file matches the existing one
                // If it doesn't match, or if the existing one is missing, then we need to keep the file
                // Otherwise, we can skip it
                var remoteModDefsFolder = FileSearch.FindDataFile(ModDefsFilename, searchArchivedDatasetFolder: false, logFileNotFound: logModFilesFileNotFound);
                if (string.IsNullOrEmpty(remoteModDefsFolder))
                {
                    // ModDefs file not found on the server
                    if (fiModDefsFile.Length == 0)
                    {
                        // File is empty; no point in keeping it
                        m_jobParams.AddResultFileToSkip(ModDefsFilename);
                    }
                }
                else if (remoteModDefsFolder.ToLower().StartsWith(@"\\proto"))
                {
                    if (clsGlobal.FilesMatch(fiModDefsFile.FullName, Path.Combine(remoteModDefsFolder, ModDefsFilename)))
                    {
                        m_jobParams.AddResultFileToSkip(ModDefsFilename);
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

        protected bool RetrieveToolVersionFile(string strResultType)
        {
            bool blnSuccess = false;

            try
            {
                // Make sure the ResultType is valid
                var eResultType = PHRPReader.clsPHRPReader.GetPeptideHitResultType(strResultType);

                string strToolVersionFile = PHRPReader.clsPHRPReader.GetToolVersionInfoFilename(eResultType);
                string strToolVersionFileNewName = string.Empty;

                string strToolNameForScript = m_jobParams.GetJobParameter("ToolName", string.Empty);
                if (eResultType == PHRPReader.clsPHRPReader.ePeptideHitResultType.MSGFDB && strToolNameForScript == "MSGFPlus_IMS")
                {
                    // PeptideListToXML expects the ToolVersion file to be named "Tool_Version_Info_MSGFPlus.txt"
                    // However, this is the MSGFPlus_IMS script, so the file is currently "Tool_Version_Info_MSGFPlus_IMS.txt"
                    // We'll copy the current file locally, then rename it to the expected name
                    strToolVersionFileNewName = string.Copy(strToolVersionFile);
                    strToolVersionFile = "Tool_Version_Info_MSGFPlus_IMS.txt";
                }

                blnSuccess = FileSearch.FindAndRetrieveMiscFiles(strToolVersionFile, false, false);

                if (blnSuccess && !string.IsNullOrEmpty(strToolVersionFileNewName))
                {
                    m_PendingFileRenames.Add(strToolVersionFile, strToolVersionFileNewName);

                    strToolVersionFile = strToolVersionFileNewName;
                }
                else if (!blnSuccess)
                {
                    if (strToolVersionFile.ToLower().Contains("msgfplus"))
                    {
                        var strToolVersionFileLegacy = "Tool_Version_Info_MSGFDB.txt";
                        blnSuccess = FileSearch.FindAndRetrieveMiscFiles(strToolVersionFileLegacy, false, false);
                        if (blnSuccess)
                        {
                            // Rename the Tool_Version file to the expected name (Tool_Version_Info_MSGFPlus.txt)
                            m_PendingFileRenames.Add(strToolVersionFileLegacy, strToolVersionFile);
                            m_jobParams.AddResultFileToSkip(strToolVersionFileLegacy);
                        }
                    }
                }

                m_jobParams.AddResultFileToSkip(strToolVersionFile);
            }
            catch (Exception ex)
            {
                LogError("Error in RetrieveToolVersionFile: " + ex.Message);
                return false;
            }

            return blnSuccess;
        }

        #endregion
    }
}
