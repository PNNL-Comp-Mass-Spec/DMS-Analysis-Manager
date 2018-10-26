using System;
using System.IO;
using AnalysisManagerBase;

namespace AnalysisManagerXTandemPlugIn
{
    /// <summary>
    /// Retrieve resources for the X!Tandem plugin
    /// </summary>
    public class clsAnalysisResourcesXT : clsAnalysisResources
    {
        internal const string MOD_DEFS_FILE_SUFFIX = "_ModDefs.txt";
        internal const string MASS_CORRECTION_TAGS_FILENAME = "Mass_Correction_Tags.txt";

        private CondenseCDTAFile.clsCDTAFileCondenser mCDTACondenser;

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
            var orgDbDirectoryPath = mMgrParams.GetParam("OrgDbDir");
            if (!RetrieveOrgDB(orgDbDirectoryPath, out var resultCode))
                return resultCode;

            // XTandem just copies its parameter file from the central repository
            LogMessage("Getting param file");

            // Retrieve param file
            if (!RetrieveGeneratedParamFile(mJobParams.GetParam("ParmFileName")))
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // Retrieve the _DTA.txt file
            // Note that if the file was found in MyEMSL then RetrieveDtaFiles will auto-call ProcessMyEMSLDownloadQueue to download the file
            if (!FileSearch.RetrieveDtaFiles())
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // Make sure the _DTA.txt file has parent ion lines with text: scan=x and cs=y
            // X!Tandem uses this information to determine the scan number
            var strCDTAPath = Path.Combine(mWorkDir, DatasetName + "_dta.txt");
            const bool blnReplaceSourceFile = true;
            const bool blnDeleteSourceFileIfUpdated = true;

            if (!ValidateCDTAFileScanAndCSTags(strCDTAPath, blnReplaceSourceFile, blnDeleteSourceFileIfUpdated, ""))
            {
                mMessage = "Error validating the _DTA.txt file";
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // Add all the extensions of the files to delete after run
            mJobParams.AddResultFileExtensionToSkip("_dta.zip"); // Zipped DTA
            mJobParams.AddResultFileExtensionToSkip("_dta.txt"); // Unzipped, concatenated DTA
            mJobParams.AddResultFileExtensionToSkip(".dta");     // DTA files

            // If the _dta.txt file is over 2 GB in size, condense it

            if (!ValidateDTATextFileSize(mWorkDir, DatasetName + "_dta.txt"))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            var success = CopyFileToWorkDir("taxonomy_base.xml", mJobParams.GetParam("ParmFileStoragePath"), mWorkDir);
            if (!success)
            {
                LogError("clsAnalysisResourcesXT.GetResources(), failed retrieving taxonomy_base.xml file");
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            success = CopyFileToWorkDir("input_base.txt", mJobParams.GetParam("ParmFileStoragePath"), mWorkDir);
            if (!success)
            {
                LogError("clsAnalysisResourcesXT.GetResources(), failed retrieving input_base.xml file");
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            success = CopyFileToWorkDir("default_input.xml", mJobParams.GetParam("ParmFileStoragePath"), mWorkDir);
            if (!success)
            {
                LogError("clsAnalysisResourcesXT.GetResources(), failed retrieving default_input.xml file");
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            if (!ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // set up taxonomy file to reference the organism DB file (fasta)
            success = MakeTaxonomyFile();
            if (!success)
            {
                LogError("clsAnalysisResourcesXT.GetResources(), failed making taxonomy file");
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // set up run parameter file to reference spectra file, taxonomy file, and analysis parameter file
            success = MakeInputFile();
            if (!success)
            {
                LogError("clsAnalysisResourcesXT.GetResources(), failed making input file");
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        protected bool MakeTaxonomyFile()
        {
            // set up taxonomy file to reference the organsim DB file (fasta)

            var WorkingDir = mMgrParams.GetParam("WorkDir");
            var OrgDBName = mJobParams.GetParam("PeptideSearch", "generatedFastaName");
            var OrganismName = mJobParams.GetParam("OrganismName");
            var LocalOrgDBFolder = mMgrParams.GetParam("OrgDbDir");
            var OrgFilePath = Path.Combine(LocalOrgDBFolder, OrgDBName);

            // Edit base taxonomy file into actual
            try
            {
                // Create an instance of StreamWriter to write to a file.
                using (var taxonomyWriter = new StreamWriter(Path.Combine(WorkingDir, "taxonomy.xml")))
                using (var baseFileReader = new StreamReader(Path.Combine(WorkingDir, "taxonomy_base.xml")))
                {
                    while (!baseFileReader.EndOfStream)
                    {
                        var dataLine = baseFileReader.ReadLine();
                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        var updatedLine = dataLine
                            .Replace("ORGANISM_NAME", OrganismName)
                            .Replace("FASTA_FILE_PATH", OrgFilePath);

                        taxonomyWriter.WriteLine(updatedLine);
                    }

                }

            }
            catch (Exception ex)
            {
                // Let the user know what went wrong.
                LogError("clsAnalysisResourcesXT.MakeTaxonomyFile, The file could not be read" + ex.Message);
            }

            // Get rid of base file
            File.Delete(Path.Combine(WorkingDir, "taxonomy_base.xml"));

            return true;
        }

        protected bool MakeInputFile()
        {
            var result = true;

            // set up input to reference spectra file, taxonomy file, and parameter file

            var WorkingDir = mMgrParams.GetParam("WorkDir");
            var OrganismName = mJobParams.GetParam("OrganismName");
            var ParamFilePath = Path.Combine(WorkingDir, mJobParams.GetParam("parmFileName"));
            var SpectrumFilePath = Path.Combine(WorkingDir, DatasetName + "_dta.txt");
            var TaxonomyFilePath = Path.Combine(WorkingDir, "taxonomy.xml");
            var OutputFilePath = Path.Combine(WorkingDir, DatasetName + "_xt.xml");

            // Make input file
            // Start by adding the contents of the parameter file.
            // Replace substitution tags in input_base.txt with proper file path references
            // and add to input file (in proper XML format)
            try
            {
                // Create an instance of StreamWriter to write to a file.
                using (var inputFileWriter = new StreamWriter(Path.Combine(WorkingDir, "input.xml")))
                using (var baseReader = new StreamReader(Path.Combine(WorkingDir, "input_base.txt")))
                using (var paramFileReader = new StreamReader(ParamFilePath))
                {
                    while (!paramFileReader.EndOfStream)
                    {
                        var paramLine = paramFileReader.ReadLine();
                        if (string.IsNullOrWhiteSpace(paramLine))
                            continue;

                        inputFileWriter.WriteLine(paramLine);

                        if (paramLine.IndexOf("<bioml>", StringComparison.Ordinal) == -1)
                            continue;

                        while (!baseReader.EndOfStream)
                        {
                            var baseLine = baseReader.ReadLine();
                            if (string.IsNullOrWhiteSpace(baseLine))
                                continue;

                            var updatedLine = baseLine
                                .Replace("ORGANISM_NAME", OrganismName)
                                .Replace("TAXONOMY_FILE_PATH", TaxonomyFilePath)
                                .Replace("SPECTRUM_FILE_PATH", SpectrumFilePath)
                                .Replace("OUTPUT_FILE_PATH", OutputFilePath);

                            inputFileWriter.WriteLine(updatedLine);
                        }

                    }
                }

            }
            catch (Exception ex)
            {
                // Let the user know what went wrong.
                LogError("clxAnalysisResourcesXT.MakeInputFile, The file could not be read" + ex.Message);
                result = false;
            }

            // Get rid of base file
            File.Delete(Path.Combine(WorkingDir, "input_base.txt"));

            return result;
        }

        internal static string ConstructModificationDefinitionsFilename(string ParameterFileName)
        {
            return Path.GetFileNameWithoutExtension(ParameterFileName) + MOD_DEFS_FILE_SUFFIX;
        }

        protected bool ValidateDTATextFileSize(string strWorkDir, string strInputFileName)
        {
            const int FILE_SIZE_THRESHOLD = int.MaxValue;

            try
            {
                var strInputFilePath = Path.Combine(strWorkDir, strInputFileName);
                var ioFileInfo = new FileInfo(strInputFilePath);

                if (!ioFileInfo.Exists)
                {
                    mMessage = "_DTA.txt file not found: " + strInputFilePath;
                    LogError(mMessage);
                    return false;
                }

                if (ioFileInfo.Length >= FILE_SIZE_THRESHOLD)
                {
                    // Need to condense the file

                    var strMessage = string.Format("{0} is {1:F2} GB in size; will now condense it by combining data points with consecutive zero-intensity value",
                                                   ioFileInfo.Name, clsGlobal.BytesToGB(ioFileInfo.Length));

                    LogMessage(strMessage);

                    mCDTACondenser = new CondenseCDTAFile.clsCDTAFileCondenser();
                    mCDTACondenser.ProgressChanged += CDTACondenser_ProgressChanged;

                    var blnSuccess = mCDTACondenser.ProcessFile(ioFileInfo.FullName, ioFileInfo.DirectoryName);

                    if (!blnSuccess)
                    {
                        mMessage = "Error condensing _DTA.txt file: " + mCDTACondenser.GetErrorMessage();
                        LogError(mMessage);
                        return false;
                    }

                    // Check the size of the new _dta.txt file
                    ioFileInfo.Refresh();

                    if (mDebugLevel >= 1)
                    {
                        strMessage = string.Format("Condensing complete; size of the new _dta.txt file is {0:F2} GB",
                                                   clsGlobal.BytesToGB(ioFileInfo.Length));
                        LogMessage(strMessage);
                    }

                    try
                    {
                        var strFilePathOld = Path.Combine(strWorkDir, Path.GetFileNameWithoutExtension(ioFileInfo.FullName) + "_Old.txt");

                        if (mDebugLevel >= 2)
                        {
                            strMessage = "Now deleting file " + strFilePathOld;
                            LogMessage(strMessage);
                        }

                        ioFileInfo = new FileInfo(strFilePathOld);
                        if (ioFileInfo.Exists)
                        {
                            ioFileInfo.Delete();
                        }
                        else
                        {
                            strMessage = "Old _DTA.txt file not found:" + ioFileInfo.FullName + "; cannot delete";
                            LogWarning(strMessage);
                        }
                    }
                    catch (Exception ex)
                    {
                        // Error deleting the file; log it but keep processing
                        LogError("Exception deleting _dta_old.txt file: " + ex.Message);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in ValidateDTATextFileSize";
                LogError(mMessage + ": " + ex.Message);
                return false;
            }

        }

        private DateTime mLastUpdateTime;

        private void CDTACondenser_ProgressChanged(string taskDescription, float percentComplete)
        {
            if (mDebugLevel >= 1)
            {
                if (mDebugLevel == 1 && DateTime.UtcNow.Subtract(mLastUpdateTime).TotalSeconds >= 60 ||
                    mDebugLevel > 1 && DateTime.UtcNow.Subtract(mLastUpdateTime).TotalSeconds >= 20)
                {
                    mLastUpdateTime = DateTime.UtcNow;

                    LogDebug(" ... " + percentComplete.ToString("0.00") + "% complete");
                }
            }
        }
    }
}