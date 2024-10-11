using System;
using System.IO;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.StatusReporting;

namespace AnalysisManagerXTandemPlugIn
{
    /// <summary>
    /// Retrieve resources for the X!Tandem plugin
    /// </summary>
    public class AnalysisResourcesXT : AnalysisResources
    {
        internal const string MOD_DEFS_FILE_SUFFIX = "_ModDefs.txt";
        internal const string MASS_CORRECTION_TAGS_FILENAME = "Mass_Correction_Tags.txt";

        private CondenseCDTAFile.clsCDTAFileCondenser mCDTACondenser;

        /// <summary>
        /// Initialize options
        /// </summary>
        public override void Setup(string stepToolName, IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, MyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(stepToolName, mgrParams, jobParams, statusTools, myEMSLUtilities);
            SetOption(Global.AnalysisResourceOptions.OrgDbRequired, true);
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

            // Retrieve FASTA file
            var orgDbDirectoryPath = mMgrParams.GetParam("OrgDbDir");

            if (!RetrieveOrgDB(orgDbDirectoryPath, out var resultCode))
                return resultCode;

            // XTandem just copies its parameter file from the central repository
            LogMessage("Getting param file");

            // Retrieve param file
            if (!RetrieveGeneratedParamFile(mJobParams.GetParam("ParamFileName")))
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // Retrieve the _DTA.txt file
            // Note that if the file was found in MyEMSL then RetrieveDtaFiles will auto-call ProcessMyEMSLDownloadQueue to download the file
            if (!FileSearchTool.RetrieveDtaFiles())
            {
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // Make sure the _DTA.txt file has parent ion lines with text: scan=x and cs=y
            // X!Tandem uses this information to determine the scan number
            var cdtaPath = Path.Combine(mWorkDir, DatasetName + "_dta.txt");
            const bool REPLACE_SOURCE_FILE = true;
            const bool DELETE_SOURCE_FILE_IF_UPDATED = true;

            if (!ValidateCDTAFileScanAndCSTags(cdtaPath, REPLACE_SOURCE_FILE, DELETE_SOURCE_FILE_IF_UPDATED, ""))
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
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            var success = CopyFileToWorkDir("taxonomy_base.xml", mJobParams.GetParam("ParamFileStoragePath"), mWorkDir);

            if (!success)
            {
                LogError("AnalysisResourcesXT.GetResources(), failed retrieving taxonomy_base.xml file");
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            success = CopyFileToWorkDir("input_base.txt", mJobParams.GetParam("ParamFileStoragePath"), mWorkDir);

            if (!success)
            {
                LogError("AnalysisResourcesXT.GetResources(), failed retrieving input_base.xml file");
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            success = CopyFileToWorkDir("default_input.xml", mJobParams.GetParam("ParamFileStoragePath"), mWorkDir);

            if (!success)
            {
                LogError("AnalysisResourcesXT.GetResources(), failed retrieving default_input.xml file");
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            if (!ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // set up taxonomy file to reference the organism DB file (FASTA)
            success = MakeTaxonomyFile();

            if (!success)
            {
                LogError("AnalysisResourcesXT.GetResources(), failed making taxonomy file");
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // set up run parameter file to reference spectra file, taxonomy file, and analysis parameter file
            success = MakeInputFile();

            if (!success)
            {
                LogError("AnalysisResourcesXT.GetResources(), failed making input file");
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private bool MakeTaxonomyFile()
        {
            // set up taxonomy file to reference the organism DB file (FASTA)

            var workingDir = mMgrParams.GetParam("WorkDir");
            var orgDBName = mJobParams.GetParam(AnalysisJob.PEPTIDE_SEARCH_SECTION, "GeneratedFastaName");
            var organismName = mJobParams.GetParam("OrganismName");
            var localOrgDBFolder = mMgrParams.GetParam("OrgDbDir");
            var orgFilePath = Path.Combine(localOrgDBFolder, orgDBName);

            // Edit base taxonomy file into actual
            try
            {
                // Create an instance of StreamWriter to write to a file.
                using var taxonomyWriter = new StreamWriter(Path.Combine(workingDir, "taxonomy.xml"));
                using var baseFileReader = new StreamReader(Path.Combine(workingDir, "taxonomy_base.xml"));

                while (!baseFileReader.EndOfStream)
                {
                    var dataLine = baseFileReader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    var updatedLine = dataLine
                        .Replace("ORGANISM_NAME", organismName)
                        .Replace("FASTA_FILE_PATH", orgFilePath);

                    taxonomyWriter.WriteLine(updatedLine);
                }
            }
            catch (Exception ex)
            {
                // Let the user know what went wrong.
                LogError("AnalysisResourcesXT.MakeTaxonomyFile, The file could not be read" + ex.Message);
            }

            // Get rid of base file
            File.Delete(Path.Combine(workingDir, "taxonomy_base.xml"));

            return true;
        }

        private bool MakeInputFile()
        {
            var result = true;

            // set up input to reference spectra file, taxonomy file, and parameter file

            var workingDir = mMgrParams.GetParam("WorkDir");
            var organismName = mJobParams.GetParam("OrganismName");
            var paramFilePath = Path.Combine(workingDir, mJobParams.GetParam("ParamFileName"));
            var spectrumFilePath = Path.Combine(workingDir, DatasetName + "_dta.txt");
            var taxonomyFilePath = Path.Combine(workingDir, "taxonomy.xml");
            var outputFilePath = Path.Combine(workingDir, DatasetName + "_xt.xml");

            // Make input file
            // Start by adding the contents of the parameter file.
            // Replace substitution tags in input_base.txt with proper file path references
            // and add to input file (in proper XML format)
            try
            {
                // Create an instance of StreamWriter to write to a file.
                using var inputFileWriter = new StreamWriter(Path.Combine(workingDir, "input.xml"));
                using var baseReader = new StreamReader(Path.Combine(workingDir, "input_base.txt"));
                using var paramFileReader = new StreamReader(paramFilePath);

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
                            .Replace("ORGANISM_NAME", organismName)
                            .Replace("TAXONOMY_FILE_PATH", taxonomyFilePath)
                            .Replace("SPECTRUM_FILE_PATH", spectrumFilePath)
                            .Replace("OUTPUT_FILE_PATH", outputFilePath);

                        inputFileWriter.WriteLine(updatedLine);
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
            File.Delete(Path.Combine(workingDir, "input_base.txt"));

            return result;
        }

        internal static string ConstructModificationDefinitionsFilename(string parameterFileName)
        {
            return Path.GetFileNameWithoutExtension(parameterFileName) + MOD_DEFS_FILE_SUFFIX;
        }

        private bool ValidateDTATextFileSize(string workDir, string inputFileName)
        {
            const int FILE_SIZE_THRESHOLD = int.MaxValue;

            try
            {
                var inputFilePath = Path.Combine(workDir, inputFileName);
                var inputFile = new FileInfo(inputFilePath);

                if (!inputFile.Exists)
                {
                    mMessage = "_DTA.txt file not found: " + inputFilePath;
                    LogError(mMessage);
                    return false;
                }

                if (inputFile.Length >= FILE_SIZE_THRESHOLD)
                {
                    // Need to condense the file

                    var message = string.Format("{0} is {1:F2} GB in size; will now condense it by combining data points with consecutive zero-intensity value",
                        inputFile.Name, Global.BytesToGB(inputFile.Length));

                    LogMessage(message);

                    mCDTACondenser = new CondenseCDTAFile.clsCDTAFileCondenser();
                    mCDTACondenser.ProgressChanged += CDTACondenser_ProgressChanged;

                    var success = mCDTACondenser.ProcessFile(inputFile.FullName, inputFile.DirectoryName);

                    if (!success)
                    {
                        mMessage = "Error condensing _DTA.txt file: " + mCDTACondenser.GetErrorMessage();
                        LogError(mMessage);
                        return false;
                    }

                    // Check the size of the new _dta.txt file
                    inputFile.Refresh();

                    if (mDebugLevel >= 1)
                    {
                        message = string.Format("Condensing complete; size of the new _dta.txt file is {0:F2} GB",
                                                   Global.BytesToGB(inputFile.Length));
                        LogMessage(message);
                    }

                    try
                    {
                        var filePathOld = Path.Combine(workDir, Path.GetFileNameWithoutExtension(inputFile.FullName) + "_Old.txt");

                        if (mDebugLevel >= 2)
                        {
                            message = "Now deleting file " + filePathOld;
                            LogMessage(message);
                        }

                        inputFile = new FileInfo(filePathOld);

                        if (inputFile.Exists)
                        {
                            inputFile.Delete();
                        }
                        else
                        {
                            message = "Old _DTA.txt file not found:" + inputFile.FullName + "; cannot delete";
                            LogWarning(message);
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
            // ReSharper disable once ConvertIfStatementToSwitchStatement
            if (mDebugLevel < 1)
                return;

            // ReSharper disable once InvertIf
            if (mDebugLevel == 1 && DateTime.UtcNow.Subtract(mLastUpdateTime).TotalSeconds >= 60 ||
                mDebugLevel > 1 && DateTime.UtcNow.Subtract(mLastUpdateTime).TotalSeconds >= 20)
            {
                mLastUpdateTime = DateTime.UtcNow;

                LogDebug(" ... " + percentComplete.ToString("0.00") + "% complete");
            }
        }
    }
}