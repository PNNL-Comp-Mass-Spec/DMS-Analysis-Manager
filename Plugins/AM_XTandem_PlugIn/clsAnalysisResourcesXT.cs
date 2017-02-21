using System;
using System.IO;
using System.Threading;
using AnalysisManagerBase;

namespace AnalysisManagerXTandemPlugIn
{
    public class clsAnalysisResourcesXT : clsAnalysisResources
    {
        internal const string MOD_DEFS_FILE_SUFFIX = "_ModDefs.txt";
        internal const string MASS_CORRECTION_TAGS_FILENAME = "Mass_Correction_Tags.txt";

        private CondenseCDTAFile.clsCDTAFileCondenser mCDTACondenser;

        public override void Setup(IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, clsMyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(mgrParams, jobParams, statusTools, myEMSLUtilities);
            SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, true);
        }

        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            //Retrieve Fasta file
            if (!RetrieveOrgDB(m_mgrParams.GetParam("orgdbdir")))
                return CloseOutType.CLOSEOUT_FAILED;

            // XTandem just copies its parameter file from the central repository
            LogMessage("Getting param file");

            //Retrieve param file
            if (!RetrieveGeneratedParamFile(m_jobParams.GetParam("ParmFileName")))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Retrieve the _DTA.txt file
            // Note that if the file was found in MyEMSL then RetrieveDtaFiles will auto-call ProcessMyEMSLDownloadQueue to download the file
            if (!FileSearch.RetrieveDtaFiles())
            {
                //Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Make sure the _DTA.txt file has parent ion lines with text: scan=x and cs=y
            // X!Tandem uses this information to determine the scan number
            string strCDTAPath = Path.Combine(m_WorkingDir, DatasetName + "_dta.txt");
            const bool blnReplaceSourceFile = true;
            const bool blnDeleteSourceFileIfUpdated = true;

            if (!ValidateCDTAFileScanAndCSTags(strCDTAPath, blnReplaceSourceFile, blnDeleteSourceFileIfUpdated, ""))
            {
                m_message = "Error validating the _DTA.txt file";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            //Add all the extensions of the files to delete after run
            m_jobParams.AddResultFileExtensionToSkip("_dta.zip"); //Zipped DTA
            m_jobParams.AddResultFileExtensionToSkip("_dta.txt"); //Unzipped, concatenated DTA
            m_jobParams.AddResultFileExtensionToSkip(".dta");  //DTA files

            // If the _dta.txt file is over 2 GB in size, then condense it

            if (!ValidateDTATextFileSize(m_WorkingDir, DatasetName + "_dta.txt"))
            {
                //Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var success = CopyFileToWorkDir("taxonomy_base.xml", m_jobParams.GetParam("ParmFileStoragePath"), m_WorkingDir);
            if (!success)
            {
                const string Msg = "clsAnalysisResourcesXT.GetResources(), failed retrieving taxonomy_base.xml file.";
                LogError(Msg);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            success = CopyFileToWorkDir("input_base.txt", m_jobParams.GetParam("ParmFileStoragePath"), m_WorkingDir);
            if (!success)
            {
                const string Msg = "clsAnalysisResourcesXT.GetResources(), failed retrieving input_base.xml file.";
                LogError(Msg);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            success = CopyFileToWorkDir("default_input.xml", m_jobParams.GetParam("ParmFileStoragePath"), m_WorkingDir);
            if (!success)
            {
                const string Msg = "clsAnalysisResourcesXT.GetResources(), failed retrieving default_input.xml file.";
                LogError(Msg);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!base.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // set up taxonomy file to reference the organism DB file (fasta)
            success = MakeTaxonomyFile();
            if (!success)
            {
                const string Msg = "clsAnalysisResourcesXT.GetResources(), failed making taxonomy file.";
                LogError(Msg);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // set up run parameter file to reference spectra file, taxonomy file, and analysis parameter file
            success = MakeInputFile();
            if (!success)
            {
                const string Msg = "clsAnalysisResourcesXT.GetResources(), failed making input file.";
                LogError(Msg);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        protected bool MakeTaxonomyFile()
        {
            // set up taxonomy file to reference the organsim DB file (fasta)

            string WorkingDir = m_mgrParams.GetParam("WorkDir");
            string OrgDBName = m_jobParams.GetParam("PeptideSearch", "generatedFastaName");
            string OrganismName = m_jobParams.GetParam("OrganismName");
            string LocalOrgDBFolder = m_mgrParams.GetParam("orgdbdir");
            string OrgFilePath = Path.Combine(LocalOrgDBFolder, OrgDBName);

            //edit base taxonomy file into actual
            try
            {
                // Create an instance of StreamWriter to write to a file.
                var inputFile = new StreamWriter(Path.Combine(WorkingDir, "taxonomy.xml"));

                // Create an instance of StreamReader to read from a file.
                var inputBase = new StreamReader(Path.Combine(WorkingDir, "taxonomy_base.xml"));
                string inpLine = null;
                // Read and display the lines from the file until the end
                // of the file is reached.
                do
                {
                    inpLine = inputBase.ReadLine();
                    if ((inpLine != null))
                    {
                        inpLine = inpLine.Replace("ORGANISM_NAME", OrganismName);
                        inpLine = inpLine.Replace("FASTA_FILE_PATH", OrgFilePath);
                        inputFile.WriteLine(inpLine);
                    }
                } while (!(inpLine == null));
                inputBase.Close();
                inputFile.Close();
            }
            catch (Exception E)
            {
                // Let the user know what went wrong.
                LogError(
                    "clsAnalysisResourcesXT.MakeTaxonomyFile, The file could not be read" + E.Message);
            }

            //get rid of base file
            File.Delete(Path.Combine(WorkingDir, "taxonomy_base.xml"));

            return true;
        }

        protected bool MakeInputFile()
        {
            var result = true;

            // set up input to reference spectra file, taxonomy file, and parameter file

            string WorkingDir = m_mgrParams.GetParam("WorkDir");
            string OrganismName = m_jobParams.GetParam("OrganismName");
            string ParamFilePath = Path.Combine(WorkingDir, m_jobParams.GetParam("parmFileName"));
            string SpectrumFilePath = Path.Combine(WorkingDir, DatasetName + "_dta.txt");
            string TaxonomyFilePath = Path.Combine(WorkingDir, "taxonomy.xml");
            string OutputFilePath = Path.Combine(WorkingDir, DatasetName + "_xt.xml");

            //make input file
            //start by adding the contents of the parameter file.
            //replace substitution tags in input_base.txt with proper file path references
            //and add to input file (in proper XML format)
            try
            {
                // Create an instance of StreamWriter to write to a file.
                var inputFile = new StreamWriter(Path.Combine(WorkingDir, "input.xml"));
                // Create an instance of StreamReader to read from a file.
                var inputBase = new StreamReader(Path.Combine(WorkingDir, "input_base.txt"));
                var paramFile = new StreamReader(ParamFilePath);
                string paramLine = null;
                string inpLine = null;

                // Read and display the lines from the file until the end
                // of the file is reached.
                do
                {
                    paramLine = paramFile.ReadLine();
                    if (paramLine == null)
                    {
                        break;
                    }
                    inputFile.WriteLine(paramLine);
                    if (paramLine.IndexOf("<bioml>", StringComparison.Ordinal) != -1)
                    {
                        do
                        {
                            inpLine = inputBase.ReadLine();
                            if ((inpLine != null))
                            {
                                inpLine = inpLine.Replace("ORGANISM_NAME", OrganismName);
                                inpLine = inpLine.Replace("TAXONOMY_FILE_PATH", TaxonomyFilePath);
                                inpLine = inpLine.Replace("SPECTRUM_FILE_PATH", SpectrumFilePath);
                                inpLine = inpLine.Replace("OUTPUT_FILE_PATH", OutputFilePath);
                                inputFile.WriteLine(inpLine);
                            }
                        } while (!(inpLine == null));
                    }
                } while (!(paramLine == null));
                inputBase.Close();
                inputFile.Close();
                paramFile.Close();
            }
            catch (Exception E)
            {
                // Let the user know what went wrong.
                LogError(
                    "clxAnalysisResourcesXT.MakeInputFile, The file could not be read" + E.Message);
                result = false;
            }

            //get rid of base file
            File.Delete(Path.Combine(WorkingDir, "input_base.txt"));

            return result;
        }

        static internal string ConstructModificationDefinitionsFilename(string ParameterFileName)
        {
            return Path.GetFileNameWithoutExtension(ParameterFileName) + MOD_DEFS_FILE_SUFFIX;
        }

        protected bool ValidateDTATextFileSize(string strWorkDir, string strInputFileName)
        {
            const int FILE_SIZE_THRESHOLD = int.MaxValue;

            string strInputFilePath = null;
            string strFilePathOld = null;

            string strMessage = null;

            bool blnSuccess = false;

            try
            {
                strInputFilePath = Path.Combine(strWorkDir, strInputFileName);
                var ioFileInfo = new FileInfo(strInputFilePath);

                if (!ioFileInfo.Exists)
                {
                    m_message = "_DTA.txt file not found: " + strInputFilePath;
                    LogError(m_message);
                    return false;
                }

                if (ioFileInfo.Length >= FILE_SIZE_THRESHOLD)
                {
                    // Need to condense the file

                    strMessage = ioFileInfo.Name + " is " + (ioFileInfo.Length / 1024.0 / 1024 / 1024).ToString("0.00") +
                                 " GB in size; will now condense it by combining data points with consecutive zero-intensity values";
                    LogMessage(strMessage);

                    mCDTACondenser = new CondenseCDTAFile.clsCDTAFileCondenser();
                    mCDTACondenser.ProgressChanged += mCDTACondenser_ProgressChanged;

                    blnSuccess = mCDTACondenser.ProcessFile(ioFileInfo.FullName, ioFileInfo.DirectoryName);

                    if (!blnSuccess)
                    {
                        m_message = "Error condensing _DTA.txt file: " + mCDTACondenser.GetErrorMessage();
                        LogError(m_message);
                        return false;
                    }
                    else
                    {
                        // Wait 500 msec, then check the size of the new _dta.txt file
                        Thread.Sleep(500);

                        ioFileInfo.Refresh();

                        if (m_DebugLevel >= 1)
                        {
                            strMessage = "Condensing complete; size of the new _dta.txt file is " +
                                         (ioFileInfo.Length / 1024.0 / 1024 / 1024).ToString("0.00") + " GB";
                            LogMessage(strMessage);
                        }

                        try
                        {
                            strFilePathOld = Path.Combine(strWorkDir, Path.GetFileNameWithoutExtension(ioFileInfo.FullName) + "_Old.txt");

                            if (m_DebugLevel >= 2)
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
                            LogError(
                                "Exception deleting _dta_old.txt file: " + ex.Message);
                        }
                    }
                }

                blnSuccess = true;
            }
            catch (Exception ex)
            {
                m_message = "Exception in ValidateDTATextFileSize";
                LogError(m_message + ": " + ex.Message);
                return false;
            }

            return blnSuccess;
        }

        private DateTime dtLastUpdateTime;

        private void mCDTACondenser_ProgressChanged(string taskDescription, float percentComplete)
        {
            if (m_DebugLevel >= 1)
            {
                if (m_DebugLevel == 1 && DateTime.UtcNow.Subtract(dtLastUpdateTime).TotalSeconds >= 60 ||
                    m_DebugLevel > 1 && DateTime.UtcNow.Subtract(dtLastUpdateTime).TotalSeconds >= 20)
                {
                    dtLastUpdateTime = DateTime.UtcNow;

                    LogDebug(
                        " ... " + percentComplete.ToString("0.00") + "% complete");
                }
            }
        }
    }
}