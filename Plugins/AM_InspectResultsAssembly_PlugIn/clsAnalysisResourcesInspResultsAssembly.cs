using System;
using System.IO;
using AnalysisManagerBase;

namespace AnalysisManagerInspResultsAssemblyPlugIn
{
    public class clsAnalysisResourcesInspResultsAssembly : clsAnalysisResources
    {
        #region "Methods"

        public override void Setup(IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, clsMyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(mgrParams, jobParams, statusTools, myEMSLUtilities);
            SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, true);
        }

        /// <summary>
        /// Retrieves files necessary for performance of Sequest analysis
        /// </summary>
        /// <returns>CloseOutType indicating success or failure</returns>
        /// <remarks></remarks>
        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            string numClonedSteps = null;

            string transferFolderName = Path.Combine(m_jobParams.GetParam("transferFolderPath"), DatasetName);
            string zippedResultName = DatasetName + "_inspect.zip";
            const string searchLogResultName = "InspectSearchLog.txt";

            transferFolderName = Path.Combine(transferFolderName, m_jobParams.GetParam("OutputFolderName"));

            //Retrieve Fasta file (used by the PeptideToProteinMapper)
            if (!RetrieveOrgDB(m_mgrParams.GetParam("orgdbdir")))
                return CloseOutType.CLOSEOUT_FAILED;

            //Retrieve param file
            if (!RetrieveGeneratedParamFile(m_jobParams.GetParam("ParmFileName")))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Retrieve the Inspect Input Params file
            if (!FileSearch.RetrieveFile(clsAnalysisToolRunnerInspResultsAssembly.INSPECT_INPUT_PARAMS_FILENAME, transferFolderName))
            {
                //Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FAILED;
            }

            numClonedSteps = m_jobParams.GetParam("NumberOfClonedSteps");
            if (string.IsNullOrEmpty(numClonedSteps))
            {
                // This is not a parallelized job
                // Retrieve the zipped Inspect result file
                if (!FileSearch.RetrieveFile(zippedResultName, transferFolderName))
                {
                    if (m_DebugLevel >= 3)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                            "RetrieveFile returned False for " + zippedResultName + " using folder " + transferFolderName);
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Unzip Inspect result file
                if (m_DebugLevel >= 2)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Unzipping Inspect result file");
                }
                if (UnzipFileStart(Path.Combine(m_WorkingDir, zippedResultName), m_WorkingDir, "clsAnalysisResourcesInspResultsAssembly.GetResources", false))
                {
                    if (m_DebugLevel >= 1)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Inspect result file unzipped");
                    }
                }

                // Retrieve the Inspect search log file
                if (!FileSearch.RetrieveFile(searchLogResultName, transferFolderName))
                {
                    if (m_DebugLevel >= 3)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                            "RetrieveFile returned False for " + searchLogResultName + " using folder " + transferFolderName);
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                m_jobParams.AddResultFileExtensionToSkip(searchLogResultName);
            }
            else
            {
                // This is a parallelized job
                // Retrieve multi inspect result files
                if (!RetrieveMultiInspectResultFiles())
                {
                    //Errors were reported in function call, so just return
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            //All finished
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Retrieves inspect and inspect log and error files
        /// </summary>
        /// <returns>TRUE for success, FALSE for error</returns>
        /// <remarks></remarks>
        protected bool RetrieveMultiInspectResultFiles()
        {
            string InspectResultsFile = null;
            string strFileName = string.Empty;

            int numOfResultFiles = 0;
            int fileNum = 0;
            string DatasetName = m_jobParams.GetParam("datasetNum");
            string transferFolderName = Path.Combine(m_jobParams.GetParam("transferFolderPath"), DatasetName);
            string dtaFilename = null;

            var intFileCopyCount = 0;
            int intLogFileIndex = 0;

            transferFolderName = Path.Combine(transferFolderName, m_jobParams.GetParam("OutputFolderName"));

            try
            {
                numOfResultFiles = m_jobParams.GetJobParameter("NumberOfClonedSteps", 0);
            }
            catch (Exception)
            {
                numOfResultFiles = 0;
            }

            if (numOfResultFiles < 1)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    "Job parameter 'NumberOfClonedSteps' is empty or 0; unable to continue");
                return false;
            }

            for (fileNum = 1; fileNum <= numOfResultFiles; fileNum++)
            {
                //Copy each Inspect result file from the transfer directory
                InspectResultsFile = DatasetName + "_" + fileNum + "_inspect.txt";
                dtaFilename = DatasetName + "_" + fileNum + "_dta.txt";

                if (File.Exists(Path.Combine(transferFolderName, InspectResultsFile)))
                {
                    if (!CopyFileToWorkDir(InspectResultsFile, transferFolderName, m_WorkingDir))
                    {
                        // Error copying file (error will have already been logged)
                        if (m_DebugLevel >= 3)
                        {
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                "CopyFileToWorkDir returned False for " + InspectResultsFile + " using folder " + transferFolderName);
                        }
                        return false;
                    }
                    intFileCopyCount += 1;

                    // Update the list of files to delete from the server
                    m_jobParams.AddServerFileToDelete(Path.Combine(transferFolderName, InspectResultsFile));
                    m_jobParams.AddServerFileToDelete(Path.Combine(transferFolderName, dtaFilename));

                    // Update the list of local files to delete
                    m_jobParams.AddResultFileToSkip(InspectResultsFile);
                }

                // Copy the various log files
                for (intLogFileIndex = 1; intLogFileIndex <= 3; intLogFileIndex++)
                {
                    switch (intLogFileIndex)
                    {
                        case 1:
                            //Copy the Inspect error file from the transfer directory
                            strFileName = DatasetName + "_" + fileNum + "_error.txt";
                            break;
                        case 2:
                            //Copy each Inspect search log file from the transfer directory
                            strFileName = "InspectSearchLog_" + fileNum + ".txt";
                            break;
                        case 3:
                            //Copy each Inspect console output file from the transfer directory
                            strFileName = "InspectConsoleOutput_" + fileNum + ".txt";
                            break;
                    }

                    if (File.Exists(Path.Combine(transferFolderName, strFileName)))
                    {
                        if (!CopyFileToWorkDir(strFileName, transferFolderName, m_WorkingDir))
                        {
                            // Error copying file (error will have already been logged)
                            if (m_DebugLevel >= 3)
                            {
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                    "CopyFileToWorkDir returned False for " + strFileName + " using folder " + transferFolderName);
                            }
                            return false;
                        }
                        intFileCopyCount += 1;

                        // Update the list of files to delete from the server
                        m_jobParams.AddServerFileToDelete(Path.Combine(transferFolderName, strFileName));

                        // Update the list of local files to delete
                        m_jobParams.AddResultFileToSkip(strFileName);
                    }
                }
            }

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO,
                "Multi Inspect Result Files copied to local working directory; copied " + intFileCopyCount + " files");

            return true;
        }

        #endregion
    }
}
