using System;
using System.IO;
using System.Threading;
using AnalysisManagerBase;

namespace AnalysisManagerICR2LSPlugIn
{
    /// <summary>
    /// Performs PEK analysis using ICR-2LS on LTQ-FT MS data
    /// </summary>
    /// <remarks></remarks>
    public class clsAnalysisToolRunnerLTQ_FTPek : clsAnalysisToolRunnerICRBase
    {
        public override CloseOutType RunTool()
        {
            string DSNamePath = null;

            bool UseAllScans = false;

            string OutFileNamePath = null;
            string ParamFilePath = null;
            bool blnSuccess = false;

            //Start with base class function to get settings information
            var ResCode = base.RunTool();
            if (ResCode != CloseOutType.CLOSEOUT_SUCCESS)
                return ResCode;

            // Store the ICR2LS version info in the database
            if (!StoreToolVersionInfo())
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    "Aborting since StoreToolVersionInfo returned false");
                m_message = "Error determining ICR2LS version";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            //Verify a param file has been specified
            ParamFilePath = Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"));
            if (!File.Exists(ParamFilePath))
            {
                //Param file wasn't specified, but is required for ICR-2LS analysis
                m_message = "ICR-2LS Param file not found";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + ": " + ParamFilePath);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            //Add handling of settings file info here if it becomes necessary in the future

            //Get scan settings from settings file
            var MinScan = m_jobParams.GetJobParameter("scanstart", 0);
            var MaxScan = m_jobParams.GetJobParameter("ScanStop", 0);

            // Determine whether or not we should be processing MS2 spectra
            var SkipMS2 = !m_jobParams.GetJobParameter("ProcessMS2", false);

            if ((MinScan == 0 && MaxScan == 0) || MinScan > MaxScan || MaxScan > 500000)
            {
                UseAllScans = true;
            }
            else
            {
                UseAllScans = false;
            }

            //Assemble the data file name and path
            DSNamePath = Path.Combine(m_WorkDir, m_Dataset + ".raw");
            if (!File.Exists(DSNamePath))
            {
                m_message = "Raw file not found: " + DSNamePath;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            //Assemble the output file name and path
            OutFileNamePath = Path.Combine(m_WorkDir, m_Dataset + ".pek");

            blnSuccess = base.StartICR2LS(DSNamePath, ParamFilePath, OutFileNamePath, ICR2LSProcessingModeConstants.LTQFTPEK, UseAllScans, SkipMS2,
                MinScan, MaxScan);

            if (blnSuccess)
            {
                if (!VerifyPEKFileExists(m_WorkDir, m_Dataset))
                {
                    m_message = "ICR-2LS successfully finished but did not make a .Pek file; if all spectra are MS/MS use settings file LTQ_FTPEK_ProcessMS2.txt";
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            else
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error running ICR-2LS on file " + DSNamePath);

                // If a .PEK file exists, then call PerfPostAnalysisTasks() to move the .Pek file into the results folder, which we'll then archive in the Failed Results folder
                if (VerifyPEKFileExists(m_WorkDir, m_Dataset))
                {
                    m_message = "ICR-2LS returned false (see .PEK file in Failed results folder)";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                        ".Pek file was found, so will save results to the failed results archive folder");

                    PerfPostAnalysisTasks(false);

                    // Try to save whatever files were moved into the results folder
                    var objAnalysisResults = new clsAnalysisResults(m_mgrParams, m_jobParams);
                    objAnalysisResults.CopyFailedResultsToArchiveFolder(Path.Combine(m_WorkDir, m_ResFolderName));
                }
                else
                {
                    m_message = "Error running ICR-2LS (.Pek file not found in " + m_WorkDir + ")";
                }

                return CloseOutType.CLOSEOUT_FAILED;
            }

            //Run the cleanup routine from the base class
            if (PerfPostAnalysisTasks(true) != CloseOutType.CLOSEOUT_SUCCESS)
            {
                if (string.IsNullOrEmpty(m_message))
                {
                    m_message = "Error performing post analysis tasks";
                }
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        protected override CloseOutType DeleteDataFile()
        {
            //Deletes the .raw file from the working directory
            string[] FoundFiles = null;

            //Delete the .raw file
            try
            {
                Thread.Sleep(5000);             //Allow extra time for ICR2LS to release file locks
                FoundFiles = Directory.GetFiles(m_WorkDir, "*.raw");
                foreach (string MyFile in FoundFiles)
                {
                    // Add the file to .FilesToDelete just in case the deletion fails
                    m_jobParams.AddResultFileToSkip(MyFile);
                    DeleteFileWithRetries(MyFile);
                }
                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR,
                    "Error deleting .raw file, job " + m_JobNum + ex.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }
    }
}
