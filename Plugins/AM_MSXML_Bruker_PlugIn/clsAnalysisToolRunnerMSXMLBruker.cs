//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 03/30/2011
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using AnalysisManagerBase;

namespace AnalysisManagerMsXmlBrukerPlugIn
{

    /// <summary>
    /// Class for running MSXML Bruker
    /// </summary>
    public class clsAnalysisToolRunnerMSXMLBruker : clsAnalysisToolRunnerBase
    {
        #region "Module Variables"

        protected const float PROGRESS_PCT_MSXML_GEN_RUNNING = 5;

        protected const string COMPASS_XPORT = "CompassXport.exe";

        protected DirectoryInfo mMSXmlCacheFolder;

        protected clsCompassXportRunner mCompassXportRunner;

        #endregion

        #region "Methods"

        protected const int MAX_CSV_FILES = 50;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <remarks>Presently not used</remarks>
        // ReSharper disable once EmptyConstructor
        public clsAnalysisToolRunnerMSXMLBruker()
        {
            // Empty constructor
        }

        /// <summary>
        /// Runs ReadW tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
            // Do the base class stuff
            if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Store the CompassXport version info in the database
            if (!StoreToolVersionInfo())
            {
                LogError("Aborting since StoreToolVersionInfo returned false");
                if (string.IsNullOrEmpty(m_message))
                {
                    m_message = "Error determining CompassXport version";
                }
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var msXMLCacheFolderPath = m_mgrParams.GetParam("MSXMLCacheFolderPath", string.Empty);
            mMSXmlCacheFolder = new DirectoryInfo(msXMLCacheFolderPath);

            if (!mMSXmlCacheFolder.Exists)
            {
                LogError("MSXmlCache folder not found: " + msXMLCacheFolderPath);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var processingErrorMessage = string.Empty;
            FileInfo fiResultsFile = null;

            var eResult = CreateMSXmlFile(out fiResultsFile);

            if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
            {
                // Something went wrong
                // In order to help diagnose things, we will move whatever files were created into the eResult folder,
                //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                if (string.IsNullOrEmpty(m_message))
                {
                    m_message = "Error running CompassXport";
                }
                processingErrorMessage = string.Copy(m_message);

                if (eResult == CloseOutType.CLOSEOUT_NO_DATA)
                {
                }
                else
                {
                }
            }
            else
            {
                // Gzip the .mzML or .mzXML file then copy to the server cache
                PostProcessMsXmlFile(fiResultsFile);
            }

            // Stop the job timer
            m_StopTime = DateTime.UtcNow;

            // Delete the raw data files
            if (m_DebugLevel > 3)
            {
                LogDebug("clsAnalysisToolRunnerMSXMLBruker.RunTool(), Deleting raw data file");
            }

            var deleteSuccess = DeleteRawDataFiles();
            if (!deleteSuccess)
            {
                LogError("clsAnalysisToolRunnerMSXMLBruker.RunTool(), Problem deleting raw data files: " + m_message);

                if (!string.IsNullOrEmpty(processingErrorMessage))
                {
                    m_message = processingErrorMessage;
                }
                else
                {
                    // Don't treat this as a critical error; leave eReturnCode unchanged
                    m_message = "Error deleting raw data files";
                }
            }

            // Update the job summary file
            if (m_DebugLevel > 3)
            {
                LogDebug("clsAnalysisToolRunnerMSXMLBruker.RunTool(), Updating summary file");
            }

            UpdateSummaryFile();

            var success = CopyResultsToTransferDirectory();

            return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;

        }

        /// <summary>
        /// Generate the mzXML or mzML file
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        private CloseOutType CreateMSXmlFile(out FileInfo fiResultsFile)
        {
            if (m_DebugLevel > 4)
            {
                LogDebug("clsAnalysisToolRunnerMSXMLGen.CreateMSXmlFile(): Enter");
            }

            var msXmlGenerator = m_jobParams.GetParam("MSXMLGenerator");
            // Typically CompassXport.exe

            var msXmlFormat = m_jobParams.GetParam("MSXMLOutputType");
            // Typically mzXML or mzML
            var CentroidMSXML = Convert.ToBoolean(m_jobParams.GetParam("CentroidMSXML"));

            string CompassXportProgramPath = null;

            var blnSuccess = false;

            // Initialize the Results File output parameter to a dummy name for now
            fiResultsFile = new FileInfo(Path.Combine(m_WorkDir, "NonExistent_Placeholder_File.tmp"));

            if (string.Equals(msXmlGenerator, COMPASS_XPORT, StringComparison.OrdinalIgnoreCase))
            {
                CompassXportProgramPath = m_mgrParams.GetParam("CompassXportLoc");

                if (string.IsNullOrEmpty(CompassXportProgramPath))
                {
                    m_message = "Manager parameter CompassXportLoc is not defined in the Manager Control DB";
                    LogError(m_message);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (!File.Exists(CompassXportProgramPath))
                {
                    m_message = "CompassXport program not found";
                    LogError(m_message + " at " + CompassXportProgramPath);
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            else
            {
                m_message = "Invalid value for MSXMLGenerator: " + msXmlGenerator;
                LogError(m_message);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var eOutputType = clsCompassXportRunner.GetMsXmlOutputTypeByName(msXmlFormat);
            if (eOutputType == clsCompassXportRunner.MSXMLOutputTypeConstants.Invalid)
            {
                LogWarning("msXmlFormat string is not recognized (" + msXmlFormat + "); it is typically mzXML, mzML, or CSV; will default to mzXML");
                eOutputType = clsCompassXportRunner.MSXMLOutputTypeConstants.mzXML;
            }

            fiResultsFile = new FileInfo(Path.Combine(m_WorkDir, m_Dataset + "." + clsCompassXportRunner.GetMsXmlOutputTypeByID(eOutputType)));

            // Instantiate the processing class
            mCompassXportRunner = new clsCompassXportRunner(m_WorkDir, CompassXportProgramPath, m_Dataset, eOutputType, CentroidMSXML);
            RegisterEvents(mCompassXportRunner);

            mCompassXportRunner.LoopWaiting += CompassXportRunner_LoopWaiting;
            mCompassXportRunner.ProgRunnerStarting += mCompassXportRunner_ProgRunnerStarting;

            // Create the file
            blnSuccess = mCompassXportRunner.CreateMSXMLFile();

            if (!blnSuccess)
            {
                LogWarning(mCompassXportRunner.ErrorMessage);
                return CloseOutType.CLOSEOUT_FAILED;
            }
            else if (mCompassXportRunner.ErrorMessage.Length > 0)
            {
                LogWarning(mCompassXportRunner.ErrorMessage);
            }

            if (eOutputType != clsCompassXportRunner.MSXMLOutputTypeConstants.CSV)
            {
                if (fiResultsFile.Exists)
                {
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                m_message = "MSXml results file not found: " + fiResultsFile.FullName;
                LogError(m_message);
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // CompassXport created one CSV file for each spectrum in the dataset
            // Confirm that fewer than 100 CSV files were created

            var diWorkDir = new DirectoryInfo(m_WorkDir);
            var fiFiles = diWorkDir.GetFiles("*.csv").ToList();

            if (fiFiles.Count < MAX_CSV_FILES)
            {
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            m_message = "CompassXport created " + fiFiles.Count +
                        " CSV files. The CSV conversion mode is only appropriate for datasets with fewer than " + MAX_CSV_FILES +
                        " spectra; create a mzXML file instead (e.g., settings file mzXML_Bruker.xml)";
            LogError(m_message);

            foreach (var fiFile in fiFiles)
            {
                try
                {
                    fiFile.Delete();
                }
                catch (Exception)
                {
                    // Ignore errors here
                }
            }

            return CloseOutType.CLOSEOUT_FAILED;
        }

        private CloseOutType PostProcessMsXmlFile(FileInfo fiResultsFile)
        {
            // Compress the file using GZip
            LogMessage("GZipping " + fiResultsFile.Name);
            fiResultsFile = GZipFile(fiResultsFile);

            if (fiResultsFile == null)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Copy the .mzXML.gz or .mzML.gz file to the MSXML cache
            var remoteCachefilePath = CopyFileToServerCache(mMSXmlCacheFolder.FullName, fiResultsFile.FullName, purgeOldFilesIfNeeded: true);

            if (string.IsNullOrEmpty(remoteCachefilePath))
            {
                if (string.IsNullOrEmpty(m_message))
                {
                    LogError("CopyFileToServerCache returned false for " + fiResultsFile.Name);
                }
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Create the _CacheInfo.txt file
            var cacheInfoFilePath = fiResultsFile.FullName + "_CacheInfo.txt";
            using (var swOutFile = new StreamWriter(new FileStream(cacheInfoFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
            {
                swOutFile.WriteLine(remoteCachefilePath);
            }

            m_jobParams.AddResultFileToSkip(fiResultsFile.Name);

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo()
        {
            var strToolVersionInfo = string.Empty;

            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // Store paths to key files in ioToolFiles
            var ioToolFiles = new List<FileInfo>();

            var msXmlGenerator = m_jobParams.GetParam("MSXMLGenerator");

            // Typically CompassXport.exe
            if (string.Equals(msXmlGenerator, COMPASS_XPORT, StringComparison.OrdinalIgnoreCase))
            {
                var compassXportPath = m_mgrParams.GetParam("CompassXportLoc");
                if (string.IsNullOrEmpty(compassXportPath))
                {
                    m_message = "Path defined by manager param CompassXportLoc is empty";
                    LogError(m_message);
                    return false;
                }

                try
                {
                    ioToolFiles.Add(new FileInfo(compassXportPath));
                }
                catch (Exception ex)
                {
                    m_message = "Path defined by manager param CompassXportLoc is invalid: " + compassXportPath;
                    LogError(m_message + "; " + ex.Message);
                    return false;
                }
            }
            else
            {
                if (string.IsNullOrEmpty(msXmlGenerator))
                {
                    m_message = "Job Parameter MSXMLGenerator is not defined";
                }
                else
                {
                    m_message = "Invalid value for MSXMLGenerator, should be " + COMPASS_XPORT + ", not " + msXmlGenerator;
                }

                LogError(m_message);
                return false;
            }

            try
            {
                return SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        #endregion

        #region "Event Handlers"

        /// <summary>
        /// Event handler for CompassXportRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void CompassXportRunner_LoopWaiting()
        {
            UpdateStatusFile(PROGRESS_PCT_MSXML_GEN_RUNNING);

            LogProgress("CompassXport");
        }

        /// <summary>
        /// Event handler for mCompassXportRunner.ProgRunnerStarting event
        /// </summary>
        /// <param name="CommandLine">The command being executed (program path plus command line arguments)</param>
        /// <remarks></remarks>
        private void mCompassXportRunner_ProgRunnerStarting(string CommandLine)
        {
            LogDebug(CommandLine);
        }

        #endregion
    }
}
