//*********************************************************************************************************
// Written by Matt Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//
//*********************************************************************************************************

using System;
using System.IO;
using System.Threading;
using AnalysisManagerBase;

namespace AnalysisManagerMasicPlugin
{
    /// <summary>
    /// Derived class for performing MASIC analysis on Finnigan datasets
    /// </summary>
    /// <remarks></remarks>
    public class clsAnalysisToolRunnerMASICFinnigan : clsAnalysisToolRunnerMASICBase
    {
        #region "Module Variables"

        protected AnalysisManagerMsXmlGenPlugIn.clsMSXMLCreator mMSXmlCreator;

        #endregion

        public clsAnalysisToolRunnerMASICFinnigan()
        {
        }

        [Obsolete("No longer necessary")]
        public static bool NeedToConvertRawToMzXML(FileInfo fiInputFile)
        {
            const long TWO_GB = 1024L * 1024 * 1024 * 2;

            if (fiInputFile.Length > TWO_GB)
                return true;

            return false;
        }

        protected override CloseOutType RunMASIC()
        {
            string strParameterFilePath = null;

            var strParameterFileName = m_jobParams.GetParam("parmFileName");

            if ((strParameterFileName != null) && strParameterFileName.Trim().ToLower() != "na")
            {
                strParameterFilePath = Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"));
            }
            else
            {
                strParameterFilePath = string.Empty;
            }

            // Determine the path to the .Raw file
            var strRawFileName = m_Dataset + ".raw";
            var strInputFilePath = clsAnalysisResources.ResolveStoragePath(m_WorkDir, strRawFileName);

            if (string.IsNullOrWhiteSpace(strInputFilePath))
            {
                // Unable to resolve the file path
                m_ErrorMessage = "Could not find " + strRawFileName + " or " + strRawFileName + clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX +
                                 " in the working folder; unable to run MASIC";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_ErrorMessage);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Examine the size of the .Raw file
            FileInfo fiInputFile = new FileInfo(strInputFilePath);
            if (!fiInputFile.Exists)
            {
                // Unable to resolve the file path
                m_ErrorMessage = "Could not find " + fiInputFile.FullName + "; unable to run MASIC";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_ErrorMessage);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!string.IsNullOrEmpty(strParameterFilePath))
            {
                // Make sure the parameter file has IncludeHeaders defined and set to True
                ValidateParameterFile(strParameterFilePath);
            }

            // Deprecated in December 2016
            //var strScanStatsFilePath = Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.SCAN_STATS_FILE_SUFFIX);
            //var strScanStatsExFilePath = Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.SCAN_STATS_EX_FILE_SUFFIX);
            //
            //FileInfo fiScanStatsOverrideFile;
            //FileInfo fiScanStatsExOverrideFile;
            //
            //var blnConvertRawToMzXML = NeedToConvertRawToMzXML(fiInputFile);
            //var eCloseout = CloseOutType.CLOSEOUT_SUCCESS;
            //
            //if (blnConvertRawToMzXML)
            //{
            //    eCloseout = StartConvertRawToMzXML(fiInputFile, strScanStatsFilePath, strScanStatsExFilePath, out fiScanStatsOverrideFile, out fiScanStatsExOverrideFile, out strInputFilePath)
            //    if (eCloseout == CloseOutType.CLOSEOUT_SUCCESS)
            //    {
            //        return eCloseout;
            //    }
            //}

            var eCloseout = base.StartMASICAndWait(strInputFilePath, m_WorkDir, strParameterFilePath);

            // Deprecated in December 2016
            //if (eCloseout == CloseOutType.CLOSEOUT_SUCCESS && blnConvertRawToMzXML)
            //{
            //    eCloseout = ReplaceScanStatsFiles(strScanStatsFilePath, strScanStatsExFilePath, fiScanStatsOverrideFile, fiScanStatsExOverrideFile);
            //}

            return eCloseout;
        }

        [Obsolete("No longer used")]
        private CloseOutType ReplaceScanStatsFiles(string strScanStatsFilePath, string strScanStatsExFilePath, FileInfo fiScanStatsOverrideFile, FileInfo fiScanStatsExOverrideFile)
        {
            try
            {
                // Replace the _ScanStats.txt file created by MASIC with the ScanStats file created in clsAnalysisResourcesMASIC
                if (File.Exists(strScanStatsFilePath))
                {
                    Thread.Sleep(250);
                    PRISM.Processes.clsProgRunner.GarbageCollectNow();

                    File.Delete(strScanStatsFilePath);
                    Thread.Sleep(250);
                }

                // Rename the override file to have the correct name
                fiScanStatsOverrideFile.MoveTo(strScanStatsFilePath);

                if (fiScanStatsExOverrideFile.Exists)
                {
                    // Replace the _ScanStatsEx.txt file created by MASIC with the ScanStatsEx file created in clsAnalysisResourcesMASIC
                    if (File.Exists(strScanStatsExFilePath))
                    {
                        File.Delete(strScanStatsExFilePath);
                        Thread.Sleep(250);
                    }

                    // Rename the override file to have the correct name
                    fiScanStatsExOverrideFile.MoveTo(strScanStatsExFilePath);
                }
            }
            catch (Exception ex)
            {
                m_message = "Error replacing the ScanStats files created from the mzXML file with the ScanStats files created from the .Raw file";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    m_message + " (ReplaceScanStatsFiles): " + ex.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Converts the .Raw file specified by fiThermoRawFile to a .mzXML file
        /// </summary>
        /// <param name="fiThermoRawFile"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        protected string ConvertRawToMzXML(FileInfo fiThermoRawFile)
        {
            string strMSXmlGeneratorAppPath = null;
            bool blnSuccess = false;

            strMSXmlGeneratorAppPath = base.GetMSXmlGeneratorAppPath();

            mMSXmlCreator = new AnalysisManagerMsXmlGenPlugIn.clsMSXMLCreator(strMSXmlGeneratorAppPath, m_WorkDir, m_Dataset, m_DebugLevel, m_jobParams);
            RegisterEvents(mMSXmlCreator);
            mMSXmlCreator.LoopWaiting += mMSXmlCreator_LoopWaiting;

            blnSuccess = mMSXmlCreator.CreateMZXMLFile();

            if (!blnSuccess && string.IsNullOrEmpty(m_message))
            {
                m_message = mMSXmlCreator.ErrorMessage;
                if (string.IsNullOrEmpty(m_message))
                {
                    m_message = "Unknown error creating the mzXML file for dataset " + m_Dataset;
                }
                else if (!m_message.Contains(m_Dataset))
                {
                    m_message += "; dataset " + m_Dataset;
                }
            }

            if (!blnSuccess)
                return string.Empty;

            string strMzXMLFilePath = Path.ChangeExtension(fiThermoRawFile.FullName, "mzXML");
            if (!File.Exists(strMzXMLFilePath))
            {
                m_message = "MSXmlCreator did not create the .mzXML file";
                return string.Empty;
            }

            return strMzXMLFilePath;
        }

        protected override CloseOutType DeleteDataFile()
        {
            //Deletes the .raw file from the working directory
            string[] FoundFiles = null;

            //Delete the .raw file
            try
            {
                FoundFiles = Directory.GetFiles(m_WorkDir, "*.raw");
                foreach (string MyFile in FoundFiles)
                {
                    DeleteFileWithRetries(MyFile);
                }
                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error finding .raw files to delete, job " + m_JobNum);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        [Obsolete("No longer used")]
        private CloseOutType StartConvertRawToMzXML(FileInfo fiInputFile, string strScanStatsFilePath, string strScanStatsExFilePath,
            out FileInfo fiScanStatsOverrideFile, out FileInfo fiScanStatsExOverrideFile, out string strInputFilePath)
        {
            // .Raw file is over 2 GB in size
            // Will convert it to mzXML and centroid (so that MASIC will use less memory)

            strInputFilePath = string.Empty;

            try
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO,
                    ".Raw file is over 2 GB; converting to a centroided .mzXML file");

                // The ScanStats file should have been created by clsAnalysisResourcesMASIC
                // Rename it now so that we can replace the one created by MASIC with the one created by clsAnalysisResourcesMASIC
                fiScanStatsOverrideFile = new FileInfo(strScanStatsFilePath);
                fiScanStatsExOverrideFile = new FileInfo(strScanStatsExFilePath);

                if (!fiScanStatsOverrideFile.Exists)
                {
                    m_message = "ScanStats file not found (should have been created by clsAnalysisResourcesMASIC)";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                        m_message + ": " + fiScanStatsOverrideFile.FullName);
                    return CloseOutType.CLOSEOUT_FAILED;
                }
                else
                {
                    string strScanStatsOverrideFilePath = strScanStatsFilePath + ".override";
                    fiScanStatsOverrideFile.MoveTo(strScanStatsOverrideFilePath);
                }

                if (fiScanStatsExOverrideFile.Exists)
                {
                    string strScanStatsExOverrideFilePath = strScanStatsExFilePath + ".override";
                    fiScanStatsExOverrideFile.MoveTo(strScanStatsExOverrideFilePath);
                }

                string strMzXMLFilePath = null;
                strMzXMLFilePath = ConvertRawToMzXML(fiInputFile);

                if (string.IsNullOrEmpty(strMzXMLFilePath))
                {
                    if (string.IsNullOrEmpty(m_message))
                        m_message = "Empty path returned by ConvertRawToMzXML for " + fiInputFile.FullName;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                strInputFilePath = strMzXMLFilePath;

                m_EvalMessage = ".Raw file over 2 GB; converted to a centroided .mzXML file";
            }
            catch (Exception ex)
            {
                m_message = "Error preparing to convert the Raw file to a MzXML file";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    m_message + " (StartConvertRawToMzXML): " + ex.Message);
                fiScanStatsOverrideFile = null;
                fiScanStatsExOverrideFile = null;
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        #region "Event Handlers"

        private void mMSXmlCreator_LoopWaiting()
        {
            UpdateStatusFile();

            LogProgress("MSXmlCreator");
        }

        #endregion
    }
}
