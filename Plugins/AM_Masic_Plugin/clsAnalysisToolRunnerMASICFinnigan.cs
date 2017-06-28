//*********************************************************************************************************
// Written by Matt Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//
//*********************************************************************************************************

using System;
using System.IO;
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

        protected override CloseOutType RunMASIC()
        {
            string strParameterFilePath;

            var strParameterFileName = m_jobParams.GetParam("parmFileName");

            if (strParameterFileName != null && strParameterFileName.Trim().ToLower() != "na")
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
                LogError(m_ErrorMessage);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Examine the size of the .Raw file
            var fiInputFile = new FileInfo(strInputFilePath);
            if (!fiInputFile.Exists)
            {
                // Unable to resolve the file path
                m_ErrorMessage = "Could not find " + fiInputFile.FullName + "; unable to run MASIC";
                LogError(m_ErrorMessage);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!string.IsNullOrEmpty(strParameterFilePath))
            {
                // Make sure the parameter file has IncludeHeaders defined and set to True
                ValidateParameterFile(strParameterFilePath);
            }

            var eCloseout = StartMASICAndWait(strInputFilePath, m_WorkDir, strParameterFilePath);

            return eCloseout;
        }

        /// <summary>
        /// Converts the .Raw file specified by fiThermoRawFile to a .mzXML file
        /// </summary>
        /// <param name="fiThermoRawFile"></param>
        /// <returns>Path to the newly created .mzXML file</returns>
        protected string ConvertRawToMzXML(FileInfo fiThermoRawFile)
        {
            var strMSXmlGeneratorAppPath = GetMSXmlGeneratorAppPath();

            mMSXmlCreator = new AnalysisManagerMsXmlGenPlugIn.clsMSXMLCreator(strMSXmlGeneratorAppPath, m_WorkDir, m_Dataset, m_DebugLevel, m_jobParams);
            RegisterEvents(mMSXmlCreator);
            mMSXmlCreator.LoopWaiting += mMSXmlCreator_LoopWaiting;

            var blnSuccess = mMSXmlCreator.CreateMZXMLFile();

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

            var strMzXMLFilePath = Path.ChangeExtension(fiThermoRawFile.FullName, "mzXML");
            if (!File.Exists(strMzXMLFilePath))
            {
                m_message = "MSXmlCreator did not create the .mzXML file";
                return string.Empty;
            }

            return strMzXMLFilePath;
        }

        /// <summary>
        /// Deletes the .raw file from the working directory
        /// </summary>
        /// <returns></returns>
        protected override CloseOutType DeleteDataFile()
        {
            try
            {
                var FoundFiles = Directory.GetFiles(m_WorkDir, "*.raw");
                foreach (var MyFile in FoundFiles)
                {
                    DeleteFileWithRetries(MyFile);
                }
                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error finding .raw files to delete", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
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
