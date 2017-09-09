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

        private AnalysisManagerMsXmlGenPlugIn.clsMSXMLCreator mMSXmlCreator;

        #endregion

        protected override CloseOutType RunMASIC()
        {
            string parameterFilePath;

            var parameterFileName = m_jobParams.GetParam("parmFileName");

            if (parameterFileName != null && parameterFileName.Trim().ToLower() != "na")
            {
                parameterFilePath = Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"));
            }
            else
            {
                parameterFilePath = string.Empty;
            }

            // Determine the path to the .Raw file
            var rawFileName = m_Dataset + ".raw";
            var inputFilePath = clsAnalysisResources.ResolveStoragePath(m_WorkDir, rawFileName);

            if (string.IsNullOrWhiteSpace(inputFilePath))
            {
                // Unable to resolve the file path
                m_ErrorMessage = "Could not find " + rawFileName + " or " + rawFileName + clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX +
                                 " in the working folder; unable to run MASIC";
                LogError(m_ErrorMessage);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Examine the size of the .Raw file
            var fiInputFile = new FileInfo(inputFilePath);
            if (!fiInputFile.Exists)
            {
                // Unable to resolve the file path
                m_ErrorMessage = "Could not find " + fiInputFile.FullName + "; unable to run MASIC";
                LogError(m_ErrorMessage);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!string.IsNullOrEmpty(parameterFilePath))
            {
                // Make sure the parameter file has IncludeHeaders defined and set to True
                ValidateParameterFile(parameterFilePath);
            }

            var eCloseout = StartMASICAndWait(inputFilePath, m_WorkDir, parameterFilePath);

            return eCloseout;
        }

        /// <summary>
        /// Converts the .Raw file specified by fiThermoRawFile to a .mzXML file
        /// </summary>
        /// <param name="fiThermoRawFile"></param>
        /// <returns>Path to the newly created .mzXML file</returns>
        private string ConvertRawToMzXML(FileInfo fiThermoRawFile)
        {
            var msXmlGeneratorAppPath = GetMSXmlGeneratorAppPath();

            mMSXmlCreator = new AnalysisManagerMsXmlGenPlugIn.clsMSXMLCreator(msXmlGeneratorAppPath, m_WorkDir, m_Dataset, m_DebugLevel, m_jobParams);
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

            var mzXMLFilePath = Path.ChangeExtension(fiThermoRawFile.FullName, "mzXML");
            if (!File.Exists(mzXMLFilePath))
            {
                m_message = "MSXmlCreator did not create the .mzXML file";
                return string.Empty;
            }

            return mzXMLFilePath;
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
