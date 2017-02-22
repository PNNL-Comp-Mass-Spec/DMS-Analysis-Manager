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
    /// Derived class for performing MASIC analysis on Agilent datasets
    /// </summary>
    /// <remarks></remarks>
    public class clsAnalysisToolRunnerMASICAgilent : clsAnalysisToolRunnerMASICBase
    {
        public clsAnalysisToolRunnerMASICAgilent()
        {
        }

        protected override CloseOutType RunMASIC()
        {
            string strParameterFileName = null;
            string strParameterFilePath = null;

            string strMgfFileName = null;
            string strInputFilePath = null;

            strParameterFileName = m_jobParams.GetParam("parmFileName");

            if ((strParameterFileName != null) && strParameterFileName.Trim().ToLower() != "na")
            {
                strParameterFilePath = Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"));
            }
            else
            {
                strParameterFilePath = string.Empty;
            }

            // Determine the path to the .Raw file
            strMgfFileName = m_Dataset + ".mgf";
            strInputFilePath = clsAnalysisResources.ResolveStoragePath(m_WorkDir, strMgfFileName);

            if (strInputFilePath == null || strInputFilePath.Length == 0)
            {
                // Unable to resolve the file path
                m_ErrorMessage = "Could not find " + strMgfFileName + " or " + strMgfFileName + clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX +
                                 " in the working folder; unable to run MASIC";
                LogError(m_ErrorMessage);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return base.StartMASICAndWait(strInputFilePath, m_WorkDir, strParameterFilePath);
        }

        protected override CloseOutType DeleteDataFile()
        {
            //Deletes the .cdf and .mgf files from the working directory
            string[] FoundFiles = null;

            //Delete the .cdf file
            try
            {
                FoundFiles = Directory.GetFiles(m_WorkDir, "*.cdf");
                foreach (string MyFile in FoundFiles)
                {
                    DeleteFileWithRetries(MyFile);
                }
            }
            catch (Exception ex)
            {
                LogError("Error deleting .cdf file", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            //Delete the .mgf file
            try
            {
                FoundFiles = Directory.GetFiles(m_WorkDir, "*.mgf");
                foreach (string MyFile in FoundFiles)
                {
                    DeleteFileWithRetries(MyFile);
                }
            }
            catch (Exception)
            {
                LogError("Error deleting .mgf file");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }
    }
}
