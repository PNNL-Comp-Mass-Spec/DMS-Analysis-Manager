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
            var strMgfFileName = m_Dataset + ".mgf";
            var strInputFilePath = clsAnalysisResources.ResolveStoragePath(m_WorkDir, strMgfFileName);

            if (string.IsNullOrEmpty(strInputFilePath))
            {
                // Unable to resolve the file path
                m_ErrorMessage = "Could not find " + strMgfFileName + " or " + strMgfFileName + clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX +
                                 " in the working folder; unable to run MASIC";
                LogError(m_ErrorMessage);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return StartMASICAndWait(strInputFilePath, m_WorkDir, strParameterFilePath);
        }

        protected override CloseOutType DeleteDataFile()
        {
            // Deletes the .cdf and .mgf files from the working directory
            string[] foundFiles;

            // Delete the .cdf file
            try
            {
                foundFiles = Directory.GetFiles(m_WorkDir, "*.cdf");
                foreach (var MyFile in foundFiles)
                {
                    DeleteFileWithRetries(MyFile);
                }
            }
            catch (Exception ex)
            {
                LogError("Error deleting .cdf file", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Delete the .mgf file
            try
            {
                foundFiles = Directory.GetFiles(m_WorkDir, "*.mgf");
                foreach (var MyFile in foundFiles)
                {
                    DeleteFileWithRetries(MyFile);
                }
            }
            catch (Exception ex)
            {
                LogError("Error deleting .mgf file", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }
    }
}
