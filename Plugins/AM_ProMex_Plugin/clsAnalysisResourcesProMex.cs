//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 01/30/2015
//
//*********************************************************************************************************

using System;
using AnalysisManagerBase;

namespace AnalysisManagerProMexPlugIn
{
    public class clsAnalysisResourcesProMex : clsAnalysisResources
    {
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

            // Get the ProMex parameter file

            string paramFileStoragePathKeyName = null;
            string proMexParmFileStoragePath = null;
            paramFileStoragePathKeyName = clsGlobal.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX + "ProMex";

            proMexParmFileStoragePath = m_mgrParams.GetParam(paramFileStoragePathKeyName);
            if (string.IsNullOrEmpty(proMexParmFileStoragePath))
            {
                proMexParmFileStoragePath = "C:\\DMS_Programs\\ProMex";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN,
                    "Parameter '" + paramFileStoragePathKeyName +
                    "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " +
                    proMexParmFileStoragePath);
            }

            string paramFileName = null;

            // If this is a ProMex script, then the ProMex parameter file name is tracked as the job's parameter file
            // Otherwise, for MSPathFinder scripts, the ProMex parameter file is defined in the Job's settings file, and is thus accessible as job parameter ProMexParamFile

            var toolName = m_jobParams.GetParam("ToolName");
            var proMexScript = toolName.StartsWith("ProMex", StringComparison.CurrentCultureIgnoreCase);
            var proMexBruker = IsProMexBrukerJob(m_jobParams);

            if (proMexScript)
            {
                paramFileName = m_jobParams.GetJobParameter("ParmFileName", "");

                if (string.IsNullOrEmpty(paramFileName))
                {
                    m_message = "Job Parameter File name is empty";
                    LogError(m_message);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                m_jobParams.AddAdditionalParameter("StepParameters", "ProMexParamFile", paramFileName);
            }
            else
            {
                paramFileName = m_jobParams.GetParam("ProMexParamFile");

                if (string.IsNullOrEmpty(paramFileName))
                {
                    // Settings file does not contain parameter ProMexParamFile
                    m_message = "Parameter 'ProMexParamFile' is not defined in the settings file for this job";
                    LogError(m_message);
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            if (!FileSearch.RetrieveFile(paramFileName, proMexParmFileStoragePath))
            {
                if (proMexScript)
                {
                    m_message = clsGlobal.AppendToComment(m_message, "see the parameter file name defined for this Analysis Job");
                }
                else
                {
                    m_message = clsGlobal.AppendToComment(m_message, "see the Analysis Job's settings file, entry ProMexParamFile");
                }
                return CloseOutType.CLOSEOUT_FAILED;
            }

            CloseOutType eResult;

            if (proMexBruker)
            {
                // Retrieve the mzML file
                // Note that ProMex will create a PBF file using the .mzML file
                eResult = RetrieveMzMLFile();
            }
            else
            {
                // Retrieve the PBF file
                eResult = RetrievePBFFile();
            }

            if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return eResult;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Returns True if this is a ProMex_Bruker job
        /// </summary>
        /// <param name="jobParams"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static bool IsProMexBrukerJob(IJobParams jobParams)
        {
            var toolName = jobParams.GetParam("ToolName");
            var proMexBruker = toolName.StartsWith("ProMex_Bruker", StringComparison.CurrentCultureIgnoreCase);

            return proMexBruker;
        }

        protected CloseOutType RetrieveMzMLFile()
        {
            var currentTask = "Initializing";

            try
            {
                // Retrieve the .mzML file from the MSXml cache folder

                currentTask = "RetrieveMzMLFile";

                var eResult = GetMzMLFile();
                if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return eResult;
                }

                m_jobParams.AddResultFileExtensionToSkip(DOT_MZML_EXTENSION);

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                m_message = "Exception in RetrieveMzMLFile: " + ex.Message;
                LogError(
                    m_message + "; task = " + currentTask + "; " + clsGlobal.GetExceptionStackTrace(ex));
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        protected CloseOutType RetrievePBFFile()
        {
            var currentTask = "Initializing";

            try
            {
                // Retrieve the .pbf file from the MSXml cache folder

                currentTask = "RetrievePBFFile";

                var eResult = GetPBFFile();
                if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return eResult;
                }

                m_jobParams.AddResultFileExtensionToSkip(DOT_PBF_EXTENSION);

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                m_message = "Exception in RetrievePBFFile: " + ex.Message;
                LogError(
                    m_message + "; task = " + currentTask + "; " + clsGlobal.GetExceptionStackTrace(ex));
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }
    }
}
