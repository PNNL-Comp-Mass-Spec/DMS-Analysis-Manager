//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 01/30/2015
//
//*********************************************************************************************************

using AnalysisManagerBase;
using System;

namespace AnalysisManagerProMexPlugIn
{
    /// <summary>
    /// Retrieve resources for the ProMex plugin
    /// </summary>
    public class clsAnalysisResourcesProMex : clsAnalysisResources
    {

        /// <summary>
        /// Initialize options
        /// </summary>
        public override void Setup(string stepToolName, IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, clsMyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(stepToolName, mgrParams, jobParams, statusTools, myEMSLUtilities);
            SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, true);
        }

        /// <summary>
        /// Retrieve required files
        /// </summary>
        /// <returns>Closeout code</returns>
        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            // Get the ProMex parameter file

            var paramFileStoragePathKeyName = clsGlobal.STEP_TOOL_PARAM_FILE_STORAGE_PATH_PREFIX + "ProMex";

            var proMexParmFileStoragePath = mMgrParams.GetParam(paramFileStoragePathKeyName);
            if (string.IsNullOrEmpty(proMexParmFileStoragePath))
            {
                proMexParmFileStoragePath = @"C:\DMS_Programs\ProMex";
                LogErrorToDatabase("Parameter '" + paramFileStoragePathKeyName +
                    "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); " +
                    "will assume: " + proMexParmFileStoragePath);
            }

            string paramFileName;

            // If this is a ProMex script, the ProMex parameter file name is tracked as the job's parameter file
            // Otherwise, for MSPathFinder scripts, the ProMex parameter file is defined in the Job's settings file, and is thus accessible as job parameter ProMexParamFile

            // The ToolName job parameter holds the name of the job script we are executing
            var scriptName = mJobParams.GetParam("ToolName");

            var proMexScript = scriptName.StartsWith("ProMex", StringComparison.OrdinalIgnoreCase);
            var proMexBruker = IsProMexBrukerJob(mJobParams);

            if (proMexScript)
            {
                paramFileName = mJobParams.GetJobParameter("ParmFileName", "");

                if (string.IsNullOrEmpty(paramFileName))
                {
                    mMessage = "Job Parameter File name is empty";
                    LogError(mMessage);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mJobParams.AddAdditionalParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, "ProMexParamFile", paramFileName);
            }
            else
            {
                paramFileName = mJobParams.GetParam("ProMexParamFile");

                if (string.IsNullOrEmpty(paramFileName))
                {
                    // Settings file does not contain parameter ProMexParamFile
                    LogError("Parameter 'ProMexParamFile' is not defined in the settings file for this job");
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            if (!FileSearch.RetrieveFile(paramFileName, proMexParmFileStoragePath))
            {
                if (proMexScript)
                {
                    UpdateStatusMessage("see the parameter file name defined for this Analysis Job");
                }
                else
                {
                    UpdateStatusMessage("see the Analysis Job's settings file, entry ProMexParamFile");
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
            // The ToolName job parameter holds the name of the job script we are executing
            var scriptName = jobParams.GetParam("ToolName");
            var proMexBruker = scriptName.StartsWith("ProMex_Bruker", StringComparison.OrdinalIgnoreCase);

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

                mJobParams.AddResultFileExtensionToSkip(DOT_MZML_EXTENSION);

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in RetrieveMzMLFile: " + ex.Message;
                LogError(mMessage + "; task = " + currentTask + "; " + clsGlobal.GetExceptionStackTrace(ex));
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

                mJobParams.AddResultFileExtensionToSkip(DOT_PBF_EXTENSION);

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in RetrievePBFFile: " + ex.Message;
                LogError(mMessage + "; task = " + currentTask + "; " + clsGlobal.GetExceptionStackTrace(ex));
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }
    }
}
