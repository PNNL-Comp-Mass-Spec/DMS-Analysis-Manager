//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 01/30/2015
//
//*********************************************************************************************************

using System;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.StatusReporting;

namespace AnalysisManagerProMexPlugIn
{
    /// <summary>
    /// Retrieve resources for the ProMex plugin
    /// </summary>
    public class AnalysisResourcesProMex : AnalysisResources
    {
        /// <summary>
        /// Initialize options
        /// </summary>
        public override void Setup(string stepToolName, IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, MyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(stepToolName, mgrParams, jobParams, statusTools, myEMSLUtilities);
            SetOption(Global.AnalysisResourceOptions.OrgDbRequired, true);
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

            const string paramFileStoragePathKeyName = Global.STEP_TOOL_PARAM_FILE_STORAGE_PATH_PREFIX + "ProMex";

            var proMexParamFileStoragePath = mMgrParams.GetParam(paramFileStoragePathKeyName);

            if (string.IsNullOrEmpty(proMexParamFileStoragePath))
            {
                proMexParamFileStoragePath = @"C:\DMS_Programs\ProMex";
                LogErrorToDatabase("Parameter '" + paramFileStoragePathKeyName +
                    "' is not defined (obtained using V_Pipeline_Step_Tool_Storage_Paths in the Broker DB); " +
                    "will assume: " + proMexParamFileStoragePath);
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
                paramFileName = mJobParams.GetJobParameter("ParamFileName", "");

                if (string.IsNullOrEmpty(paramFileName))
                {
                    mMessage = "Job Parameter File name is empty";
                    LogError(mMessage);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mJobParams.AddAdditionalParameter(AnalysisJob.STEP_PARAMETERS_SECTION, "ProMexParamFile", paramFileName);
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

            if (!FileSearchTool.RetrieveFile(paramFileName, proMexParamFileStoragePath))
            {
                if (proMexScript)
                {
                    UpdateStatusMessage("see the parameter file name defined for this Analysis Job", true);
                }
                else
                {
                    UpdateStatusMessage("see the Analysis Job's settings file, entry ProMexParamFile", true);
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
        /// Returns true if this is a ProMex_Bruker job
        /// </summary>
        /// <param name="jobParams"></param>
        public static bool IsProMexBrukerJob(IJobParams jobParams)
        {
            // The ToolName job parameter holds the name of the job script we are executing
            var scriptName = jobParams.GetParam("ToolName");
            return scriptName.StartsWith("ProMex_Bruker", StringComparison.OrdinalIgnoreCase);
        }

        private CloseOutType RetrieveMzMLFile()
        {
            var currentTask = "Initializing";

            try
            {
                // Retrieve the .mzML file from the MSXml cache folder

                currentTask = "RetrieveMzMLFile";

                var result = GetMzMLFile();

                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                mJobParams.AddResultFileExtensionToSkip(DOT_MZML_EXTENSION);

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in RetrieveMzMLFile: " + ex.Message;
                LogError(mMessage + "; task = " + currentTask + "; " + Global.GetExceptionStackTrace(ex));
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private CloseOutType RetrievePBFFile()
        {
            var currentTask = "Initializing";

            try
            {
                // Retrieve the .pbf file from the MSXml cache folder

                currentTask = "RetrievePBFFile";

                var result = GetPBFFile();

                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                mJobParams.AddResultFileExtensionToSkip(DOT_PBF_EXTENSION);

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in RetrievePBFFile: " + ex.Message;
                LogError(mMessage + "; task = " + currentTask + "; " + Global.GetExceptionStackTrace(ex));
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }
    }
}
