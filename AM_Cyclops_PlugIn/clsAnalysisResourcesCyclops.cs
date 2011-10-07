using System;
using AnalysisManagerBase;

namespace AnalysisManager_Cyclops_PlugIn
{
    public class clsAnalysisResourcesCyclops : clsAnalysisResources
    {

        public static string AppFilePath = "";

        public override AnalysisManagerBase.IJobParams.CloseOutType GetResources()
        {
            //Clear out list of files to delete or keep when packaging the blnSuccesss
            clsGlobal.ResetFilesToDeleteOrKeep();

            //m_jobParams.SetParam("ApeDbName") 
            //m_jobParams.SetParam("ApeWorkflow")
            //m_jobParams.SetParam("AnalysisType")
            m_jobParams.SetParam("ApeDbName", "johntest.db3");
            m_jobParams.SetParam("ApeWorkflow", "johntestWF.xml");
            m_jobParams.SetParam("AnalysisType", "SpectralCounting");

            if (!CopyFileToWorkDir("johntest.db3", "C:\\Dev\\", m_WorkingDir))
            {
                //Errors were reported in function call, so just return
                return IJobParams.CloseOutType.CLOSEOUT_FAILED;
            }

            if (!CopyFileToWorkDir("johntestWF.xml", "C:\\Dev\\", m_WorkingDir))
            {
                //Errors were reported in function call, so just return
                return IJobParams.CloseOutType.CLOSEOUT_FAILED;
            }

            string[] SplitString = null;
            string strApeWorkflowFileName = null;
            string strMAParameterFileStoragePath = null;
            string strParamFileStoragePathKeyName = null;
            string strInputFileExtension = string.Empty;

            // Retrieve the Parameter file name specified for this job
            strApeWorkflowFileName = m_jobParams.GetParam("ApeWorkflow");
            if (strApeWorkflowFileName == null || strApeWorkflowFileName.Length == 0)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Ape Workflow not defined in the settings for this job; unable to continue");
                return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            //***********Need to figure this out ***************
            //// Retrieve the Parameter file specified for this job
            //strParamFileStoragePathKeyName = AnalysisManagerBase.clsAnalysisMgrSettings.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX + "Ape\\" + m_jobParams.GetParam("AnalysisType");
            //strMAParameterFileStoragePath = m_mgrParams.GetParam(strParamFileStoragePathKeyName);
            //if (strMAParameterFileStoragePath == null || strMAParameterFileStoragePath.Length == 0)
            //{
            //    strMAParameterFileStoragePath = "\\\\gigasax\\DMS_Parameter_Files\\Ape\\" + m_jobParams.GetParam("AnalysisType");
            //    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Parameter '" + strParamFileStoragePathKeyName + "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " + strMAParameterFileStoragePath);
            //}

            ////Now copy the parameter file to the working directory
            //if (!CopyFileToWorkDir(strApeWorkflowFileName, strMAParameterFileStoragePath, m_WorkingDir))
            //{
            //    //Errors were reported in function call, so just return
            //    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
            //}

            if (m_DebugLevel >= 1)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Retrieving input files");
            }

            ////Retrieve all the files that are needed to run the analysis tool
            //if (!RetrieveAggregateFiles(SplitString))
            //{
            //    //Errors were reported in function call, so just return
            //    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
            //}

            return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;
        }

    }
}
