using System;
using System.IO;

using AnalysisManagerBase;

namespace AnalysisManager_IDM_Plugin
{
    public class clsAnalysisResourcesIDM : clsAnalysisResources
    {
        #region Members
        public static string AppFilePath = "";
        #endregion

        #region Methods
        public override IJobParams.CloseOutType GetResources()
        {

            try
            {
                // Retrieve shared resources, including the JobParameters file from the previous job step
                var result = GetSharedResources();
                if (result != IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                if (m_DebugLevel >= 1)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Retrieving input files");
                }

                var dataPackageFolderPath = Path.Combine(m_jobParams.GetParam("transferFolderPath"), m_jobParams.GetParam("OutputFolderName"));

                if (!CopyFileToWorkDir("Results.db3", Path.Combine(dataPackageFolderPath, m_jobParams.GetParam("StepInputFolderName")), m_WorkingDir))
                {
                    //Errors were reported in function call, so just return
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                bool useExistingIDMResults = m_jobParams.GetJobParameter("UseExistingIDMResults", false);

                if (useExistingIDMResults)
                {
                    var fiIDMResultsDB = new FileInfo(Path.Combine(dataPackageFolderPath, m_jobParams.GetParam("StepOutputFolderName"), "Results.db3"));
                    if (fiIDMResultsDB.Exists)
                    {
                        string targetFilePath = Path.Combine(m_WorkingDir, clsAnalysisToolRunnerIDM.EXISTING_IDM_RESULTS_FILE_NAME);
                        fiIDMResultsDB.CopyTo(targetFilePath);

                        m_jobParams.AddResultFileToSkip(clsAnalysisToolRunnerIDM.EXISTING_IDM_RESULTS_FILE_NAME);
                    }
                }

            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception retrieving resources", ex);
            }

            return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;
        }
        #endregion
    }
}
