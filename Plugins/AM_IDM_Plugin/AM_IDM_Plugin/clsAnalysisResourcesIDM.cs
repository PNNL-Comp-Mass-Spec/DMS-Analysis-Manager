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
        public override CloseOutType GetResources()
        {

            try
            {
                // Retrieve shared resources, including the JobParameters file from the previous job step
                var result = GetSharedResources();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                if (m_DebugLevel >= 1)
                {
                    LogMessage("Retrieving input files");
                }

                var dataPackageFolderPath = Path.Combine(m_jobParams.GetParam("transferFolderPath"), m_jobParams.GetParam("OutputFolderName"));

                if (!CopyFileToWorkDir("Results.db3", Path.Combine(dataPackageFolderPath, m_jobParams.GetParam("StepInputFolderName")), m_WorkingDir))
                {
                    //Errors were reported in function call, so just return
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var useExistingIDMResults = m_jobParams.GetJobParameter("UseExistingIDMResults", false);

                if (useExistingIDMResults)
                {
                    var fiIDMResultsDB = new FileInfo(Path.Combine(dataPackageFolderPath, m_jobParams.GetParam("StepOutputFolderName"), "Results.db3"));
                    if (fiIDMResultsDB.Exists)
                    {
                        var targetFilePath = Path.Combine(m_WorkingDir, clsAnalysisToolRunnerIDM.EXISTING_IDM_RESULTS_FILE_NAME);
                        fiIDMResultsDB.CopyTo(targetFilePath);

                        m_jobParams.AddResultFileToSkip(clsAnalysisToolRunnerIDM.EXISTING_IDM_RESULTS_FILE_NAME);
                    }
                }

            }
            catch (Exception ex)
            {
                LogError("Exception retrieving resources", ex);
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }
        #endregion
    }
}
