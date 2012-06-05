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
        public override AnalysisManagerBase.IJobParams.CloseOutType GetResources()
        {

            try
            {
                if (m_DebugLevel >= 1)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Retrieving input files");

                    string dataPackageFolderPath = Path.Combine(m_jobParams.GetParam("transferFolderPath"), m_jobParams.GetParam("OutputFolderName"));
                    string analysisType = m_jobParams.GetParam("AnalysisType");

                    if (!CopyFileToWorkDir("Results.db3", Path.Combine(dataPackageFolderPath, m_jobParams.GetParam("StepInputFolderName")), m_WorkingDir))
                    {
                        //Errors were reported in function call, so just return
                        return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                    }

                    string strInputFileExtension = string.Empty;
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
