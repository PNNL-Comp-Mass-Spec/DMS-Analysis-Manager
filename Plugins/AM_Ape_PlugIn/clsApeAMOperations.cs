using AnalysisManagerBase;

namespace AnalysisManager_Ape_PlugIn
{
    public class clsApeAMOperations
    {

        #region Member Variables

        protected IJobParams m_jobParams;

        protected IMgrParams m_mgrParams;
        
        private string mErrorMessage = string.Empty;

        #endregion

        #region "Properties"

        public string ErrorMessage
        {
            get { return mErrorMessage; }
        }

        #endregion

        #region Constructors

        public clsApeAMOperations(IJobParams jobParms, IMgrParams mgrParms) {
            m_jobParams = jobParms;
            m_mgrParams = mgrParms;
        }

        #endregion

        /// <summary>
        /// Run a list of Ape operations
        /// </summary>
        /// <param name="apeOperations"></param>
        /// <returns></returns>
        public bool RunApeOperations(string apeOperations) {
            bool ok = false;
            foreach (string apeOperation in apeOperations.Split(',')) {
                ok = RunApeOperation(apeOperation.Trim());
                if (!ok) break;
            }
            return ok;
        }

        #region Ape Operations

        /// <summary>
        /// Run defined Ape operation(s)
        /// </summary>
        /// <param name="apeOperation"></param>
        /// <returns></returns>
        private bool RunApeOperation(string apeOperation)
        {
            bool blnSuccess;

            // Note: case statements must be lowercase
            switch (apeOperation.ToLower())
            {
                case "runworkflow":
                    var apeWfObj = new clsApeAMRunWorkflow(m_jobParams, m_mgrParams);

                    // Attach the progress event handler
                    apeWfObj.ProgressChanged += ApeProgressChanged;

                    blnSuccess = apeWfObj.RunWorkflow();

                    if (!blnSuccess)
                        mErrorMessage = "Error running apeWorkflow: " + apeWfObj.ErrorMessage;

                    break;

                case "getimprovresults":
                    var apeImpObj = new clsApeAMGetImprovResults(m_jobParams, m_mgrParams);

                    // Attach the progress event handler
                    apeImpObj.ProgressChanged += ApeProgressChanged;

                    blnSuccess = apeImpObj.GetImprovResults(m_jobParams.GetParam("DataPackageID"));

                    if (!blnSuccess)
                        mErrorMessage = "Error getting ImprovResults: " + apeImpObj.ErrorMessage;

                    break;

                case "getqrollupresults":
                    var apeQImpObj = new clsApeAMGetQRollupResults(m_jobParams, m_mgrParams);

                    // Attach the progress event handler
                    apeQImpObj.ProgressChanged += ApeProgressChanged;

                    blnSuccess = apeQImpObj.GetQRollupResults(m_jobParams.GetParam("DataPackageID"));

                    if (!blnSuccess)
                        mErrorMessage = "Error obtaining QRollup Results: " + apeQImpObj.ErrorMessage;

                    break;

                case "getviperresults":
                    var apeVImpObj = new clsApeAMGetViperResults(m_jobParams, m_mgrParams);

                    // Attach the progress event handler
                    apeVImpObj.ProgressChanged += ApeProgressChanged;

                    blnSuccess = apeVImpObj.GetQRollupResults(m_jobParams.GetParam("DataPackageID"));

                    if (!blnSuccess)
                        mErrorMessage = "Error obtaining VIPER results: " + apeVImpObj.ErrorMessage;

                    break;

                default:
                    blnSuccess = false;
                    mErrorMessage = "Ape Operation: " + apeOperation + "not recognized";                    
                    break;
            }
            return blnSuccess;
        }


        void ApeProgressChanged(object sender, clsApeAMBase.ProgressChangedEventArgs e)
        {

            // Update the step tool progress
            // However, Ape routinely reports progress of 0% or 100% at the start and end of certain subtasks, so ignore those values
            //if (e.percentComplete > 0 && e.percentComplete < 100)
                //m_progress = PROGRESS_PCT_APE_START + (PROGRESS_PCT_APE_DONE - PROGRESS_PCT_APE_START) * e.percentComplete / 100.0F;

            //if (!string.IsNullOrEmpty(e.taskDescription))
                //m_CurrentApeTask = e.taskDescription;

            //if (System.DateTime.UtcNow.Subtract(m_LastStatusUpdateTime).TotalSeconds >= 10)
            //{
            //    m_LastStatusUpdateTime = System.DateTime.UtcNow;
            //    m_StatusTools.UpdateAndWrite(EnumMgrStatus.RUNNING, EnumTaskStatus.RUNNING, EnumTaskStatusDetail.RUNNING_TOOL, m_progress);
            //}
        }

        #endregion


    }
}
