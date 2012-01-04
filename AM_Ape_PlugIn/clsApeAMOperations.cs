using System;
using AnalysisManagerBase;

namespace AnalysisManager_Ape_PlugIn
{
    public class clsApeAMOperations
    {

        #region Member Variables

        protected IJobParams m_jobParams;

        protected IMgrParams m_mgrParams;

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
        /// <param name="mageOperations"></param>
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
            bool blnSuccess = false;

            // Note: case statements must be lowercase
            switch (apeOperation.ToLower())
            {
                case "runworkflow":
                    clsApeAMRunWorkflow apeWfObj = new clsApeAMRunWorkflow(m_jobParams, m_mgrParams);

                    // Attach the progress event handler
                    apeWfObj.ProgressChanged += new clsApeAMBase.ProgressChangedEventHandler(ApeProgressChanged);

                    blnSuccess = apeWfObj.RunWorkflow(m_jobParams.GetParam("DataPackageID"));

                    break;

                case "getimprovresults":
                    clsApeAMGetImprovResults apeImpObj = new clsApeAMGetImprovResults(m_jobParams, m_mgrParams);

                    // Attach the progress event handler
                    apeImpObj.ProgressChanged += new clsApeAMBase.ProgressChangedEventHandler(ApeProgressChanged);

                    blnSuccess = apeImpObj.GetImprovResults(m_jobParams.GetParam("DataPackageID"));

                    break;

                default:
                    blnSuccess = false;
                    //m_message = "Ape Operation: " + apeOperation + "not recognized.";
                    // Future: throw an error
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
            //    m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress);
            //}
        }

        #endregion


    }
}
