using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using AnalysisManagerBase;

namespace AnalysisManager_AScore_PlugIn
{
    class clsAScoreAMRunPhospho : clsAScoreAMBase
    {
        #region Member Variables
   
        #endregion

        #region Constructors

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="jobParms"></param>
        /// <param name="mgrParms"></param>
        /// <param name="monitor"></param>
        public clsAScoreAMRunPhospho(IJobParams jobParms, IMgrParams mgrParms) : base(jobParms, mgrParms)
        {      
        }

        #endregion

        /// <summary>
        /// Setup and run AScore pipeline according to job parameters
        /// </summary>
        public bool RunPhospho(String dataPackageID)
        {
            bool blnSuccess = true;

            clsAScoreMage dvas = new clsAScoreMage(mJobParms, mMgrParms);
            dvas.Run();


            return blnSuccess;
        }



    }
}
