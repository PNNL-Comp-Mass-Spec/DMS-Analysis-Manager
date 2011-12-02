using AnalysisManagerBase;
using System.IO;
using System;
using Mage;
using MageDisplayLib;
using AnalysisManager_MAC;

namespace AnalysisManager_Mage_PlugIn {

    public class clsAnalysisToolRunnerMage : clsAnalysisToolRunnerMAC {

        /// <summary>
        /// sequentially run the Mage operations listed in "MageOperations" parameter
        /// </summary>
        protected override bool RunMACTool() {
            bool ok = true;
            //Change the name of the log file for the local log file to the plug in log filename
            String LogFileName = Path.Combine(m_WorkDir, "Mage_Log");
            log4net.GlobalContext.Properties["LogName"] = LogFileName;
            clsLogTools.ChangeLogFileName(LogFileName);

            // run the appropriate Mage pipeline(s) according to operations list parameter
            string mageOperations = m_jobParams.GetParam("MageOperations");
            MageAMOperations ops = new MageAMOperations(m_jobParams, m_mgrParams);
            ok = ops.RunMageOperations(mageOperations);

            // Change the name of the log file back to the analysis manager log file
            LogFileName = m_mgrParams.GetParam("logfilename");
            log4net.GlobalContext.Properties["LogName"] = LogFileName;
            clsLogTools.ChangeLogFileName(LogFileName);

            return ok;
        }

    }
}