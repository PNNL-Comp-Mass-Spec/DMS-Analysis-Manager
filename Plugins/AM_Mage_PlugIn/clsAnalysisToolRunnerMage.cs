using AnalysisManagerBase;
using System.IO;
using System.Collections.Generic;
using System;
using AnalysisManager_MAC;

namespace AnalysisManager_Mage_PlugIn {

    public class clsAnalysisToolRunnerMage : clsAnalysisToolRunnerMAC {

        /// <summary>
        /// sequentially run the Mage operations listed in "MageOperations" parameter
        /// </summary>
        protected override bool RunMACTool() {
            //Change the name of the log file for the local log file to the plug in log filename
            String logFileName = Path.Combine(m_WorkDir, "Mage_Log");
            log4net.GlobalContext.Properties["LogName"] = logFileName;
            clsLogTools.ChangeLogFileName(logFileName);

            // run the appropriate Mage pipeline(s) according to operations list parameter
            string mageOperations = m_jobParams.GetParam("MageOperations");
            var ops = new MageAMOperations(m_jobParams, m_mgrParams);
            bool ok = ops.RunMageOperations(mageOperations);

            // Change the name of the log file back to the analysis manager log file
            logFileName = m_mgrParams.GetParam("logfilename");
            log4net.GlobalContext.Properties["LogName"] = logFileName;
            clsLogTools.ChangeLogFileName(logFileName);

			if (!string.IsNullOrEmpty(ops.WarningMsg))
			{
				m_EvalMessage = ops.WarningMsg;
			}
            return ok;
        }

 
        /// <summary>
        /// Get name and version info for primary Mage MAC tool assembly
        /// </summary>
        /// <returns></returns>
        protected override string GetToolNameAndVersion() {
            string strToolVersionInfo = string.Empty;
            System.Reflection.AssemblyName oAssemblyName = System.Reflection.Assembly.Load("Mage").GetName();
            string strNameAndVersion = oAssemblyName.Name + ", Version=" + oAssemblyName.Version;
            strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, strNameAndVersion);
            return strToolVersionInfo;
        }

       /// <summary>
        /// Get file version info for supplemental Mage assemblies
        /// </summary>
        /// <returns>List of file info for supplemental DLLs</returns>
        protected override List<FileInfo> GetToolSupplementalVersionInfo() {
            var ioToolFiles = new List<FileInfo>
                                  {
                                      new FileInfo("Mage.dll"),
                                      new FileInfo("MageExtContentFilters.dll"),
                                      new FileInfo("MageExtExtractionFilters.dll")
                                  };
            return ioToolFiles;
        }

    }
}