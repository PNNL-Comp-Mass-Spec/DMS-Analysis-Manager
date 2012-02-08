using AnalysisManagerBase;
using System.IO;
using System.Collections.Generic;
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

 
        /// <summary>
        /// Get name and version info for primary Mage MAC tool assembly
        /// </summary>
        /// <returns></returns>
        protected override string GetToolNameAndVersion() {
            string strToolVersionInfo = string.Empty;
            System.Reflection.AssemblyName oAssemblyName = System.Reflection.Assembly.Load("Mage").GetName();
            string strNameAndVersion = null;
            strNameAndVersion = oAssemblyName.Name + ", Version=" + oAssemblyName.Version.ToString();
            strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, strNameAndVersion);
            return strToolVersionInfo;
        }

       /// <summary>
        /// Get file version info for supplemental Mage assemblies
        /// </summary>
        /// <returns>List of file info for supplemental DLLs</returns>
        protected override List<System.IO.FileInfo> GetToolSupplementalVersionInfo() {
            System.Collections.Generic.List<System.IO.FileInfo> ioToolFiles = new System.Collections.Generic.List<System.IO.FileInfo>();
            ioToolFiles.Add(new System.IO.FileInfo("Mage.dll"));
            ioToolFiles.Add(new System.IO.FileInfo("MageExtContentFilters.dll"));
            ioToolFiles.Add(new System.IO.FileInfo("MageExtExtractionFilters.dll"));
            return ioToolFiles;
        }

    }
}