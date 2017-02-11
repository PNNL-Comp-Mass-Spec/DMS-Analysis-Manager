using System;
using System.IO;
using AnalysisManagerBase;

namespace AnalysisManagerSequestPlugin
{
    public class clsAnalysisToolRunnerAgilentSeq : clsAnalysisToolRunnerSeqBase
    {
        protected CloseOutType DeleteDataFile()
        {
            //Deletes the data files (.mgf and .cdf) from the working directory
            string[] FoundFiles = null;

            try
            {
                //Delete the .mgf file
                FoundFiles = Directory.GetFiles(m_WorkDir, "*.mgf");
                foreach (string MyFile in FoundFiles)
                {
                    DeleteFileWithRetries(MyFile);
                }
                //Delete the .cdf file, if present
                FoundFiles = Directory.GetFiles(m_WorkDir, "*.cdf");
                foreach (string MyFile in FoundFiles)
                {
                    DeleteFileWithRetries(MyFile);
                }
            }
            catch (Exception Err)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR,
                    "Error deleting raw data file(s), job " + m_JobNum + ", step " + m_jobParams.GetParam("Step") + Err.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }
            return CloseOutType.CLOSEOUT_SUCCESS;
        }
    }
}
