using System;
using System.IO;
using AnalysisManagerBase;

namespace AnalysisManagerSequestPlugin
{
    public class clsAnalysisToolRunnerAgilentSeq : clsAnalysisToolRunnerSeqBase
    {
        protected CloseOutType DeleteDataFile()
        {
            // Deletes the data files (.mgf and .cdf) from the working directory

            try
            {
                // Delete the .mgf file
                var mgfFiles = Directory.GetFiles(m_WorkDir, "*.mgf");
                foreach (var file in mgfFiles)
                {
                    DeleteFileWithRetries(file);
                }

                // Delete the .cdf file, if present
                var cdfFiles = Directory.GetFiles(m_WorkDir, "*.cdf");
                foreach (var file in cdfFiles)
                {
                    DeleteFileWithRetries(file);
                }
            }
            catch (Exception ex)
            {
                LogError("Error deleting mgf and cdf file(s)", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
            return CloseOutType.CLOSEOUT_SUCCESS;
        }
    }
}
