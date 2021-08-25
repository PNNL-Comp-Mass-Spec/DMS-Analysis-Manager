using System;
using System.IO;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerSequestPlugin
{
    public class AnalysisToolRunnerAgilentSeq : AnalysisToolRunnerSeqBase
    {
        // Ignore Spelling: cdf, mgf

        private CloseOutType DeleteDataFile()
        {
            // Deletes the data files (.mgf and .cdf) from the working directory

            try
            {
                // Delete the .mgf file
                var mgfFiles = Directory.GetFiles(mWorkDir, "*.mgf");
                foreach (var file in mgfFiles)
                {
                    DeleteFileWithRetries(file);
                }

                // Delete the .cdf file, if present
                var cdfFiles = Directory.GetFiles(mWorkDir, "*.cdf");
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
