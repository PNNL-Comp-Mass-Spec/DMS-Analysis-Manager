//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 10/12/2011
//
//*********************************************************************************************************

using AnalysisManagerBase;

namespace AnalysisManagerMSAlignHistonePlugIn
{
    public class clsAnalysisResourcesMSAlignHistone : clsAnalysisResources
    {
        public const string MSDECONV_MSALIGN_FILE_SUFFIX = "_msdeconv.msalign";

        public override void Setup(IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, clsMyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(mgrParams, jobParams, statusTools, myEMSLUtilities);
            SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, true);
        }

        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            // Make sure the machine has enough free memory to run MSAlign
            if (!ValidateFreeMemorySize("MSAlignJavaMemorySize", "MSAlign"))
            {
                m_message = "Not enough free memory to run MSAlign";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Retrieve param file
            if (!RetrieveFile(m_jobParams.GetParam("ParmFileName"), m_jobParams.GetParam("ParmFileStoragePath")))
                return CloseOutType.CLOSEOUT_FAILED;

            // Retrieve Fasta file
            if (!RetrieveOrgDB(m_mgrParams.GetParam("orgdbdir")))
                return CloseOutType.CLOSEOUT_FAILED;

            // Retrieve the MSAlign file
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting data files");
            var fileToGet = m_DatasetName + MSDECONV_MSALIGN_FILE_SUFFIX;
            if (!FindAndRetrieveMiscFiles(fileToGet, false))
            {
                //Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }
            m_jobParams.AddResultFileToSkip(fileToGet);

            if (!base.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }
    }
}
