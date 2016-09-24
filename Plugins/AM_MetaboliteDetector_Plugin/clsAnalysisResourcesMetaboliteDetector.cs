/*****************************************************************
** Written by Matthew Monroe for the US Department of Energy    **
** Pacific Northwest National Laboratory, Richland, WA          **
** Created 09/23/2016                                           **
**                                                              **
*****************************************************************/

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml.XPath;
using AnalysisManagerBase;

namespace AnalysisManagerMetaboliteDetectorPlugin
{
    public class clsAnalysisResourcesMetaboliteDetector : clsAnalysisResources
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public clsAnalysisResourcesMetaboliteDetector()
        {
           
        }

        public override IJobParams.CloseOutType GetResources()
        {

            var currentTask = "Initializing";

            try
            {
                // Retrieve the parameter file
                currentTask = "Retrieve the parameter file";
                var paramFileName = m_jobParams.GetParam("ParmFileName");
                var paramFileStoragePath = m_jobParams.GetParam("ParmFileStoragePath");

                var success = RetrieveFile(paramFileName, paramFileStoragePath);
                if (!success)
                {
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                

                currentTask = "Process the MyEMSL download queue";

                success = ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders);
                if (!success)
                {
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;

            }
            catch (Exception ex)
            {
                LogError("Exception in GetResources; task = " + currentTask, ex);
                return IJobParams.CloseOutType.CLOSEOUT_FAILED;
            }

        }


    }
}
