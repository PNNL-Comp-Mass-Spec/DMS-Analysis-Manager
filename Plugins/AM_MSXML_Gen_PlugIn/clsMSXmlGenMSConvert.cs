//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 05/10/2011
//
// Uses MSConvert to create a .mzXML or .mzML file
// Also used by RecalculatePrecursorIonsUpdateMzML in clsAnalysisToolRunnerMSXMLGen to re-index a .mzML file to create a new .mzML file
//*********************************************************************************************************

using System;
using System.IO;
using AnalysisManagerBase;

namespace AnalysisManagerMsXmlGenPlugIn
{
    public class clsMSXmlGenMSConvert : clsMSXmlGen
    {
        public const int DEFAULT_CENTROID_PEAK_COUNT_TO_RETAIN = 500;

        /// <summary>
        /// Number of data points to keep when centroiding
        /// </summary>
        /// <remarks>0 to keep default (500); -1 to keep all</remarks>
        private int mCentroidPeakCountToRetain;

        /// <summary>
        /// Custom arguments that will override the auto-defined arguments
        /// </summary>
        /// <remarks></remarks>
        private readonly string mCustomMSConvertArguments;

        protected override string ProgramName
        {
            get { return "MSConvert"; }
        }

        #region "Methods"

        public clsMSXmlGenMSConvert(string WorkDir, string msConvertProgramPath, string datasetName,
            clsAnalysisResources.eRawDataTypeConstants rawDataType, clsAnalysisResources.MSXMLOutputTypeConstants eOutputType,
            string customMSConvertArguments) : base(WorkDir, msConvertProgramPath, datasetName, rawDataType, eOutputType, centroidMSXML: false)
        {
            mCustomMSConvertArguments = customMSConvertArguments;

            mUseProgRunnerResultCode = false;
        }

        public clsMSXmlGenMSConvert(string workDir, string msConvertProgramPath, string datasetName,
            clsAnalysisResources.eRawDataTypeConstants rawDataType, clsAnalysisResources.MSXMLOutputTypeConstants eOutputType, bool centroidMSXML,
            int centroidPeakCountToRetain) : base(workDir, msConvertProgramPath, datasetName, rawDataType, eOutputType, centroidMSXML)
        {
            mCentroidPeakCountToRetain = centroidPeakCountToRetain;

            mUseProgRunnerResultCode = false;
        }

        public clsMSXmlGenMSConvert(string workDir, string msConvertProgramPath, string datasetName,
            clsAnalysisResources.eRawDataTypeConstants rawDataType, clsAnalysisResources.MSXMLOutputTypeConstants eOutputType, bool centroidMS1,
            bool centroidMS2, int centroidPeakCountToRetain)
            : base(workDir, msConvertProgramPath, datasetName, rawDataType, eOutputType, centroidMS1, centroidMS2)
        {
            mCentroidPeakCountToRetain = centroidPeakCountToRetain;

            mUseProgRunnerResultCode = false;
        }

        protected override string CreateArguments(string msXmlFormat, string rawFilePath)
        {
            var cmdStr = " " + clsGlobal.PossiblyQuotePath(rawFilePath);

            if (string.IsNullOrWhiteSpace(mCustomMSConvertArguments))
            {
                if (mCentroidMS1 || mCentroidMS2)
                {
                    // Centroid the data by first applying the peak-picking algorithm, then keeping the top N data points
                    // Syntax details:
                    //   peakPicking prefer_vendor:<true|false>  int_set(MS levels)
                    //   threshold <count|count-after-ties|absolute|bpi-relative|tic-relative|tic-cutoff> <threshold> <most-intense|least-intense> [int_set(MS levels)]

                    // So, the following means to apply peak picking to all spectra (MS1 and MS2) and then keep the top 150 peaks (sorted by intensity)
                    // --filter "peakPicking true 1-" --filter "threshold count 150 most-intense"

                    if (mCentroidMS1 & !mCentroidMS2)
                    {
                        cmdStr += " --filter \"peakPicking true 1\"";
                    }
                    else if (!mCentroidMS1 & mCentroidMS2)
                    {
                        cmdStr += " --filter \"peakPicking true 2-\"";
                    }
                    else
                    {
                        cmdStr += " --filter \"peakPicking true 1-\"";
                    }

                    if (mCentroidPeakCountToRetain < 0)
                    {
                        // Keep all points
                    }
                    else
                    {
                        if (mCentroidPeakCountToRetain == 0)
                        {
                            mCentroidPeakCountToRetain = DEFAULT_CENTROID_PEAK_COUNT_TO_RETAIN;
                        }
                        else if (mCentroidPeakCountToRetain < 25)
                        {
                            mCentroidPeakCountToRetain = 25;
                        }

                        cmdStr += " --filter \"threshold count " + mCentroidPeakCountToRetain + " most-intense\"";
                    }
                }

                cmdStr += " --" + msXmlFormat + " --32";
            }
            else
            {
                cmdStr += " " + mCustomMSConvertArguments;
            }

            mOutputFileName = GetOutputFileName(msXmlFormat, rawFilePath, mRawDataType);

            // Specify the output directory and the output file name
            cmdStr += "  -o " + mWorkDir + " --outfile " + mOutputFileName;

            return cmdStr;
        }

        protected override string GetOutputFileName(string msXmlFormat, string rawFilePath, clsAnalysisResources.eRawDataTypeConstants rawDataType)
        {
            if (string.Equals(msXmlFormat, "mzML", StringComparison.InvariantCultureIgnoreCase) &&
                mRawDataType == clsAnalysisResources.eRawDataTypeConstants.mzML)
            {
                // Input and output files are both .mzML
                return Path.GetFileNameWithoutExtension(rawFilePath) + "_new" + clsAnalysisResources.DOT_MZML_EXTENSION;
            }
            else if (string.Equals(msXmlFormat, "mzXML", StringComparison.InvariantCultureIgnoreCase) &&
                     mRawDataType == clsAnalysisResources.eRawDataTypeConstants.mzXML)
            {
                // Input and output files are both .mzXML
                return Path.GetFileNameWithoutExtension(rawFilePath) + "_new" + clsAnalysisResources.DOT_MZXML_EXTENSION;
            }
            else
            {
                return Path.GetFileName(Path.ChangeExtension(rawFilePath, msXmlFormat));
            }
        }

        protected override bool SetupTool()
        {
            // Tool setup for MSConvert involves creating a
            //  registry entry at HKEY_CURRENT_USER\Software\ProteoWizard
            //  to indicate that we agree to the Thermo license

            var objProteowizardTools = new clsProteowizardTools(DebugLevel);

            return objProteowizardTools.RegisterProteoWizard();
        }

        #endregion
    }
}
