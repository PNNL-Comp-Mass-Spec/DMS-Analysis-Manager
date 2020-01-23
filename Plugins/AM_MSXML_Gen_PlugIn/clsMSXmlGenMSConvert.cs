//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 05/10/2011
//
// Uses MSConvert to create a .mzXML or .mzML file
// Also used by RecalculatePrecursorIonsUpdateMzML in clsAnalysisToolRunnerMSXMLGen to re-index a .mzML file to create a new .mzML file
//*********************************************************************************************************

using AnalysisManagerBase;
using System;
using System.IO;

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

        protected override string ProgramName => "MSConvert";

        #region "Methods"

        public clsMSXmlGenMSConvert(
            string workDir,
            string msConvertProgramPath,
            string datasetName,
            clsAnalysisResources.eRawDataTypeConstants rawDataType,
            clsAnalysisResources.MSXMLOutputTypeConstants eOutputType,
            string customMSConvertArguments,
            IJobParams jobParams) : base(workDir, msConvertProgramPath, datasetName, rawDataType, eOutputType, centroidMSXML: false, jobParams: jobParams)
        {
            mCustomMSConvertArguments = customMSConvertArguments;

            mUseProgRunnerResultCode = false;
        }

        public clsMSXmlGenMSConvert(
            string workDir,
            string msConvertProgramPath,
            string datasetName,
            clsAnalysisResources.eRawDataTypeConstants rawDataType,
            clsAnalysisResources.MSXMLOutputTypeConstants eOutputType,
            bool centroidMSXML,
            int centroidPeakCountToRetain,
            IJobParams jobParams) : base(workDir, msConvertProgramPath, datasetName, rawDataType, eOutputType, centroidMSXML, jobParams: jobParams)
        {
            mCentroidPeakCountToRetain = centroidPeakCountToRetain;

            mUseProgRunnerResultCode = false;
        }

        public clsMSXmlGenMSConvert(
            string workDir,
            string msConvertProgramPath,
            string datasetName,
            clsAnalysisResources.eRawDataTypeConstants rawDataType,
            clsAnalysisResources.MSXMLOutputTypeConstants eOutputType,
            bool centroidMS1,
            bool centroidMS2,
            int centroidPeakCountToRetain,
            IJobParams jobParams)
            : base(workDir, msConvertProgramPath, datasetName, rawDataType, eOutputType, centroidMS1, centroidMS2, jobParams)
        {
            mCentroidPeakCountToRetain = centroidPeakCountToRetain;

            mUseProgRunnerResultCode = false;
        }

        protected override string CreateArguments(string msXmlFormat, string rawFilePath)
        {
            var arguments = " " + clsGlobal.PossiblyQuotePath(rawFilePath);

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

                    if (mCentroidMS1 && !mCentroidMS2)
                    {
                        arguments += " --filter \"peakPicking true 1\"";
                    }
                    else if (!mCentroidMS1 && mCentroidMS2)
                    {
                        arguments += " --filter \"peakPicking true 2-\"";
                    }
                    else
                    {
                        arguments += " --filter \"peakPicking true 1-\"";
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

                        arguments += " --filter \"threshold count " + mCentroidPeakCountToRetain + " most-intense\"";
                    }
                }

                arguments += " --" + msXmlFormat + " --32";
            }
            else
            {
                // Replace single quotes with double quotes
                // For example, switch from:
                // --filter 'peakPicking true 1-' --filter 'scanNumber [0,24427] [24441,49450]' --mzML --32
                // to:
                // --filter "peakPicking true 1-" --filter "scanNumber [0,24427] [24441,49450]" --mzML --32

                arguments += " " + mCustomMSConvertArguments.Replace("'", "\"");
            }

            mOutputFileName = GetOutputFileName(msXmlFormat, rawFilePath, mRawDataType);

            // Instruct MSConvert to use a single thread to avoid it freezing if one of the threads throws an exception
            arguments += " --singleThreaded";

            // Specify the output directory and the output file name
            arguments += "  -o " + mWorkDir + " --outfile " + mOutputFileName;

            return arguments;
        }

        protected override string GetOutputFileName(string msXmlFormat, string rawFilePath, clsAnalysisResources.eRawDataTypeConstants rawDataType)
        {
            if (string.Equals(msXmlFormat, MZML_FILE_FORMAT, StringComparison.OrdinalIgnoreCase) &&
                mRawDataType == clsAnalysisResources.eRawDataTypeConstants.mzML)
            {
                // Input and output files are both .mzML
                return Path.GetFileNameWithoutExtension(rawFilePath) + "_new" + clsAnalysisResources.DOT_MZML_EXTENSION;
            }

            if (string.Equals(msXmlFormat, MZXML_FILE_FORMAT, StringComparison.OrdinalIgnoreCase) &&
                mRawDataType == clsAnalysisResources.eRawDataTypeConstants.mzXML)
            {
                // Input and output files are both .mzXML
                return Path.GetFileNameWithoutExtension(rawFilePath) + "_new" + clsAnalysisResources.DOT_MZXML_EXTENSION;
            }

            return Path.GetFileName(Path.ChangeExtension(rawFilePath, msXmlFormat));
        }

        protected override bool SetupTool()
        {
            // Tool setup for MSConvert involves creating a
            //  registry entry at HKEY_CURRENT_USER\Software\ProteoWizard
            //  to indicate that we agree to the Thermo license

            var proteowizardTools = new clsProteowizardTools(DebugLevel);

            return proteowizardTools.RegisterProteoWizard();
        }

        #endregion
    }
}
