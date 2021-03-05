//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 07/19/2010
//
// Uses ReAdW to create a .mzXML or .mzML file
//*********************************************************************************************************

using AnalysisManagerBase;
using System;
using System.IO;

namespace AnalysisManagerMsXmlGenPlugIn
{
    public class clsMSXMLGenReadW : clsMSXmlGen
    {
        // Ignore Spelling: centroiding

        protected override string ProgramName => "ReAdW";

        #region "Methods"

        public clsMSXMLGenReadW(
            string workDir,
            string readWProgramPath,
            string datasetName,
            clsAnalysisResources.eRawDataTypeConstants rawDataType,
            clsAnalysisResources.MSXMLOutputTypeConstants outputType,
            bool centroidMSXML,
            IJobParams jobParams)
            : base(workDir, readWProgramPath, datasetName, clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile, outputType, centroidMSXML, jobParams)
        {
            if (rawDataType != clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile)
            {
                throw new ArgumentOutOfRangeException(nameof(rawDataType), "clsMSXMLGenReadW can only be used to process Thermo .Raw files");
            }

            mUseProgRunnerResultCode = true;
        }

        protected override string CreateArguments(string msXmlFormat, string rawFilePath)
        {
            string arguments;

            if (!msXmlFormat.Equals(MZXML_FILE_FORMAT) && !msXmlFormat.Equals(MZML_FILE_FORMAT))
            {
                throw new ArgumentOutOfRangeException(nameof(msXmlFormat), "ReAdW only supports mzXML and mzML as an output format, not " + msXmlFormat);
            }

            if (mProgramPath.IndexOf(@"\v2.", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                // Version 2.x syntax
                // Syntax is: readw <raw file path> <c/p> [<output file>]

                if (mCentroidMS1 || mCentroidMS2)
                {
                    // Centroiding is enabled
                    arguments = " " + rawFilePath + " c";
                }
                else
                {
                    arguments = " " + rawFilePath + " p";
                }
            }
            else
            {
                // Version 3 or higher
                // Syntax is ReAdW [options] <raw file path> [<output file>]
                //  where Options will include --mzXML and possibly -c

                if (mCentroidMS1 || mCentroidMS2)
                {
                    // Centroiding is enabled
                    arguments = " --" + msXmlFormat + " " + " -c " + rawFilePath;
                }
                else
                {
                    // Not centroiding
                    arguments = " --" + msXmlFormat + " " + rawFilePath;
                }
            }

            mOutputFileName = GetOutputFileName(msXmlFormat, rawFilePath, mRawDataType);

            return arguments;
        }

        protected override string GetOutputFileName(string msXmlFormat, string rawFilePath, clsAnalysisResources.eRawDataTypeConstants rawDataType)
        {
            return Path.GetFileName(Path.ChangeExtension(rawFilePath, msXmlFormat));
        }

        protected override bool SetupTool()
        {
            // No special setup is required for ReAdW
            return true;
        }

        #endregion
    }
}
