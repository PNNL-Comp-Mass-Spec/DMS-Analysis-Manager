//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 07/19/2010
//
// Uses ReAdW to create a .mzXML or .mzML file
//*********************************************************************************************************

using System;
using System.IO;
using AnalysisManagerBase;

namespace AnalysisManagerMsXmlGenPlugIn
{
    public class clsMSXMLGenReadW : clsMSXmlGen
    {
        protected override string ProgramName
        {
            get { return "ReAdW"; }
        }

        #region "Methods"

        public clsMSXMLGenReadW(string WorkDir, string ReadWProgramPath, string DatasetName, clsAnalysisResources.eRawDataTypeConstants RawDataType,
            clsAnalysisResources.MSXMLOutputTypeConstants eOutputType, bool CentroidMSXML)
            : base(WorkDir, ReadWProgramPath, DatasetName, clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile, eOutputType, CentroidMSXML)
        {
            if (RawDataType != clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile)
            {
                throw new ArgumentOutOfRangeException("clsMSXMLGenReadW can only be used to process Thermo .Raw files");
            }

            mUseProgRunnerResultCode = true;
        }

        protected override string CreateArguments(string msXmlFormat, string RawFilePath)
        {
            string cmdStr = null;

            if (mProgramPath.ToLower().Contains("\\v2."))
            {
                // Version 2.x syntax
                // Syntax is: readw <raw file path> <c/p> [<output file>]

                if (mCentroidMS1 || mCentroidMS2)
                {
                    // Centroiding is enabled
                    cmdStr = " " + RawFilePath + " c";
                }
                else
                {
                    cmdStr = " " + RawFilePath + " p";
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
                    cmdStr = " --" + msXmlFormat + " " + " -c " + RawFilePath;
                }
                else
                {
                    // Not centroiding
                    cmdStr = " --" + msXmlFormat + " " + RawFilePath;
                }
            }

            mOutputFileName = GetOutputFileName(msXmlFormat, RawFilePath, mRawDataType);

            return cmdStr;
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
