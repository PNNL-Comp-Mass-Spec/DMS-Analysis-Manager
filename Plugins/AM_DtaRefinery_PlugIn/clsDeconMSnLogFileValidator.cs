using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;

namespace AnalysisManagerDtaRefineryPlugIn
{
    /// <summary>
    /// This class can be used to validate the data in a _DeconMSn_log.txt file
    /// It makes sure that the intensity values in the last two columns are all > 0
    /// Values that are 0 are auto-changed to 1
    /// </summary>
    /// <remarks></remarks>
    public class clsDeconMSnLogFileValidator
    {
        private string mErrorMessage = string.Empty;

        private bool mFileUpdated;

        /// <summary>
        /// Error message (if any)
        /// </summary>
        public string ErrorMessage
        {
            get { return mErrorMessage; }
        }

        /// <summary>
        /// Indicates whether the intensity values in the original file were updated
        /// </summary>
        /// <returns>True if the file was updated</returns>
        public bool FileUpdated
        {
            get { return mFileUpdated; }
        }

        private string CollapseLine(string[] strSplitLine)
        {
            StringBuilder sbCollapsed = new StringBuilder(1024);

            if (strSplitLine.Length > 0)
            {
                sbCollapsed.Append(strSplitLine[0]);
                for (var intIndex = 1; intIndex <= strSplitLine.Length - 1; intIndex++)
                {
                    sbCollapsed.Append("\t" + strSplitLine[intIndex]);
                }
            }

            return sbCollapsed.ToString();
        }

        /// <summary>
        /// Parse the specified DeconMSn log file to check for intensity values in the last two columns that are zero
        /// </summary>
        /// <param name="strSourceFilePath">Path to the file</param>
        /// <returns>True if success; false if an unrecoverable error</returns>
        public bool ValidateDeconMSnLogFile(string strSourceFilePath)
        {
            string strTempFilePath = null;

            string strLineIn = null;
            string[] strSplitLine = null;

            var blnHeaderPassed = false;
            bool blnColumnUpdated = false;

            var intParentIntensityColIndex = 9;
            var intMonoIntensityColIndex = 10;
            int intColumnCountUpdated = 0;

            try
            {
                mErrorMessage = string.Empty;
                mFileUpdated = false;

                strTempFilePath = Path.GetTempFileName();
                Thread.Sleep(250);

                using (var srSourceFile = new StreamReader(new FileStream(strSourceFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                using (var swOutFile = new StreamWriter(new FileStream(strTempFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {
                    while (!srSourceFile.EndOfStream)
                    {
                        strLineIn = srSourceFile.ReadLine();
                        intColumnCountUpdated = 0;

                        if (blnHeaderPassed)
                        {
                            strSplitLine = strLineIn.Split('\t');

                            if (strSplitLine.Length > 1 && strSplitLine[0] == "MSn_Scan")
                            {
                                // This is the header line
                                ValidateHeader(strLineIn, ref intParentIntensityColIndex, ref intMonoIntensityColIndex);
                            }
                            else if (strSplitLine.Length > 1)
                            {
                                ValidateColumnIsPositive(strSplitLine, intParentIntensityColIndex, out blnColumnUpdated);
                                if (blnColumnUpdated)
                                    intColumnCountUpdated += 1;

                                ValidateColumnIsPositive(strSplitLine, intMonoIntensityColIndex, out blnColumnUpdated);
                                if (blnColumnUpdated)
                                    intColumnCountUpdated += 1;
                            }

                            if (intColumnCountUpdated > 0)
                            {
                                mFileUpdated = true;
                                swOutFile.WriteLine(CollapseLine(strSplitLine));
                            }
                            else
                            {
                                swOutFile.WriteLine(strLineIn);
                            }
                        }
                        else
                        {
                            if (strLineIn.StartsWith("--------------"))
                            {
                                blnHeaderPassed = true;
                            }
                            else if (strLineIn.StartsWith("MSn_Scan"))
                            {
                                ValidateHeader(strLineIn, ref intParentIntensityColIndex, ref intMonoIntensityColIndex);
                                blnHeaderPassed = true;
                            }
                            swOutFile.WriteLine(strLineIn);
                        }
                    }
                }

                if (mFileUpdated)
                {
                    // First rename strFilePath
                    var ioFileInfo = new FileInfo(strSourceFilePath);
                    string strTargetFilePath = Path.Combine(ioFileInfo.DirectoryName,
                        Path.GetFileNameWithoutExtension(ioFileInfo.Name) + "_Original.txt");

                    if (File.Exists(strTargetFilePath))
                    {
                        try
                        {
                            File.Delete(strTargetFilePath);
                        }
                        catch (Exception ex)
                        {
                            mErrorMessage = "Error deleting old _Original.txt file: " + ex.Message;
                            Console.WriteLine(mErrorMessage);
                        }
                    }

                    try
                    {
                        ioFileInfo.MoveTo(strTargetFilePath);

                        // Now copy the temp file to strFilePath
                        File.Copy(strTempFilePath, strSourceFilePath, false);
                    }
                    catch (Exception ex)
                    {
                        mErrorMessage = "Error replacing source file with new file: " + ex.Message;
                        Console.WriteLine(mErrorMessage);

                        // Copy the temp file to strFilePath
                        File.Copy(strTempFilePath, Path.Combine(ioFileInfo.DirectoryName, Path.GetFileNameWithoutExtension(ioFileInfo.Name) + "_New.txt"), true);
                        File.Delete(strTempFilePath);

                        return false;
                    }
                }

                File.Delete(strTempFilePath);
            }
            catch (Exception ex)
            {
                mErrorMessage = "Exception in clsDeconMSnLogFileValidator.ValidateFile: " + ex.Message;
                Console.WriteLine(mErrorMessage);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Validate the header, updating the column indices if necessary
        /// </summary>
        /// <param name="strLineIn"></param>
        /// <param name="intParentIntensityColIndex">Input/output parameter</param>
        /// <param name="intMonoIntensityColIndex">Input/output parameter</param>
        /// <remarks></remarks>
        private void ValidateHeader(string strLineIn, ref int intParentIntensityColIndex, ref int intMonoIntensityColIndex)
        {
            string[] strSplitLine = null;
            int intColIndex = 0;

            strSplitLine = strLineIn.Split('\t');

            if (strSplitLine.Length > 1)
            {
                var lstSplitLine = new List<string>(strSplitLine);

                intColIndex = lstSplitLine.IndexOf("Parent_Intensity");
                if (intColIndex > 0)
                    intParentIntensityColIndex = intColIndex;

                intColIndex = lstSplitLine.IndexOf("Mono_Intensity");
                if (intColIndex > 0)
                    intMonoIntensityColIndex = intColIndex;
            }
        }

        private void ValidateColumnIsPositive(string[] strSplitLine, int intColIndex, out bool blnColumnUpdated)
        {
            double dblResult = 0;
            bool blnIsNumeric = false;

            blnColumnUpdated = false;

            if (strSplitLine.Length > intColIndex)
            {
                dblResult = 0;
                blnIsNumeric = double.TryParse(strSplitLine[intColIndex], out dblResult);
                if (!blnIsNumeric || dblResult < 1)
                {
                    strSplitLine[intColIndex] = "1";
                    blnColumnUpdated = true;
                }
            }
        }
    }
}
