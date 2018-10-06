using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using PRISM;

namespace AnalysisManagerDtaRefineryPlugIn
{
    /// <summary>
    /// This class can be used to validate the data in a _DeconMSn_log.txt file
    /// It makes sure that the intensity values in the last two columns are all > 0
    /// Values that are 0 are auto-changed to 1
    /// </summary>
    /// <remarks></remarks>
    public class clsDeconMSnLogFileValidator : EventNotifier
    {
        private bool mFileUpdated;

        /// <summary>
        /// Indicates whether the intensity values in the original file were updated
        /// </summary>
        /// <returns>True if the file was updated</returns>
        public bool FileUpdated => mFileUpdated;

        private string CollapseLine(IReadOnlyList<string> dataColumns)
        {
            var sbCollapsed = new StringBuilder(1024);

            if (dataColumns.Count > 0)
            {
                sbCollapsed.Append(dataColumns[0]);
                for (var intIndex = 1; intIndex <= dataColumns.Count - 1; intIndex++)
                {
                    sbCollapsed.Append("\t" + dataColumns[intIndex]);
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
            var headerValidated = false;

            var parentIntensityColIndex = 9;
            var monoIntensityColIndex = 10;

            try
            {
                mFileUpdated = false;

                var tempFilePath = Path.GetTempFileName();

                using (var reader = new StreamReader(new FileStream(strSourceFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                using (var writer = new StreamWriter(new FileStream(tempFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        var columnCountUpdated = 0;
                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        if (headerValidated)
                        {
                            var dataColumns = dataLine.Split('\t');

                            if (dataColumns.Length > 1 && dataColumns[0] == "MSn_Scan")
                            {
                                // This is the header line
                                ValidateHeader(dataLine, ref parentIntensityColIndex, ref monoIntensityColIndex);
                            }
                            else if (dataColumns.Length > 1)
                            {
                                ValidateColumnIsPositive(dataColumns, parentIntensityColIndex, out var parentIntensityColumnUpdated);
                                if (parentIntensityColumnUpdated)
                                    columnCountUpdated += 1;

                                ValidateColumnIsPositive(dataColumns, monoIntensityColIndex, out var monoIntensityColumnUpdated);
                                if (monoIntensityColumnUpdated)
                                    columnCountUpdated += 1;
                            }

                            if (columnCountUpdated > 0)
                            {
                                mFileUpdated = true;
                                writer.WriteLine(CollapseLine(dataColumns));
                            }
                            else
                            {
                                writer.WriteLine(dataLine);
                            }
                        }
                        else
                        {
                            if (dataLine.StartsWith("--------------"))
                            {
                                headerValidated = true;
                            }
                            else if (dataLine.StartsWith("MSn_Scan", StringComparison.InvariantCultureIgnoreCase))
                            {
                                ValidateHeader(dataLine, ref parentIntensityColIndex, ref monoIntensityColIndex);
                                headerValidated = true;
                            }
                            writer.WriteLine(dataLine);
                        }
                    }
                }

                if (mFileUpdated)
                {
                    // First rename strFilePath
                    var ioFileInfo = new FileInfo(strSourceFilePath);

                    if (ioFileInfo.DirectoryName == null)
                    {
                        OnErrorEvent("Unable to determine the parent directory of " + strSourceFilePath);
                        return false;
                    }

                    var strTargetFilePath = Path.Combine(ioFileInfo.DirectoryName,
                        Path.GetFileNameWithoutExtension(ioFileInfo.Name) + "_Original.txt");

                    if (File.Exists(strTargetFilePath))
                    {
                        try
                        {
                            File.Delete(strTargetFilePath);
                        }
                        catch (Exception ex)
                        {
                            OnErrorEvent("Error deleting old _Original.txt file: " + ex.Message, ex);
                        }
                    }

                    try
                    {
                        ioFileInfo.MoveTo(strTargetFilePath);

                        // Now copy the temp file to strFilePath
                        File.Copy(tempFilePath, strSourceFilePath, false);
                    }
                    catch (Exception ex)
                    {
                        OnErrorEvent("Error replacing source file with new file: " + ex.Message, ex);

                        if (ioFileInfo.DirectoryName != null)
                        {
                            // Copy the temp file to strFilePath
                            File.Copy(tempFilePath, Path.Combine(ioFileInfo.DirectoryName,
                                Path.GetFileNameWithoutExtension(ioFileInfo.Name) + "_New.txt"), true);
                            File.Delete(tempFilePath);
                        }

                        return false;
                    }
                }

                File.Delete(tempFilePath);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception in clsDeconMSnLogFileValidator.ValidateFile: " + ex.Message, ex);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Validate the header, updating the column indices if necessary
        /// </summary>
        /// <param name="dataLine"></param>
        /// <param name="parentIntensityColIndex">Input/output parameter</param>
        /// <param name="monoIntensityColIndex">Input/output parameter</param>
        /// <remarks></remarks>
        private void ValidateHeader(string dataLine, ref int parentIntensityColIndex, ref int monoIntensityColIndex)
        {
            var dataColumns = dataLine.Split('\t');

            if (dataColumns.Length > 1)
            {
                var lstSplitLine = new List<string>(dataColumns);

                var colIndex = lstSplitLine.IndexOf("Parent_Intensity");
                if (colIndex > 0)
                    parentIntensityColIndex = colIndex;

                colIndex = lstSplitLine.IndexOf("Mono_Intensity");
                if (colIndex > 0)
                    monoIntensityColIndex = colIndex;
            }
        }

        /// <summary>
        /// Examines the data in dataColumns[colIndex] to see if it is numeric and 1 or grater
        /// If not numeric, or if less than 1, it is changed to be 1
        /// </summary>
        /// <param name="dataColumns"></param>
        /// <param name="colIndex"></param>
        /// <param name="columnUpdated"></param>
        private void ValidateColumnIsPositive(IList<string> dataColumns, int colIndex, out bool columnUpdated)
        {
            columnUpdated = false;

            if (dataColumns.Count <= colIndex)
                return;

            var isNumeric = double.TryParse(dataColumns[colIndex], out var result);
            if (!isNumeric || result < 1)
            {
                dataColumns[colIndex] = "1";
                columnUpdated = true;
            }
        }
    }
}
