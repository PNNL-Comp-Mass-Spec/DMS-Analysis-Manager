
using System;
using System.IO;
using System.Threading;

namespace AnalysisManagerBase
{

    public class clsCDTAUtilities : clsEventNotifier
    {

        private CondenseCDTAFile.clsCDTAFileCondenser m_CDTACondenser;

        /// <summary>
        /// Removes any spectra with 2 or fewer ions in a _DTA.txt ifle
        /// </summary>
        /// <param name="strWorkDir">Folder with the CDTA file</param>
        /// <param name="strInputFileName">CDTA filename</param>
        /// <returns>True if success; false if an error</returns>
        public bool RemoveSparseSpectra(string strWorkDir, string strInputFileName)
        {

            const int MINIMUM_ION_COUNT = 3;

            var blnParentIonLineIsNext = false;

            var intIonCount = 0;
            var intSpectraParsed = 0;
            var intSpectraRemoved = 0;

            var sbCurrentSpectrum = new System.Text.StringBuilder();


            try
            {
                var strSourceFilePath = Path.Combine(strWorkDir, strInputFileName);

                if (string.IsNullOrEmpty(strWorkDir))
                {
                    OnErrorEvent("Error in RemoveSparseSpectra: strWorkDir is empty");
                    return false;
                }

                if (string.IsNullOrEmpty(strInputFileName))
                {
                    OnErrorEvent("Error in RemoveSparseSpectra: strInputFileName is empty");
                    return false;
                }

                var fiOriginalFile = new FileInfo(strSourceFilePath);
                if (!fiOriginalFile.Exists)
                {
                    OnErrorEvent("Error in RemoveSparseSpectra: source file not found: " + strSourceFilePath);
                    return false;
                }

                var fiUpdatedFile = new FileInfo(strSourceFilePath + ".tmp");

                // Open the input file
                using (var srInFile = new StreamReader(new FileStream(fiOriginalFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {

                    // Create the output file
                    using (var swOutFile = new StreamWriter(new FileStream(fiUpdatedFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    {

                        while (!srInFile.EndOfStream)
                        {
                            var strLineIn = srInFile.ReadLine();

                            if (string.IsNullOrEmpty(strLineIn))
                            {
                                sbCurrentSpectrum.AppendLine();
                            }
                            else
                            {

                                if (strLineIn.StartsWith("="))
                                {
                                    // DTA header line, for example:
                                    // =================================== "H20120523_JQ_CPTAC2_4TP_Exp1_IMAC_01.0002.0002.3.dta" ==================================

                                    if (sbCurrentSpectrum.Length > 0)
                                    {
                                        if (intIonCount >= MINIMUM_ION_COUNT || intSpectraParsed == 0)
                                        {
                                            // Write the cached spectrum
                                            swOutFile.Write(sbCurrentSpectrum.ToString());
                                        }
                                        else
                                        {
                                            intSpectraRemoved += 1;
                                        }
                                        sbCurrentSpectrum.Clear();
                                        intIonCount = 0;
                                    }

                                    blnParentIonLineIsNext = true;
                                    intSpectraParsed += 1;

                                }
                                else if (blnParentIonLineIsNext)
                                {
                                    // strLineIn contains the parent ion line text

                                    blnParentIonLineIsNext = false;
                                }
                                else
                                {
                                    // Line is not a header or the parent ion line
                                    // Assume a data line
                                    intIonCount += 1;
                                }

                                sbCurrentSpectrum.AppendLine(strLineIn);

                            }
                        }

                        if (sbCurrentSpectrum.Length > 0)
                        {
                            if (intIonCount >= MINIMUM_ION_COUNT)
                            {
                                // Write the cached spectrum
                                swOutFile.Write(sbCurrentSpectrum.ToString());
                            }
                            else
                            {
                                intSpectraRemoved += 1;
                            }
                        }

                    }
                }

                var blnSpectraRemoved = false;
                const bool blnReplaceSourceFile = true;
                const bool blnDeleteSourceFileIfUpdated = true;

                if (intSpectraRemoved > 0)
                {
                    OnStatusEvent("Removed " + intSpectraRemoved + " spectra from " + strInputFileName + " since fewer than " + MINIMUM_ION_COUNT + " ions");
                    blnSpectraRemoved = true;
                }

                FinalizeCDTAValidation(blnSpectraRemoved, blnReplaceSourceFile, blnDeleteSourceFileIfUpdated, fiOriginalFile, fiUpdatedFile);

            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception in RemoveSparseSpectra: " + ex.Message);
                return false;
            }

            return true;

        }

        /// <summary>
        /// Replaces the original file with a new CDTA file if blnNewCDTAFileHasUpdates=True; deletes the new CDTA file if blnNewCDTAFileHasUpdates=false
        /// </summary>
        /// <param name="blnNewCDTAFileHasUpdates">True if the new CDTA file has updated info</param>
        /// <param name="blnReplaceSourceFile">If True, then replaces the source file with and updated file</param>
        /// <param name="blnDeleteSourceFileIfUpdated">Only valid if blnReplaceSourceFile=True: If True, then the source file is deleted if an updated version is created. If false, then the source file is renamed to .old if an updated version is created.</param>
        /// <param name="fiOriginalFile">File handle to the original CDTA file</param>
        /// <param name="fiUpdatedFile">File handle to the new CDTA file</param>
        /// <remarks></remarks>

        protected void FinalizeCDTAValidation(bool blnNewCDTAFileHasUpdates, bool blnReplaceSourceFile, bool blnDeleteSourceFileIfUpdated,
                                              FileInfo fiOriginalFile, FileInfo fiUpdatedFile)
        {
            if (blnNewCDTAFileHasUpdates)
            {
                Thread.Sleep(100);

                var strSourceFilePath = fiOriginalFile.FullName;

                if (blnReplaceSourceFile)
                {
                    // Replace the original file with the new one
                    string strOldFilePath;
                    var intAddon = 0;

                    do
                    {
                        strOldFilePath = fiOriginalFile.FullName + ".old";
                        if (intAddon > 0)
                        {
                            strOldFilePath += intAddon.ToString();
                        }
                        intAddon += 1;
                    } while (File.Exists(strOldFilePath));

                    fiOriginalFile.MoveTo(strOldFilePath);
                    Thread.Sleep(100);

                    fiUpdatedFile.MoveTo(strSourceFilePath);

                    if (blnDeleteSourceFileIfUpdated)
                    {
                        Thread.Sleep(125);
                        PRISM.Processes.clsProgRunner.GarbageCollectNow();

                        fiOriginalFile.Delete();
                    }


                }
                else
                {
                    // Directly wrote to the output file; nothing to rename
                }
            }
            else
            {
                // No changes were made; nothing to update
                // However, delete the new file we created
                Thread.Sleep(125);
                PRISM.Processes.clsProgRunner.GarbageCollectNow();

                fiUpdatedFile.Delete();

            }

        }

        /// <summary>
        /// Makes sure the specified _DTA.txt file has scan=x and cs=y tags in the parent ion line
        /// </summary>
        /// <param name="strSourceFilePath">Input _DTA.txt file to parse</param>
        /// <param name="blnReplaceSourceFile">If True, then replaces the source file with and updated file</param>
        /// <param name="blnDeleteSourceFileIfUpdated">Only valid if blnReplaceSourceFile=True: If True, then the source file is deleted if an updated version is created. If false, then the source file is renamed to .old if an updated version is created.</param>
        /// <param name="strOutputFilePath">Output file path to use for the updated file; required if blnReplaceSourceFile=False; ignored if blnReplaceSourceFile=True</param>
        /// <returns>True if success; false if an error</returns>
        public bool ValidateCDTAFileScanAndCSTags(string strSourceFilePath, bool blnReplaceSourceFile, bool blnDeleteSourceFileIfUpdated,
                                                  string strOutputFilePath)
        {

          
            var blnParentIonLineIsNext = false;
            var blnParentIonLineUpdated = false;

            try
            {
                if (string.IsNullOrEmpty(strSourceFilePath))
                {
                    OnErrorEvent("Error in ValidateCDTAFileScanAndCSTags: strSourceFilePath is empty");
                    return false;
                }

                var fiOriginalFile = new FileInfo(strSourceFilePath);
                if (!fiOriginalFile.Exists)
                {
                    OnErrorEvent("Error in ValidateCDTAFileScanAndCSTags: source file not found: " + strSourceFilePath);
                    return false;
                }

                string strOutputFilePathTemp;
                if (blnReplaceSourceFile)
                {
                    strOutputFilePathTemp = strSourceFilePath + ".tmp";
                }
                else
                {
                    // strOutputFilePath must contain a valid file path
                    if (string.IsNullOrEmpty(strOutputFilePath))
                    {
                        OnErrorEvent(
                            "Error in ValidateCDTAFileScanAndCSTags: variable strOutputFilePath must define a file path when blnReplaceSourceFile=False");
                        return false;
                    }
                    strOutputFilePathTemp = strOutputFilePath;
                }

                var fiUpdatedFile = new FileInfo(strOutputFilePathTemp);

                // We use the DtaTextFileReader to parse out the scan and charge from the header line
                var objReader = new MSDataFileReader.clsDtaTextFileReader(false);

                // Open the input file
                using (var srInFile = new StreamReader(new FileStream(fiOriginalFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {

                    // Create the output file
                    using (var swOutFile = new StreamWriter(new FileStream(fiUpdatedFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    {

                        while (!srInFile.EndOfStream)
                        {
                            var strLineIn = srInFile.ReadLine();

                            if (string.IsNullOrEmpty(strLineIn))
                            {
                                swOutFile.WriteLine();
                            }
                            else
                            {
                                var intScanNumberStart = 0;
                                var intCharge = 0;

                                if (strLineIn.StartsWith("="))
                                {
                                    // Parse the DTA header line, for example:
                                    // =================================== "H20120523_JQ_CPTAC2_4TP_Exp1_IMAC_01.0002.0002.3.dta" ==================================

                                    // Remove the leading and trailing characters, then extract the scan and charge
                                    var strDTAHeader = strLineIn.Trim('=', ' ', '"');

                                    int intScanNumberEnd;
                                    int intScanCount;

                                    objReader.ExtractScanInfoFromDtaHeader(strDTAHeader, out intScanNumberStart, out intScanNumberEnd, out intScanCount, out intCharge);

                                    blnParentIonLineIsNext = true;

                                }
                                else if (blnParentIonLineIsNext)
                                {
                                    // strLineIn contains the parent ion line text

                                    // Construct the parent ion line to write out
                                    // Will contain the MH+ value of the parent ion (thus always the 1+ mass, even if actually a different charge)
                                    // Next contains the charge state, then scan= and cs= tags, for example:
                                    // 447.34573 1   scan=3 cs=1

                                    if (!strLineIn.Contains("scan="))
                                    {
                                        // Append scan=x to the parent ion line
                                        strLineIn = strLineIn.Trim() + "   scan=" + intScanNumberStart;
                                        blnParentIonLineUpdated = true;
                                    }

                                    if (!strLineIn.Contains("cs="))
                                    {
                                        // Append cs=y to the parent ion line
                                        strLineIn = strLineIn.Trim() + " cs=" + intCharge;
                                        blnParentIonLineUpdated = true;
                                    }

                                    blnParentIonLineIsNext = false;

                                }

                                swOutFile.WriteLine(strLineIn);

                            }
                        }

                    }
                }

                FinalizeCDTAValidation(blnParentIonLineUpdated, blnReplaceSourceFile, blnDeleteSourceFileIfUpdated, fiOriginalFile, fiUpdatedFile);

                return true;

            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception in ValidateCDTAFileScanAndCSTags: " + ex.Message);
                return false;
            }


        }

        /// <summary>
        /// Condenses CDTA files that are over 2 GB in size
        /// </summary>
        /// <param name="strWorkDir">Folder with the CDTA file</param>
        /// <param name="strInputFileName">CDTA filename</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public bool ValidateCDTAFileSize(string strWorkDir, string strInputFileName)
        {
            const int FILE_SIZE_THRESHOLD = Int32.MaxValue;

            try
            {
                var strInputFilePath = Path.Combine(strWorkDir, strInputFileName);
                var ioFileInfo = new FileInfo(strInputFilePath);

                if (!ioFileInfo.Exists)
                {
                    OnErrorEvent("_DTA.txt file not found: " + strInputFilePath);
                    return false;
                }

                if (ioFileInfo.Length < FILE_SIZE_THRESHOLD)
                    return true;

                // Need to condense the file
                var strMessage = ioFileInfo.Name + " is " + clsGlobal.BytesToGB(ioFileInfo.Length).ToString("0.00") + " GB in size; " +
                                 "will now condense it by combining data points with consecutive zero-intensity values";

                OnStatusEvent(strMessage);

                m_CDTACondenser = new CondenseCDTAFile.clsCDTAFileCondenser();
                m_CDTACondenser.ProgressChanged += m_CDTACondenser_ProgressChanged;

                var success = m_CDTACondenser.ProcessFile(ioFileInfo.FullName, ioFileInfo.DirectoryName);

                if (!success)
                {
                    OnErrorEvent("Error condensing _DTA.txt file: " + m_CDTACondenser.GetErrorMessage());
                    return false;
                }
                
                // Wait 500 msec, then check the size of the new _dta.txt file
                Thread.Sleep(500);

                ioFileInfo.Refresh();

                OnStatusEvent(
                    "Condensing complete; size of the new _dta.txt file is " + 
                    clsGlobal.BytesToGB(ioFileInfo.Length).ToString("0.00") + " GB");

                try
                {
                    var strFilePathOld = Path.Combine(strWorkDir, Path.GetFileNameWithoutExtension(ioFileInfo.FullName) + "_Old.txt");

                    OnStatusEvent("Now deleting file " + strFilePathOld);

                    ioFileInfo = new FileInfo(strFilePathOld);
                    if (ioFileInfo.Exists)
                    {
                        ioFileInfo.Delete();
                    }
                    else
                    {
                        OnErrorEvent("Old _DTA.txt file not found:" + ioFileInfo.FullName + "; cannot delete");
                    }

                }
                catch (Exception ex)
                {
                    // Error deleting the file; log it but keep processing
                    OnWarningEvent("Exception deleting _dta_old.txt file: " + ex.Message);
                }

                return true;

            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception in ValidateCDTAFileSize: " + ex.Message);
                return false;
            }

        }

        #region "Event Handlers"


        private void m_CDTACondenser_ProgressChanged(string taskDescription, float percentComplete)
        {
            OnProgressUpdate(taskDescription, percentComplete);
        }

        #endregion

        #region "Events"
        /*
        public event ErrorEventEventHandler ErrorEvent;

        public delegate void ErrorEventEventHandler(string ErrorMessage);

        public event InfoEventEventHandler InfoEvent;

        public delegate void InfoEventEventHandler(string Message, int DebugLevel);

        public event ProgressEventEventHandler ProgressEvent;

        public delegate void ProgressEventEventHandler(string taskDescription, float PercentComplete);

        public event WarningEventEventHandler WarningEvent;

        public delegate void WarningEventEventHandler(string Message);

        protected void ReportError(string strErrorMessage)
        {
            ErrorEvent?.Invoke(strErrorMessage);
        }

        protected void ReportInfo(string strMessage, int intDebugLevel)
        {
            InfoEvent?.Invoke(strMessage, intDebugLevel);
        }

        protected void ReportProgress(string taskDescription, float PercentComplete)
        {
            ProgressEvent?.Invoke(taskDescription, PercentComplete);
        }

        protected void ReportWarning(string strMessage)
        {
            WarningEvent?.Invoke(strMessage);
        }
        */

        #endregion

    }

}