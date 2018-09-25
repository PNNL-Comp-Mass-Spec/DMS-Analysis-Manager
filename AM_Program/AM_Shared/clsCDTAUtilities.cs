
using System;
using System.IO;
using PRISM;

namespace AnalysisManagerBase
{

    /// <summary>
    /// DTA utilities
    /// </summary>
    public class clsCDTAUtilities : EventNotifier
    {

        private CondenseCDTAFile.clsCDTAFileCondenser m_CDTACondenser;

        /// <summary>
        /// Convert a _dta.txt file to a .mgf file
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        public bool ConvertCDTAToMGF(
            FileInfo cdtaFile,
            string datasetName,
            bool combine2And3PlusCharges = false,
            int maximumIonsPer100MzInterval = 0,
            bool createIndexFile = true
            )
        {

            try
            {
                OnStatusEvent(string.Format("Converting {0} to a .mgf file", cdtaFile.Name));

                var dtaToMGF = new DTAtoMGF.clsDTAtoMGF
                {
                    Combine2And3PlusCharges = combine2And3PlusCharges,
                    FilterSpectra = false,
                    MaximumIonsPer100MzInterval = maximumIonsPer100MzInterval,
                    NoMerge = true,
                    CreateIndexFile = createIndexFile
                };

                if (cdtaFile.Directory == null)
                {
                    OnErrorEvent("Unable to determine the parent directory of " + cdtaFile.FullName);
                    return false;
                }

                var workDir = cdtaFile.Directory.FullName;

                if (!cdtaFile.Exists)
                {
                    OnErrorEvent("_dta.txt file not found; cannot convert to .mgf: " + cdtaFile.FullName);
                    return false;
                }

                if (!dtaToMGF.ProcessFile(cdtaFile.FullName))
                {
                    OnErrorEvent("Error converting " + cdtaFile.Name + " to a .mgf file: " + dtaToMGF.GetErrorMessage());
                    return false;
                }

                ProgRunner.GarbageCollectNow();

                var newMGFFile = new FileInfo(Path.Combine(workDir, datasetName + ".mgf"));

                if (!newMGFFile.Exists)
                {
                    // MGF file was not created
                    OnErrorEvent("A .mgf file was not created using the _dta.txt file: " + dtaToMGF.GetErrorMessage());
                    return false;
                }

            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception in ConvertCDTAToMGF: " + ex.Message, ex);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Removes any spectra with 2 or fewer ions in a _DTA.txt file
        /// </summary>
        /// <param name="workDir">Folder with the CDTA file</param>
        /// <param name="inputFileName">CDTA filename</param>
        /// <returns>True if success; false if an error</returns>
        public bool RemoveSparseSpectra(string workDir, string inputFileName)
        {

            const int MINIMUM_ION_COUNT = 3;

            var parentIonLineIsNext = false;

            var ionCount = 0;
            var spectraParsed = 0;
            var spectraCountRemoved = 0;

            var sbCurrentSpectrum = new System.Text.StringBuilder();


            try
            {
                var sourceFilePath = Path.Combine(workDir, inputFileName);

                if (string.IsNullOrEmpty(workDir))
                {
                    OnErrorEvent("Error in RemoveSparseSpectra: workDir is empty");
                    return false;
                }

                if (string.IsNullOrEmpty(inputFileName))
                {
                    OnErrorEvent("Error in RemoveSparseSpectra: inputFileName is empty");
                    return false;
                }

                var fiOriginalFile = new FileInfo(sourceFilePath);
                if (!fiOriginalFile.Exists)
                {
                    OnErrorEvent("Error in RemoveSparseSpectra: source file not found: " + sourceFilePath);
                    return false;
                }

                var fiUpdatedFile = new FileInfo(sourceFilePath + ".tmp");

                // Open the input file
                using (var srInFile = new StreamReader(new FileStream(fiOriginalFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {

                    // Create the output file
                    using (var swOutFile = new StreamWriter(new FileStream(fiUpdatedFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    {

                        while (!srInFile.EndOfStream)
                        {
                            var lineIn = srInFile.ReadLine();

                            if (string.IsNullOrEmpty(lineIn))
                            {
                                sbCurrentSpectrum.AppendLine();
                            }
                            else
                            {

                                if (lineIn.StartsWith("="))
                                {
                                    // DTA header line, for example:
                                    // =================================== "H20120523_JQ_CPTAC2_4TP_Exp1_IMAC_01.0002.0002.3.dta" ==================================

                                    if (sbCurrentSpectrum.Length > 0)
                                    {
                                        if (ionCount >= MINIMUM_ION_COUNT || spectraParsed == 0)
                                        {
                                            // Write the cached spectrum
                                            swOutFile.Write(sbCurrentSpectrum.ToString());
                                        }
                                        else
                                        {
                                            spectraCountRemoved += 1;
                                        }
                                        sbCurrentSpectrum.Clear();
                                        ionCount = 0;
                                    }

                                    parentIonLineIsNext = true;
                                    spectraParsed += 1;

                                }
                                else if (parentIonLineIsNext)
                                {
                                    // lineIn contains the parent ion line text

                                    parentIonLineIsNext = false;
                                }
                                else
                                {
                                    // Line is not a header or the parent ion line
                                    // Assume a data line
                                    ionCount += 1;
                                }

                                sbCurrentSpectrum.AppendLine(lineIn);

                            }
                        }

                        if (sbCurrentSpectrum.Length > 0)
                        {
                            if (ionCount >= MINIMUM_ION_COUNT)
                            {
                                // Write the cached spectrum
                                swOutFile.Write(sbCurrentSpectrum.ToString());
                            }
                            else
                            {
                                spectraCountRemoved += 1;
                            }
                        }

                    }
                }

                var spectraRemoved = false;
                const bool replaceSourceFile = true;
                const bool deleteSourceFileIfUpdated = true;

                if (spectraCountRemoved > 0)
                {
                    OnStatusEvent("Removed " + spectraCountRemoved + " spectra from " + inputFileName + " since fewer than " + MINIMUM_ION_COUNT + " ions");
                    spectraRemoved = true;
                }

                FinalizeCDTAValidation(spectraRemoved, replaceSourceFile, deleteSourceFileIfUpdated, fiOriginalFile, fiUpdatedFile);

            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception in RemoveSparseSpectra: " + ex.Message);
                return false;
            }

            return true;

        }

        /// <summary>
        /// Replaces the original file with a new CDTA file if newCDTAFileHasUpdates=True; deletes the new CDTA file if newCDTAFileHasUpdates=false
        /// </summary>
        /// <param name="newCDTAFileHasUpdates">True if the new CDTA file has updated info</param>
        /// <param name="replaceSourceFile">If True, replaces the source file with and updated file</param>
        /// <param name="deleteSourceFileIfUpdated">
        /// Only valid if replaceSourceFile=True;
        /// If True, the source file is deleted if an updated version is created.
        /// If false, the source file is renamed to .old if an updated version is created.
        /// </param>
        /// <param name="fiOriginalFile">File handle to the original CDTA file</param>
        /// <param name="fiUpdatedFile">File handle to the new CDTA file</param>
        /// <remarks></remarks>
        protected void FinalizeCDTAValidation(bool newCDTAFileHasUpdates, bool replaceSourceFile, bool deleteSourceFileIfUpdated,
                                              FileInfo fiOriginalFile, FileInfo fiUpdatedFile)
        {
            if (newCDTAFileHasUpdates)
            {
                var sourceFilePath = fiOriginalFile.FullName;

                if (!replaceSourceFile)
                {
                    // Directly wrote to the output file; nothing to rename
                    return;
                }

                // Replace the original file with the new one
                string oldFilePath;
                var addon = 0;

                do
                {
                    oldFilePath = fiOriginalFile.FullName + ".old";
                    if (addon > 0)
                    {
                        oldFilePath += addon.ToString();
                    }
                    addon += 1;
                } while (File.Exists(oldFilePath));

                fiOriginalFile.MoveTo(oldFilePath);

                fiUpdatedFile.MoveTo(sourceFilePath);

                if (deleteSourceFileIfUpdated)
                {
                    ProgRunner.GarbageCollectNow();
                    fiOriginalFile.Delete();
                }
            }
            else
            {
                // No changes were made; nothing to update
                // However, delete the new file we created
                ProgRunner.GarbageCollectNow();

                fiUpdatedFile.Delete();

            }

        }

        /// <summary>
        /// Makes sure the specified _DTA.txt file has scan=x and cs=y tags in the parent ion line
        /// </summary>
        /// <param name="sourceFilePath">Input _DTA.txt file to parse</param>
        /// <param name="replaceSourceFile">If True, replaces the source file with and updated file</param>
        /// <param name="deleteSourceFileIfUpdated">
        /// Only valid if replaceSourceFile=True;
        /// If True, the source file is deleted if an updated version is created.
        /// If false, the source file is renamed to .old if an updated version is created.
        /// </param>
        /// <param name="outputFilePath">
        /// Output file path to use for the updated file; required if replaceSourceFile=False; ignored if replaceSourceFile=True
        /// </param>
        /// <returns>True if success; false if an error</returns>
        public bool ValidateCDTAFileScanAndCSTags(string sourceFilePath, bool replaceSourceFile, bool deleteSourceFileIfUpdated,
                                                  string outputFilePath)
        {

            var parentIonLineIsNext = false;
            var parentIonLineUpdated = false;

            try
            {
                if (string.IsNullOrEmpty(sourceFilePath))
                {
                    OnErrorEvent("Error in ValidateCDTAFileScanAndCSTags: sourceFilePath is empty");
                    return false;
                }

                var fiOriginalFile = new FileInfo(sourceFilePath);
                if (!fiOriginalFile.Exists)
                {
                    OnErrorEvent("Error in ValidateCDTAFileScanAndCSTags: source file not found: " + sourceFilePath);
                    return false;
                }

                string outputFilePathTemp;
                if (replaceSourceFile)
                {
                    outputFilePathTemp = sourceFilePath + ".tmp";
                }
                else
                {
                    // outputFilePath must contain a valid file path
                    if (string.IsNullOrEmpty(outputFilePath))
                    {
                        OnErrorEvent(
                            "Error in ValidateCDTAFileScanAndCSTags: variable outputFilePath must define a file path when replaceSourceFile=False");
                        return false;
                    }
                    outputFilePathTemp = outputFilePath;
                }

                var fiUpdatedFile = new FileInfo(outputFilePathTemp);

                // We use the DtaTextFileReader to parse out the scan and charge from the header line
                var dtaTextReader = new MSDataFileReader.clsDtaTextFileReader(false);

                // Open the input file
                using (var srInFile = new StreamReader(new FileStream(fiOriginalFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {

                    // Create the output file
                    using (var swOutFile = new StreamWriter(new FileStream(fiUpdatedFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    {

                        while (!srInFile.EndOfStream)
                        {
                            var lineIn = srInFile.ReadLine();

                            if (string.IsNullOrEmpty(lineIn))
                            {
                                swOutFile.WriteLine();
                                continue;
                            }

                            var scanNumberStart = 0;
                            var charge = 0;

                            if (lineIn.StartsWith("="))
                            {
                                // Parse the DTA header line, for example:
                                // =================================== "H20120523_JQ_CPTAC2_4TP_Exp1_IMAC_01.0002.0002.3.dta" ==================================

                                // Remove the leading and trailing characters, then extract the scan and charge
                                var strDTAHeader = lineIn.Trim('=', ' ', '"');

                                dtaTextReader.ExtractScanInfoFromDtaHeader(strDTAHeader, out scanNumberStart, out _, out _, out charge);

                                parentIonLineIsNext = true;

                            }
                            else if (parentIonLineIsNext)
                            {
                                // lineIn contains the parent ion line text

                                // Construct the parent ion line to write out
                                // Will contain the MH+ value of the parent ion (thus always the 1+ mass, even if actually a different charge)
                                // Next contains the charge state, then scan= and cs= tags, for example:
                                // 447.34573 1   scan=3 cs=1

                                if (!lineIn.Contains("scan="))
                                {
                                    // Append scan=x to the parent ion line
                                    lineIn = lineIn.Trim() + "   scan=" + scanNumberStart;
                                    parentIonLineUpdated = true;
                                }

                                if (!lineIn.Contains("cs="))
                                {
                                    // Append cs=y to the parent ion line
                                    lineIn = lineIn.Trim() + " cs=" + charge;
                                    parentIonLineUpdated = true;
                                }

                                parentIonLineIsNext = false;

                            }

                            swOutFile.WriteLine(lineIn);
                        }

                    }
                }

                FinalizeCDTAValidation(parentIonLineUpdated, replaceSourceFile, deleteSourceFileIfUpdated, fiOriginalFile, fiUpdatedFile);

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
        /// <param name="workDir">Folder with the CDTA file</param>
        /// <param name="inputFileName">CDTA filename</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public bool ValidateCDTAFileSize(string workDir, string inputFileName)
        {
            const int FILE_SIZE_THRESHOLD = int.MaxValue;

            try
            {
                var inputFilePath = Path.Combine(workDir, inputFileName);
                var ioFileInfo = new FileInfo(inputFilePath);

                if (!ioFileInfo.Exists)
                {
                    OnErrorEvent("_DTA.txt file not found: " + inputFilePath);
                    return false;
                }

                if (ioFileInfo.Length < FILE_SIZE_THRESHOLD)
                    return true;

                // Need to condense the file
                var message = ioFileInfo.Name + " is " + clsGlobal.BytesToGB(ioFileInfo.Length).ToString("0.00") + " GB in size; " +
                                 "will now condense it by combining data points with consecutive zero-intensity values";

                OnStatusEvent(message);

                m_CDTACondenser = new CondenseCDTAFile.clsCDTAFileCondenser();
                m_CDTACondenser.ProgressChanged += m_CDTACondenser_ProgressChanged;

                var success = m_CDTACondenser.ProcessFile(ioFileInfo.FullName, ioFileInfo.DirectoryName);

                if (!success)
                {
                    OnErrorEvent("Error condensing _DTA.txt file: " + m_CDTACondenser.GetErrorMessage());
                    return false;
                }

                // Check the size of the new _dta.txt file
                ioFileInfo.Refresh();

                OnStatusEvent(
                    "Condensing complete; size of the new _dta.txt file is " +
                    clsGlobal.BytesToGB(ioFileInfo.Length).ToString("0.00") + " GB");

                try
                {
                    var filePathOld = Path.Combine(workDir, Path.GetFileNameWithoutExtension(ioFileInfo.FullName) + "_Old.txt");

                    OnStatusEvent("Now deleting file " + filePathOld);

                    ioFileInfo = new FileInfo(filePathOld);
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

    }

}