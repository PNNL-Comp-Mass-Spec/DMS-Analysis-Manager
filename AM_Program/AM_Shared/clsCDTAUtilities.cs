
using PRISM;
using System;
using System.IO;

namespace AnalysisManagerBase
{
    /// <summary>
    /// DTA utilities
    /// </summary>
    public class clsCDTAUtilities : EventNotifier
    {
        // Ignore Spelling: dta

        private CondenseCDTAFile.clsCDTAFileCondenser mCDTACondenser;

        /// <summary>
        /// Convert a _dta.txt file to a .mgf file
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        // ReSharper disable once UnusedMember.Global
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

                var originalFile = new FileInfo(sourceFilePath);
                if (!originalFile.Exists)
                {
                    OnErrorEvent("Error in RemoveSparseSpectra: source file not found: " + sourceFilePath);
                    return false;
                }

                var updatedFile = new FileInfo(sourceFilePath + ".tmp");

                // Open the input file
                using (var reader = new StreamReader(new FileStream(originalFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    // Create the output file
                    using var writer = new StreamWriter(new FileStream(updatedFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read));

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrEmpty(dataLine))
                        {
                            sbCurrentSpectrum.AppendLine();
                        }
                        else
                        {
                            if (dataLine.StartsWith("="))
                            {
                                // DTA header line, for example:
                                // =================================== "H20120523_JQ_CPTAC2_4TP_Exp1_IMAC_01.0002.0002.3.dta" ==================================

                                if (sbCurrentSpectrum.Length > 0)
                                {
                                    if (ionCount >= MINIMUM_ION_COUNT || spectraParsed == 0)
                                    {
                                        // Write the cached spectrum
                                        writer.Write(sbCurrentSpectrum.ToString());
                                    }
                                    else
                                    {
                                        spectraCountRemoved++;
                                    }
                                    sbCurrentSpectrum.Clear();
                                    ionCount = 0;
                                }

                                parentIonLineIsNext = true;
                                spectraParsed++;
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
                                ionCount++;
                            }

                            sbCurrentSpectrum.AppendLine(dataLine);
                        }
                    }

                    if (sbCurrentSpectrum.Length > 0)
                    {
                        if (ionCount >= MINIMUM_ION_COUNT)
                        {
                            // Write the cached spectrum
                            writer.Write(sbCurrentSpectrum.ToString());
                        }
                        else
                        {
                            spectraCountRemoved++;
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

                FinalizeCDTAValidation(spectraRemoved, replaceSourceFile, deleteSourceFileIfUpdated, originalFile, updatedFile);
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
        /// <param name="originalFile">File handle to the original CDTA file</param>
        /// <param name="updatedFile">File handle to the new CDTA file</param>
        protected void FinalizeCDTAValidation(bool newCDTAFileHasUpdates, bool replaceSourceFile, bool deleteSourceFileIfUpdated,
                                              FileInfo originalFile, FileInfo updatedFile)
        {
            if (newCDTAFileHasUpdates)
            {
                var sourceFilePath = originalFile.FullName;

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
                    oldFilePath = originalFile.FullName + ".old";
                    if (addon > 0)
                    {
                        oldFilePath += addon.ToString();
                    }
                    addon++;
                } while (File.Exists(oldFilePath));

                originalFile.MoveTo(oldFilePath);

                updatedFile.MoveTo(sourceFilePath);

                if (deleteSourceFileIfUpdated)
                {
                    ProgRunner.GarbageCollectNow();
                    originalFile.Delete();
                }
            }
            else
            {
                // No changes were made; nothing to update
                // However, delete the new file we created
                ProgRunner.GarbageCollectNow();

                updatedFile.Delete();
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

                var originalFile = new FileInfo(sourceFilePath);
                if (!originalFile.Exists)
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

                var updatedFile = new FileInfo(outputFilePathTemp);

                // We use the DtaTextFileReader to parse out the scan and charge from the header line
                var dtaTextReader = new MSDataFileReader.clsDtaTextFileReader(false);

                // Open the input file
                using (var reader = new StreamReader(new FileStream(originalFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    // Create the output file
                    using var writer = new StreamWriter(new FileStream(updatedFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read));

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrEmpty(dataLine))
                        {
                            writer.WriteLine();
                            continue;
                        }

                        var scanNumberStart = 0;
                        var charge = 0;

                        if (dataLine.StartsWith("="))
                        {
                            // Parse the DTA header line, for example:
                            // =================================== "H20120523_JQ_CPTAC2_4TP_Exp1_IMAC_01.0002.0002.3.dta" ==================================

                            // Remove the leading and trailing characters, then extract the scan and charge
                            var dtaHeader = dataLine.Trim('=', ' ', '"');

                            dtaTextReader.ExtractScanInfoFromDtaHeader(dtaHeader, out scanNumberStart, out _, out _, out charge);

                            parentIonLineIsNext = true;
                        }
                        else if (parentIonLineIsNext)
                        {
                            // lineIn contains the parent ion line text

                            // Construct the parent ion line to write out
                            // Will contain the MH+ value of the parent ion (thus always the 1+ mass, even if actually a different charge)
                            // Next contains the charge state, then scan= and cs= tags, for example:
                            // 447.34573 1   scan=3 cs=1

                            if (!dataLine.Contains("scan="))
                            {
                                // Append scan=x to the parent ion line
                                dataLine = dataLine.Trim() + "   scan=" + scanNumberStart;
                                parentIonLineUpdated = true;
                            }

                            if (!dataLine.Contains("cs="))
                            {
                                // Append cs=y to the parent ion line
                                dataLine = dataLine.Trim() + " cs=" + charge;
                                parentIonLineUpdated = true;
                            }

                            parentIonLineIsNext = false;
                        }

                        writer.WriteLine(dataLine);
                    }
                }

                FinalizeCDTAValidation(parentIonLineUpdated, replaceSourceFile, deleteSourceFileIfUpdated, originalFile, updatedFile);

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
        public bool ValidateCDTAFileSize(string workDir, string inputFileName)
        {
            const int FILE_SIZE_THRESHOLD = int.MaxValue;

            try
            {
                var inputFilePath = Path.Combine(workDir, inputFileName);
                var cdtaFile = new FileInfo(inputFilePath);

                if (!cdtaFile.Exists)
                {
                    OnErrorEvent("_DTA.txt file not found: " + inputFilePath);
                    return false;
                }

                if (cdtaFile.Length < FILE_SIZE_THRESHOLD)
                    return true;

                // Need to condense the file
                var message = cdtaFile.Name + " is " + clsGlobal.BytesToGB(cdtaFile.Length).ToString("0.00") + " GB in size; " +
                                 "will now condense it by combining data points with consecutive zero-intensity values";

                OnStatusEvent(message);

                mCDTACondenser = new CondenseCDTAFile.clsCDTAFileCondenser();
                mCDTACondenser.ProgressChanged += CDTACondenser_ProgressChanged;

                var success = mCDTACondenser.ProcessFile(cdtaFile.FullName, cdtaFile.DirectoryName);

                if (!success)
                {
                    OnErrorEvent("Error condensing _DTA.txt file: " + mCDTACondenser.GetErrorMessage());
                    return false;
                }

                // Check the size of the new _dta.txt file
                cdtaFile.Refresh();

                OnStatusEvent(
                    "Condensing complete; size of the new _dta.txt file is " +
                    clsGlobal.BytesToGB(cdtaFile.Length).ToString("0.00") + " GB");

                try
                {
                    var filePathOld = Path.Combine(workDir, Path.GetFileNameWithoutExtension(cdtaFile.FullName) + "_Old.txt");

                    OnStatusEvent("Now deleting file " + filePathOld);

                    cdtaFile = new FileInfo(filePathOld);
                    if (cdtaFile.Exists)
                    {
                        cdtaFile.Delete();
                    }
                    else
                    {
                        OnErrorEvent("Old _DTA.txt file not found:" + cdtaFile.FullName + "; cannot delete");
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

        private void CDTACondenser_ProgressChanged(string taskDescription, float percentComplete)
        {
            OnProgressUpdate(taskDescription, percentComplete);
        }

        #endregion

    }
}