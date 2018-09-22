using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using PHRPReader;
using PRISM;

namespace AnalysisManagerExtractionPlugin
{
    public class clsPHRPMassErrorValidator : EventNotifier
    {
        #region "Module variables"

        private readonly int mDebugLevel;

        // This is a value between 0 and 100
        private const double mErrorThresholdPercent = 5;

        #endregion

        public string ErrorMessage { get; private set; } = string.Empty;

        /// <summary>
        /// Value between 0 and 100
        /// If more than this percent of the data has a mass error larger than the threshold, ValidatePHRPResultMassErrors returns false
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public double ErrorThresholdPercent => mErrorThresholdPercent;

        public clsPHRPMassErrorValidator(int intDebugLevel)
        {
            mDebugLevel = intDebugLevel;
        }

        private void InformLargeErrorExample(KeyValuePair<double, string> massErrorEntry)
        {
            OnErrorEvent("  ... large error example: " + massErrorEntry.Key + " Da for " + massErrorEntry.Value);
        }

        private clsSearchEngineParameters LoadSearchEngineParameters(clsPHRPReader phrpReader, string searchEngineParamFilePath, clsPHRPReader.ePeptideHitResultType eResultType)
        {
            clsSearchEngineParameters searchEngineParams = null;

            try
            {
                if (string.IsNullOrEmpty(searchEngineParamFilePath))
                {
                    OnWarningEvent("Search engine parameter file not defined; will assume a maximum tolerance of 10 Da");
                    searchEngineParams = new clsSearchEngineParameters(eResultType.ToString());
                    searchEngineParams.AddUpdateParameter("peptide_mass_tol", "10");
                }
                else
                {
                    var success = phrpReader.PHRPParser.LoadSearchEngineParameters(searchEngineParamFilePath, out searchEngineParams);

                    if (!success)
                    {
                        OnWarningEvent("Error loading search engine parameter file " + Path.GetFileName(searchEngineParamFilePath) +
                                           "; will assume a maximum tolerance of 10 Da");
                        searchEngineParams = new clsSearchEngineParameters(eResultType.ToString());
                        searchEngineParams.AddUpdateParameter("peptide_mass_tol", "10");
                    }
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in LoadSearchEngineParameters", ex);
            }

            return searchEngineParams;
        }

        /// <summary>
        /// Parses inputFilePath to count the number of entries where the difference in mass
        /// between the precursor neutral mass value and the computed monoisotopic mass value
        /// is more than 6 Da away (more for higher charge states)
        /// </summary>
        /// <param name="inputFilePath"></param>
        /// <param name="eResultType"></param>
        /// <param name="searchEngineParamFilePath"></param>
        /// <returns>True if less than mErrorThresholdPercent of the data is bad; False otherwise</returns>
        /// <remarks></remarks>
        public bool ValidatePHRPResultMassErrors(string inputFilePath, clsPHRPReader.ePeptideHitResultType eResultType, string searchEngineParamFilePath)
        {
            try
            {
                ErrorMessage = string.Empty;

                var oPeptideMassCalculator = new clsPeptideMassCalculator();

                var oStartupOptions = new clsPHRPStartupOptions
                {
                    LoadModsAndSeqInfo = true,
                    LoadMSGFResults = false,
                    LoadScanStatsData = false,
                    MaxProteinsPerPSM = 1,
                    PeptideMassCalculator = oPeptideMassCalculator
                };

                var intPsmCount = 0;
                var intErrorCount = 0;
                double precursorMassTolerance;

                var lstLargestMassErrors = new SortedDictionary<double, string>();

                using (var reader = new clsPHRPReader(inputFilePath, eResultType, oStartupOptions))
                {
                    RegisterEvents(reader);

                    // Progress is reported below via a manual call to OnDebugEvent
                    reader.ProgressUpdate -= OnProgressUpdate;
                    reader.SkipConsoleWriteIfNoProgressListener = true;

                    // Report any errors cached during instantiation of mPHRPReader
                    foreach (var message in reader.ErrorMessages)
                    {
                        if (string.IsNullOrEmpty(ErrorMessage))
                        {
                            ErrorMessage = string.Copy(message);
                        }
                        OnErrorEvent(message);
                    }
                    if (reader.ErrorMessages.Count > 0)
                        return false;

                    // Report any warnings cached during instantiation of mPHRPReader
                    foreach (var message in reader.WarningMessages)
                    {
                        if (message.StartsWith("Warning, taxonomy file not found"))
                        {
                            // Ignore this warning; the taxonomy file would have been used to determine the fasta file that was searched
                            // We don't need that information in this application
                        }
                        else
                        {
                            OnWarningEvent(message);
                        }
                    }

                    reader.ClearErrors();
                    reader.ClearWarnings();
                    reader.SkipDuplicatePSMs = true;

                    // Load the search engine parameters
                    var searchEngineParams = LoadSearchEngineParameters(reader, searchEngineParamFilePath, eResultType);

                    // Check for a custom charge carrier mass
                    if (clsPHRPParserMSGFDB.GetCustomChargeCarrierMass(searchEngineParams, out var customChargeCarrierMass))
                    {
                        if (mDebugLevel >= 2)
                        {
                            OnDebugEvent(string.Format("Custom charge carrier mass defined: {0:F3} Da", customChargeCarrierMass));
                        }
                        oPeptideMassCalculator.ChargeCarrierMass = customChargeCarrierMass;
                    }

                    // Define the precursor mass tolerance threshold
                    // At a minimum, use 6 Da, though we'll bump that up by 1 Da for each charge state (7 Da for CS 2, 8 Da for CS 3, 9 Da for CS 4, etc.)
                    // However, for MSGF+ we require that the masses match within 0.1 Da because the IsotopeError column allows for a more accurate comparison

                    if (searchEngineParams.PrecursorMassToleranceDa < 6)
                    {
                        precursorMassTolerance = 6;
                    }
                    else
                    {
                        precursorMassTolerance = searchEngineParams.PrecursorMassToleranceDa;
                    }

                    var highResMS1 = searchEngineParams.PrecursorMassToleranceDa < 0.75;

                    if (mDebugLevel >= 2)
                    {
                        OnDebugEvent(string.Format("Will use mass tolerance of {0:0.0} Da when determining PHRP mass errors", precursorMassTolerance));
                    }

                    // Count the number of PSMs with a mass error greater than precursorMassTolerance

                    var dtLastProgress = DateTime.UtcNow;

                    while (reader.MoveNext())
                    {

                        intPsmCount += 1;

                        if (intPsmCount % 100 == 0 && DateTime.UtcNow.Subtract(dtLastProgress).TotalSeconds >= 15)
                        {
                            dtLastProgress = DateTime.UtcNow;
                            OnDebugEvent("Validating mass errors: " + reader.PercentComplete.ToString("0.0") + "% complete");
                        }

                        var currentPSM = reader.CurrentPSM;

                        if (currentPSM.PeptideMonoisotopicMass <= 0)
                        {
                            continue;
                        }

                        // PrecursorNeutralMass is based on the mass value reported by the search engine
                        //   (will be reported mono mass or could be m/z or MH converted to neutral mass)
                        // PeptideMonoisotopicMass is the mass value computed by PHRP based on .PrecursorNeutralMass plus any modification masses associated with residues
                        var massError = currentPSM.PrecursorNeutralMass - currentPSM.PeptideMonoisotopicMass;
                        double toleranceCurrent;

                        if (eResultType == clsPHRPReader.ePeptideHitResultType.MSGFDB &&
                            highResMS1 &&
                            currentPSM.TryGetScore("IsotopeError", out var psmIsotopeError))
                        {
                            // The integer value of massError should match psmIsotopeError
                            // However, scale up the tolerance based on the peptide mass
                            toleranceCurrent = 0.2 + currentPSM.PeptideMonoisotopicMass / 50000.0;
                            massError -= Convert.ToInt32(psmIsotopeError);
                        }
                        else
                        {
                            toleranceCurrent = precursorMassTolerance + currentPSM.Charge - 1;
                        }

                        if (Math.Abs(massError) <= toleranceCurrent)
                        {
                            continue;
                        }

                        var peptideDescription = "Scan=" + currentPSM.ScanNumberStart + ", charge=" + currentPSM.Charge + ", peptide=" +
                                                 currentPSM.PeptideWithNumericMods;
                        intErrorCount += 1;

                        // Keep track of the 100 largest mass errors
                        if (lstLargestMassErrors.Count < 100)
                        {
                            if (!lstLargestMassErrors.ContainsKey(massError))
                            {
                                lstLargestMassErrors.Add(massError, peptideDescription);
                            }
                        }
                        else
                        {
                            var minValue = lstLargestMassErrors.Keys.Min();
                            if (massError > minValue && !lstLargestMassErrors.ContainsKey(massError))
                            {
                                lstLargestMassErrors.Remove(minValue);
                                lstLargestMassErrors.Add(massError, peptideDescription);
                            }
                        }
                    }

                }

                if (intPsmCount == 0)
                {
                    OnWarningEvent("PHRPReader did not find any records in " + Path.GetFileName(inputFilePath));
                    return true;
                }

                var percentInvalid = intErrorCount / (float)intPsmCount * 100;

                if (intErrorCount <= 0)
                {
                    if (mDebugLevel >= 2)
                    {
                        OnStatusEvent("All " + intPsmCount + " peptides have a mass error below " + precursorMassTolerance.ToString("0.0") + " Da");
                    }
                    return true;
                }

                ErrorMessage = percentInvalid.ToString("0.0") + "% of the peptides have a mass error over " +
                                precursorMassTolerance.ToString("0.0") + " Da";

                var warningMessage = ErrorMessage + " (" + intErrorCount + " / " + intPsmCount + ")";

                if (percentInvalid <= mErrorThresholdPercent)
                {
                    OnWarningEvent(warningMessage + "; this value is within tolerance");

                    // Blank out mErrorMessage since only a warning
                    ErrorMessage = string.Empty;
                    return true;
                }

                OnErrorEvent(warningMessage + "; this value is too large (over " + mErrorThresholdPercent.ToString("0.0") + "%)");

                // Log the first, last, and middle entry in lstLargestMassErrors
                InformLargeErrorExample(lstLargestMassErrors.First());

                if (lstLargestMassErrors.Count > 1)
                {
                    InformLargeErrorExample(lstLargestMassErrors.Last());

                    if (lstLargestMassErrors.Count > 2)
                    {
                        var iterator = 0;
                        foreach (var massError in lstLargestMassErrors)
                        {
                            iterator += 1;
                            if (iterator >= lstLargestMassErrors.Count / 2.0)
                            {
                                InformLargeErrorExample(massError);
                                break;
                            }
                        }
                    }
                }

                return false;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ValidatePHRPResultMassErrors", ex);
                ErrorMessage = "Exception in ValidatePHRPResultMassErrors";
                return false;
            }
        }

    }
}
