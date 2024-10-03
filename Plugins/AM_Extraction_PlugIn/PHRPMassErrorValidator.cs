using PHRPReader;
using PRISM;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using PHRPReader.Data;
using PHRPReader.Reader;

namespace AnalysisManagerExtractionPlugin
{
    public class PHRPMassErrorValidator : EventNotifier
    {
        // Ignore Spelling: Da, psm, tol, Validator

        private readonly int mDebugLevel;

        /// <summary>
        /// Error message
        /// </summary>
        public string ErrorMessage { get; private set; } = string.Empty;

        /// <summary>
        /// Error threshold percentage (Value between 0 and 100)
        /// </summary>
        /// <remarks>
        /// If more than this percent of the data has a mass error larger than the threshold,
        /// and if the count is greater than <see cref="ErrorThresholdCount"/>, ValidatePHRPResultMassErrors returns false
        /// </remarks>
        public double ErrorThresholdPercent { get; set; } = 5;

        /// <summary>
        /// Error count threshold
        /// </summary>
        /// <remarks>
        /// Used by ValidatePHRPResultMassErrors when determining whether too many results have a large mass error
        /// </remarks>
        public int ErrorThresholdCount { get; set; } = 25;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="debugLevel">Debug level for logging; 1=minimal logging; 5=detailed logging</param>
        public PHRPMassErrorValidator(int debugLevel)
        {
            mDebugLevel = debugLevel;
        }

        /// <summary>
        /// Read the precursor mass tolerance from the search engine's parameter file
        /// Next, compute the mass error for each PSM and keep track of the 100 largest mass errors
        /// </summary>
        /// <param name="inputFilePath">Input file path (tab-delimited text file created by PHRP)</param>
        /// <param name="resultType">PHRP result type enum</param>
        /// <param name="searchEngineParamFilePath">Search engine parameter file path</param>
        /// <param name="largestMassErrors">Output: dictionary of the largest mass errors</param>
        /// <param name="precursorMassTolerance">Output: precursor mass tolerance, in Da</param>
        /// <param name="psmCount">Output: number of PSMs</param>
        /// <param name="errorCount">Output: number of errors</param>
        /// <param name="diaSearchEnabled">Output: true if the search results come from a DIA dataset</param>
        /// <returns>True if success, false if an error</returns>
        private bool ExaminePHRPResults(
            string inputFilePath,
            PeptideHitResultTypes resultType,
            string searchEngineParamFilePath,
            out SortedDictionary<double, string> largestMassErrors,
            out double precursorMassTolerance,
            out int psmCount,
            out int errorCount,
            out bool diaSearchEnabled)
        {
            largestMassErrors = new SortedDictionary<double, string>();
            var peptideMassCalculator = new PeptideMassCalculator();

            var startupOptions = new StartupOptions
            {
                LoadModsAndSeqInfo = true,
                LoadMSGFResults = false,
                LoadScanStatsData = false,
                MaxProteinsPerPSM = 1,
                PeptideMassCalculator = peptideMassCalculator
            };

            using var reader = new ReaderFactory(inputFilePath, resultType, startupOptions);

            RegisterEvents(reader);

            // Progress is reported below via a manual call to OnDebugEvent
            reader.ProgressUpdate -= OnProgressUpdate;
            reader.SkipConsoleWriteIfNoProgressListener = true;

            // Report any errors cached during instantiation of mPHRPReader
            foreach (var message in reader.ErrorMessages)
            {
                if (string.IsNullOrEmpty(ErrorMessage))
                {
                    ErrorMessage = message;
                }

                OnErrorEvent(message);
            }

            if (reader.ErrorMessages.Count > 0)
            {
                precursorMassTolerance = 0;
                psmCount = 0;
                errorCount = 0;
                diaSearchEnabled = false;

                return false;
            }

            // Report any warnings cached during instantiation of mPHRPReader
            foreach (var message in reader.WarningMessages)
            {
                if (message.StartsWith("Warning, taxonomy file not found"))
                {
                    // Ignore this warning; the taxonomy file would have been used to determine the FASTA file that was searched
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
            var searchEngineParams = LoadSearchEngineParameters(reader, searchEngineParamFilePath, resultType);

            // Check for a custom charge carrier mass
            if (MSGFPlusSynFileReader.GetCustomChargeCarrierMass(searchEngineParams, out var customChargeCarrierMass))
            {
                if (mDebugLevel >= 2)
                {
                    OnDebugEvent("Custom charge carrier mass defined: {0:F3} Da", customChargeCarrierMass);
                }

                peptideMassCalculator.ChargeCarrierMass = customChargeCarrierMass;
            }

            // Define the precursor mass tolerance threshold
            // At a minimum, use 6 Da, though we'll bump that up by 1 Da for each charge state (7 Da for CS 2, 8 Da for CS 3, 9 Da for CS 4, etc.)
            // However, for MS-GF+ we require that the masses match within 0.1 Da because the IsotopeError column allows for a more accurate comparison

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
                OnDebugEvent("Will use mass tolerance of {0:0.0} Da when determining PHRP mass errors", precursorMassTolerance);
            }

            // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
            if (resultType == PeptideHitResultTypes.DiaNN)
            {
                diaSearchEnabled = true;
            }
            else
            {
                diaSearchEnabled = MSFraggerDIASearchEnabled(resultType, searchEngineParams);
            }

            // Count the number of PSMs with a mass error greater than precursorMassTolerance

            var lastProgressTime = DateTime.UtcNow;

            psmCount = 0;
            errorCount = 0;

            while (reader.MoveNext())
            {
                psmCount++;

                if (psmCount % 100 == 0 && DateTime.UtcNow.Subtract(lastProgressTime).TotalSeconds >= 15)
                {
                    lastProgressTime = DateTime.UtcNow;
                    OnDebugEvent("Validating mass errors: " + reader.PercentComplete.ToString("0.0") + "% complete");
                }

                var currentPSM = reader.CurrentPSM;

                if (currentPSM.PeptideMonoisotopicMass <= 0)
                {
                    continue;
                }

                // PrecursorNeutralMass is based on the mass value reported by the search engine
                //   (will be reported mono mass or could be observed m/z or MH converted to neutral mass)
                // PeptideMonoisotopicMass is the mass value computed by PHRP based on .PrecursorNeutralMass plus any modification masses associated with residues

                // For MaxQuant, precursor m/z values are loaded from the _PrecursorInfo.txt file, since precursor m/z values reported by MaxQuant are often several Da away from the observed m/z
                // DIA-NN does not report observed precursor m/z values, so massError will be zero (or very close to zero) for all PSMs

                var massError = currentPSM.PrecursorNeutralMass - currentPSM.PeptideMonoisotopicMass;

                double toleranceCurrent;

                if (resultType == PeptideHitResultTypes.MSGFPlus &&
                    highResMS1 &&
                    currentPSM.TryGetScore("IsotopeError", out var psmIsotopeError))
                {
                    // The integer value of massError should match psmIsotopeError
                    // However, scale up the tolerance based on the peptide mass
                    toleranceCurrent = 0.2 + currentPSM.PeptideMonoisotopicMass / 50000.0;

                    var psmIsotopeErrorValue = int.Parse(psmIsotopeError);

                    if (psmIsotopeErrorValue != 0)
                    {
                        massError -= psmIsotopeErrorValue;
                    }
                }
                else
                {
                    toleranceCurrent = precursorMassTolerance + currentPSM.Charge - 1;
                }

                if (Math.Abs(massError) <= toleranceCurrent)
                {
                    continue;
                }

                var peptideDescription = string.Format(
                    "Scan={0}, charge={1}, peptide={2}",
                    currentPSM.ScanNumberStart, currentPSM.Charge, currentPSM.PeptideWithNumericMods);

                errorCount++;

                // Keep track of the 100 largest mass errors
                if (largestMassErrors.Count < 100)
                {
                    if (!largestMassErrors.ContainsKey(massError))
                    {
                        largestMassErrors.Add(massError, peptideDescription);
                    }
                }
                else
                {
                    var minValue = largestMassErrors.Keys.Min();

                    if (massError > minValue && !largestMassErrors.ContainsKey(massError))
                    {
                        largestMassErrors.Remove(minValue);
                        largestMassErrors.Add(massError, peptideDescription);
                    }
                }
            }

            return true;
        }

        private void InformLargeErrorExample(KeyValuePair<double, string> massErrorEntry)
        {
            OnErrorEvent("  ... large error example: {0:f2} Da for {1}", massErrorEntry.Key, massErrorEntry.Value);
        }

        private SearchEngineParameters LoadSearchEngineParameters(ReaderFactory phrpReader, string searchEngineParamFilePath, PeptideHitResultTypes resultType)
        {
            SearchEngineParameters searchEngineParams = null;

            try
            {
                if (string.IsNullOrEmpty(searchEngineParamFilePath))
                {
                    OnWarningEvent("Search engine parameter file not defined; will assume a maximum tolerance of 10 Da");
                    searchEngineParams = new SearchEngineParameters(resultType.ToString());
                    searchEngineParams.AddUpdateParameter("peptide_mass_tol", "10");
                }
                else
                {
                    var success = phrpReader.SynFileReader.LoadSearchEngineParameters(searchEngineParamFilePath, out searchEngineParams);

                    if (!success)
                    {
                        OnWarningEvent("Error loading search engine parameter file " + Path.GetFileName(searchEngineParamFilePath) +
                                           "; will assume a maximum tolerance of 10 Da");
                        searchEngineParams = new SearchEngineParameters(resultType.ToString());
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
        /// If processing MSFragger results and the value for data_type in the parameter file is 1 or 2, return true since we're processing DIA data
        /// Otherwise, return false
        /// </summary>
        /// <param name="resultType">PHRP result type</param>
        /// <param name="searchEngineParams">Search engine parameters</param>
        private bool MSFraggerDIASearchEnabled(PeptideHitResultTypes resultType, SearchEngineParameters searchEngineParams)
        {
            if (resultType != PeptideHitResultTypes.MSFragger)
                return false;

            if (!searchEngineParams.Parameters.TryGetValue("data_type", out var dataTypeMode))
                return false;

            if (!int.TryParse(dataTypeMode, out var dataTypeModeValue))
                return false;

            return dataTypeModeValue is 1 or 2;
        }

        /// <summary>
        /// Parses inputFilePath to count the number of entries where the difference in mass
        /// between the precursor neutral mass value and the computed monoisotopic mass value
        /// is more than 6 Da away (more for higher charge states)
        /// </summary>
        /// <param name="inputFilePath">Input file path (tab-delimited text file created by PHRP)</param>
        /// <param name="resultType">PHRP result type</param>
        /// <param name="searchEngineParamFilePath">Search engine parameter file path</param>
        /// <returns>True if less than ErrorThresholdPercent of the data is bad; false otherwise</returns>
        public bool ValidatePHRPResultMassErrors(string inputFilePath, PeptideHitResultTypes resultType, string searchEngineParamFilePath)
        {
            try
            {
                ErrorMessage = string.Empty;

                var success = ExaminePHRPResults(
                    inputFilePath, resultType, searchEngineParamFilePath,
                    out var largestMassErrors,
                    out var precursorMassTolerance,
                    out var psmCount,
                    out var errorCount,
                    out var diaSearchEnabled);

                if (!success)
                    return false;

                if (psmCount == 0)
                {
                    OnWarningEvent("PHRPReader did not find any records in " + Path.GetFileName(inputFilePath));
                    return true;
                }

                var percentInvalid = errorCount / (float)psmCount * 100;

                if (errorCount <= 0)
                {
                    if (mDebugLevel >= 2)
                    {
                        OnStatusEvent("All {0:#,###} peptides have a mass error below {1:F1} Da{2}",
                            psmCount,
                            precursorMassTolerance,
                            resultType == PeptideHitResultTypes.DiaNN ? "; DIA-NN results do not include mass errors, so this is to be expected" : string.Empty);
                    }
                    return true;
                }

                // 70.69% of the peptides have a mass error over 6.0 Da (164 / 232)
                ErrorMessage = string.Format(
                    "{0:F2}% of the peptides have a mass error over {1:F1} Da",
                    percentInvalid, precursorMassTolerance);

                var warningMessage = string.Format("{0} ({1:#,###} / {2:#,###})", ErrorMessage, errorCount, psmCount);

                if (percentInvalid <= ErrorThresholdPercent || errorCount <= ErrorThresholdCount)
                {
                    OnWarningEvent(warningMessage + "; this value is within tolerance");

                    // Blank out mErrorMessage since only a warning
                    ErrorMessage = string.Empty;
                    return true;
                }

                if (diaSearchEnabled)
                {
                    OnWarningEvent(warningMessage + "; ignoring since processing DIA results");

                    // Blank out mErrorMessage since only a warning
                    ErrorMessage = string.Empty;
                    return true;
                }

                // 70.69% of the peptides have a mass error over 6.0 Da (164 / 232); this value is too large (over 10.0%)
                OnErrorEvent(warningMessage + "; this value is too large (over " + ErrorThresholdPercent.ToString("0.0") + "%)");

                OnErrorEvent("To ignore this error, update this job to use a settings file that has 'IgnoreMassErrorValidationErrors' set to true");

                // Log the first, last, and middle entry in largestMassErrors
                InformLargeErrorExample(largestMassErrors.First());

                if (largestMassErrors.Count <= 1)
                {
                    return false;
                }

                InformLargeErrorExample(largestMassErrors.Last());

                if (largestMassErrors.Count <= 2)
                {
                    return false;
                }

                var iterator = 0;

                foreach (var massError in largestMassErrors)
                {
                    iterator++;

                    if (iterator >= largestMassErrors.Count / 2.0)
                    {
                        InformLargeErrorExample(massError);
                        break;
                    }
                }

                return false;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ValidatePHRPResultMassErrors", ex);
                ErrorMessage = "Error in ValidatePHRPResultMassErrors";
                return false;
            }
        }
    }
}
