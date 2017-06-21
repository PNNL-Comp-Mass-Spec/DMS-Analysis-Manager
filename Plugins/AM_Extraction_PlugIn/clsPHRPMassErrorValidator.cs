using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using AnalysisManagerBase;
using PHRPReader;
using PRISM;

namespace AnalysisManagerExtractionPlugin
{
    public class clsPHRPMassErrorValidator : clsEventNotifier
    {
        #region "Module variables"

        private string mErrorMessage = string.Empty;
        private readonly int mDebugLevel;

        // This is a value between 0 and 100
        private const double mErrorThresholdPercent = 5;

        private clsPHRPReader mPHRPReader;

        #endregion

        public string ErrorMessage
        {
            get { return mErrorMessage; }
        }

        /// <summary>
        /// Value between 0 and 100
        /// If more than this percent of the data has a mass error larger than the threshold, then ValidatePHRPResultMassErrors returns false
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public double ErrorThresholdPercent
        {
            get { return mErrorThresholdPercent; }
        }

        public clsPHRPMassErrorValidator(int intDebugLevel)
        {
            mDebugLevel = intDebugLevel;
        }

        private void InformLargeErrorExample(KeyValuePair<double, string> massErrorEntry)
        {
            OnErrorEvent("  ... large error example: " + massErrorEntry.Key + " Da for " + massErrorEntry.Value);
        }

        private clsSearchEngineParameters LoadSearchEngineParameters(clsPHRPReader objPHRPReader, string strSearchEngineParamFilePath, clsPHRPReader.ePeptideHitResultType eResultType)
        {
            clsSearchEngineParameters objSearchEngineParams = null;
            var blnSuccess = false;

            try
            {
                if (string.IsNullOrEmpty(strSearchEngineParamFilePath))
                {
                    OnWarningEvent("Search engine parameter file not defined; will assume a maximum tolerance of 10 Da");
                    objSearchEngineParams = new clsSearchEngineParameters(eResultType.ToString());
                    objSearchEngineParams.AddUpdateParameter("peptide_mass_tol", "10");
                }
                else
                {
                    blnSuccess = objPHRPReader.PHRPParser.LoadSearchEngineParameters(strSearchEngineParamFilePath, out objSearchEngineParams);

                    if (!blnSuccess)
                    {
                        OnWarningEvent("Error loading search engine parameter file " + Path.GetFileName(strSearchEngineParamFilePath) +
                                           "; will assume a maximum tolerance of 10 Da");
                        objSearchEngineParams = new clsSearchEngineParameters(eResultType.ToString());
                        objSearchEngineParams.AddUpdateParameter("peptide_mass_tol", "10");
                    }
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in LoadSearchEngineParameters", ex);
            }

            return objSearchEngineParams;
        }

        /// <summary>
        /// Parses strInputFilePath to count the number of entries where the difference in mass
        /// between the precursor neutral mass value and the computed monoisotopic mass value
        /// is more than 6 Da away (more for higher charge states)
        /// </summary>
        /// <param name="strInputFilePath"></param>
        /// <param name="eResultType"></param>
        /// <param name="strSearchEngineParamFilePath"></param>
        /// <returns>True if less than mErrorThresholdPercent of the data is bad; False otherwise</returns>
        /// <remarks></remarks>
        public bool ValidatePHRPResultMassErrors(string strInputFilePath, clsPHRPReader.ePeptideHitResultType eResultType, string strSearchEngineParamFilePath)
        {
            try
            {
                mErrorMessage = string.Empty;

                var oPeptideMassCalculator = new clsPeptideMassCalculator();

                var oStartupOptions = new clsPHRPStartupOptions
                {
                    LoadModsAndSeqInfo = true,
                    LoadMSGFResults = false,
                    LoadScanStatsData = false,
                    MaxProteinsPerPSM = 1,
                    PeptideMassCalculator = oPeptideMassCalculator
                };

                mPHRPReader = new clsPHRPReader(strInputFilePath, eResultType, oStartupOptions);
                mPHRPReader.ErrorEvent += mPHRPReader_ErrorEvent;
                mPHRPReader.MessageEvent += mPHRPReader_MessageEvent;
                mPHRPReader.WarningEvent += mPHRPReader_WarningEvent;

                // Report any errors cached during instantiation of mPHRPReader
                foreach (var strMessage in mPHRPReader.ErrorMessages)
                {
                    if (string.IsNullOrEmpty(mErrorMessage))
                    {
                        mErrorMessage = string.Copy(strMessage);
                    }
                    OnErrorEvent(strMessage);
                }
                if (mPHRPReader.ErrorMessages.Count > 0)
                    return false;

                // Report any warnings cached during instantiation of mPHRPReader
                foreach (var strMessage in mPHRPReader.WarningMessages)
                {
                    if (strMessage.StartsWith("Warning, taxonomy file not found"))
                    {
                        // Ignore this warning; the taxonomy file would have been used to determine the fasta file that was searched
                        // We don't need that information in this application
                    }
                    else
                    {
                        OnWarningEvent(strMessage);
                    }
                }

                mPHRPReader.ClearErrors();
                mPHRPReader.ClearWarnings();
                mPHRPReader.SkipDuplicatePSMs = true;

                // Load the search engine parameters
                var objSearchEngineParams = LoadSearchEngineParameters(mPHRPReader, strSearchEngineParamFilePath, eResultType);

                // Check for a custom charge carrier mass
                double customChargeCarrierMass = 0;
                if (clsPHRPParserMSGFDB.GetCustomChargeCarrierMass(objSearchEngineParams, out customChargeCarrierMass))
                {
                    if (mDebugLevel >= 2)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                            string.Format("Custom charge carrier mass defined: {0:F3} Da", customChargeCarrierMass));
                    }
                    oPeptideMassCalculator.ChargeCarrierMass = customChargeCarrierMass;
                }

                // Define the precursor mass tolerance threshold
                // At a minimum, use 6 Da, though we'll bump that up by 1 Da for each charge state (7 Da for CS 2, 8 Da for CS 3, 9 Da for CS 4, etc.)
                // However, for MSGF+ we require that the masses match within 0.1 Da because the IsotopeError column allows for a more accurate comparison
                var dblPrecursorMassTolerance = objSearchEngineParams.PrecursorMassToleranceDa;
                if (dblPrecursorMassTolerance < 6)
                {
                    dblPrecursorMassTolerance = 6;
                }

                var highResMS1 = objSearchEngineParams.PrecursorMassToleranceDa < 0.75;

                if (mDebugLevel >= 2)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                        "Will use mass tolerance of " + dblPrecursorMassTolerance.ToString("0.0") + " Da when determining PHRP mass errors");
                }

                // Count the number of PSMs with a mass error greater than dblPrecursorMassTolerance

                var intErrorCount = 0;
                var intPsmCount = 0;
                var dtLastProgress = System.DateTime.UtcNow;

                string strPeptideDescription = null;
                var lstLargestMassErrors = new SortedDictionary<double, string>();

                while (mPHRPReader.MoveNext())
                {
                    //// This is old code that was in LoadSearchEngineParameters and was called after all PSMs had been cached in memory
                    //// Since we're no longer pre-caching PSMs in memory, this code block was moved to this function
                    //// However, I don't think this code is really needed, so I've commented it out
                    ////
                    //// Make sure mSearchEngineParams.ModInfo is up-to-date
                    //if (mPHRPReader.CurrentPSM.ModifiedResidues.Count > 0)
                    //{
                    //    foreach (var objResidue in mPHRPReader.CurrentPSM.ModifiedResidues)
                    //    {
                    //        // Check whether .ModDefinition is present in objSearchEngineParams.ModInfo
                    //        var blnMatchFound = false;
                    //        foreach (var objKnownMod in objSearchEngineParams.ModInfo)
                    //        {
                    //            if (objKnownMod == objResidue.ModDefinition)
                    //            {
                    //                blnMatchFound = true;
                    //                break;
                    //            }
                    //        }
                    //
                    //        if (!blnMatchFound)
                    //        {
                    //            objSearchEngineParams.ModInfo.Add(objResidue.ModDefinition);
                    //        }
                    //    }
                    //}

                    intPsmCount += 1;

                    if (intPsmCount % 100 == 0 && System.DateTime.UtcNow.Subtract(dtLastProgress).TotalSeconds >= 15)
                    {
                        dtLastProgress = System.DateTime.UtcNow;
                        var statusMessage = "Validating mass errors: " + mPHRPReader.PercentComplete.ToString("0.0") + "% complete";
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, statusMessage);
                        Console.WriteLine(statusMessage);
                    }

                    var objCurrentPSM = mPHRPReader.CurrentPSM;

                    if (objCurrentPSM.PeptideMonoisotopicMass <= 0)
                    {
                        continue;
                    }

                    // PrecursorNeutralMass is based on the mass value reported by the search engine
                    //   (will be reported mono mass or could be m/z or MH converted to neutral mass)
                    // PeptideMonoisotopicMass is the mass value computed by PHRP based on .PrecursorNeutralMass plus any modification masses associated with residues
                    var dblMassError = objCurrentPSM.PrecursorNeutralMass - objCurrentPSM.PeptideMonoisotopicMass;
                    double dblToleranceCurrent = 0;

                    string psmIsotopeError;
                    if (eResultType == clsPHRPReader.ePeptideHitResultType.MSGFDB &&
                        highResMS1 &&
                        objCurrentPSM.TryGetScore("IsotopeError", out psmIsotopeError))
                    {
                        // The integer value of dblMassError should match psmIsotopeError
                        // However, scale up the tolerance based on the peptide mass
                        dblToleranceCurrent = 0.2 + objCurrentPSM.PeptideMonoisotopicMass / 50000.0;
                        dblMassError -= Convert.ToInt32(psmIsotopeError);
                    }
                    else
                    {
                        dblToleranceCurrent = dblPrecursorMassTolerance + objCurrentPSM.Charge - 1;
                    }

                    if (Math.Abs(dblMassError) <= dblToleranceCurrent)
                    {
                        continue;
                    }

                    strPeptideDescription = "Scan=" + objCurrentPSM.ScanNumberStart + ", charge=" + objCurrentPSM.Charge + ", peptide=" +
                                            objCurrentPSM.PeptideWithNumericMods;
                    intErrorCount += 1;

                    // Keep track of the 100 largest mass errors
                    if (lstLargestMassErrors.Count < 100)
                    {
                        if (!lstLargestMassErrors.ContainsKey(dblMassError))
                        {
                            lstLargestMassErrors.Add(dblMassError, strPeptideDescription);
                        }
                    }
                    else
                    {
                        var dblMinValue = lstLargestMassErrors.Keys.Min();
                        if (dblMassError > dblMinValue && !lstLargestMassErrors.ContainsKey(dblMassError))
                        {
                            lstLargestMassErrors.Remove(dblMinValue);
                            lstLargestMassErrors.Add(dblMassError, strPeptideDescription);
                        }
                    }
                }

                mPHRPReader.Dispose();

                if (intPsmCount == 0)
                {
                    OnWarningEvent("PHRPReader did not find any records in " + Path.GetFileName(strInputFilePath));
                    return true;
                }

                var dblPercentInvalid = intErrorCount / (float)intPsmCount * 100;

                if (intErrorCount <= 0)
                {
                    if (mDebugLevel >= 2)
                    {
                        OnStatusEvent("All " + intPsmCount + " peptides have a mass error below " + dblPrecursorMassTolerance.ToString("0.0") + " Da");
                    }
                    return true;
                }

                mErrorMessage = dblPercentInvalid.ToString("0.0") + "% of the peptides have a mass error over " +
                                dblPrecursorMassTolerance.ToString("0.0") + " Da";

                var warningMessage = mErrorMessage + " (" + intErrorCount + " / " + intPsmCount + ")";

                if (dblPercentInvalid <= mErrorThresholdPercent)
                {
                    OnWarningEvent(warningMessage + "; this value is within tolerance");

                    // Blank out mErrorMessage since only a warning
                    mErrorMessage = string.Empty;
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
                mErrorMessage = "Exception in ValidatePHRPResultMassErrors";
                return false;
            }
        }

        private void mPHRPReader_ErrorEvent(string strErrorMessage)
        {
            OnErrorEvent("PHRPReader: " + strErrorMessage);
        }

        private void mPHRPReader_MessageEvent(string strMessage)
        {
            OnStatusEvent("PHRPReader: " + strMessage);
        }

        private void mPHRPReader_WarningEvent(string strWarningMessage)
        {
            OnWarningEvent("PHRPReader: " + strWarningMessage);
        }
    }
}
