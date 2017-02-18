
using System;
using Microsoft.Win32;
using PRISM;

namespace AnalysisManagerBase
{
    public class clsProteowizardTools : clsEventNotifier
    {

        protected int mDebugLevel;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="debugLevel"></param>
        public clsProteowizardTools(int debugLevel)
        {
            mDebugLevel = debugLevel;
        }

        public bool RegisterProteoWizard()
        {
            var blnValueMissing = false;

            try
            {
                // Tool setup for MSConvert involves creating a
                //  registry entry at HKEY_CURRENT_USER\Software\ProteoWizard
                //  to indicate that we agree to the Thermo license

                bool blnSubKeyMissing;
                try
                {
                    if (mDebugLevel >= 2)
                    {
                        OnStatusEvent(@"Confirming that 'Software\ProteoWizard' registry key exists");
                    }

                    var regSoftware = Microsoft.Win32.Registry.CurrentUser.OpenSubKey("Software", false);
                    if (regSoftware == null)
                        throw new Exception("Unable to open the Software node in the registry");

                    var regProteoWizard = regSoftware.OpenSubKey("ProteoWizard", false);

                    if (regProteoWizard == null)
                    {
                        blnSubKeyMissing = true;
                    }
                    else
                    {
                        blnSubKeyMissing = false;

                        if (mDebugLevel >= 2)
                        {
                            OnStatusEvent("Confirming that 'Thermo MSFileReader' registry key exists");
                        }

                        var objValue = regProteoWizard.GetValue("Thermo MSFileReader");

                        if (objValue == null)
                        {
                            blnValueMissing = true;
                        }
                        else if (string.IsNullOrEmpty(Convert.ToString(objValue)))
                        {
                            blnValueMissing = true;
                        }
                        else
                        {
                            if (bool.Parse(Convert.ToString(objValue)))
                            {
                                blnValueMissing = false;
                            }
                            else
                            {
                                blnValueMissing = true;
                            }
                        }

                        regProteoWizard.Close();
                    }

                    regSoftware.Close();

                }
                catch (Exception ex)
                {
                    OnWarningEvent("Exception looking for key (possibly not found): " + ex.Message);
                    blnSubKeyMissing = true;
                    blnValueMissing = true;
                }

                if (blnSubKeyMissing | blnValueMissing)
                {
                    var regSoftware = Microsoft.Win32.Registry.CurrentUser.OpenSubKey("Software", true);
                    if (regSoftware == null)
                        throw new Exception("Unable to open the Software node in the registry");

                    RegistryKey regProteoWizard;

                    if (blnSubKeyMissing)
                    {
                        if (mDebugLevel >= 1)
                        {
                            OnStatusEvent(@"Creating 'Software\ProteoWizard' SubKey");
                        }
                        regProteoWizard = regSoftware.CreateSubKey("ProteoWizard");
                    }
                    else
                    {
                        if (mDebugLevel >= 1)
                        {
                            OnStatusEvent(@"Opening 'Software\ProteoWizard' SubKey");
                        }
                        regProteoWizard = regSoftware.OpenSubKey("ProteoWizard", true);
                    }

                    if (mDebugLevel >= 1)
                    {
                        OnStatusEvent("Setting value for 'Thermo MSFileReader' registry key to 'True'");
                    }

                    if (regProteoWizard != null)
                    {
                        regProteoWizard.SetValue("Thermo MSFileReader", "True");
                        regProteoWizard.Close();
                    }
                    regSoftware.Close();
                }

            }
            catch (Exception ex)
            {
                var msg = "Error creating ProteoWizard registry key";
                Console.WriteLine(msg + ": " + ex.Message);
                OnErrorEvent(msg, ex);
                return false;
            }

            return true;
        }

    }
}
