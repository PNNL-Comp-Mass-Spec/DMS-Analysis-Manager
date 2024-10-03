using System;
using PRISM;

namespace AnalysisManagerBase.DataFileTools
{
    /// <summary>
    /// ProteoWizard tools
    /// </summary>
    public class ProteowizardTools : EventNotifier
    {
        // Ignore Spelling: Proteo, Proteowizard

        /// <summary>
        /// Debug level
        /// </summary>
        protected int mDebugLevel;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="debugLevel">Debug level for logging; 1=minimal logging; 5=detailed logging</param>
        public ProteowizardTools(int debugLevel)
        {
            mDebugLevel = debugLevel;
        }

        /// <summary>
        /// Register ProteoWizard
        /// </summary>
        public bool RegisterProteoWizard()
        {
            if (Global.LinuxOS)
            {
                OnWarningEvent("Skipping call to RegisterProteoWizard since running on Linux");
                return true;
            }

            return RegisterProteoWizardWindows();
        }

        /// <summary>
        /// Register ProteoWizard
        /// </summary>
        private bool RegisterProteoWizardWindows()
        {
            var valueMissing = false;

            try
            {
                // Tool setup for MSConvert involves creating a
                //  registry entry at HKEY_CURRENT_USER\Software\ProteoWizard
                //  to indicate that we agree to the Thermo license

                bool subKeyMissing;
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
                        subKeyMissing = true;
                    }
                    else
                    {
                        subKeyMissing = false;

                        if (mDebugLevel >= 2)
                        {
                            OnStatusEvent("Confirming that 'Thermo MSFileReader' registry key exists");
                        }

                        var registryValue = regProteoWizard.GetValue("Thermo MSFileReader");

                        if (registryValue == null)
                        {
                            valueMissing = true;
                        }
                        else if (string.IsNullOrEmpty(Convert.ToString(registryValue)))
                        {
                            valueMissing = true;
                        }
                        else if(!bool.Parse(Convert.ToString(registryValue)))
                        {
                            valueMissing = true;
                        }

                        regProteoWizard.Close();
                    }

                    regSoftware.Close();
                }
                catch (Exception ex)
                {
                    OnWarningEvent("Exception looking for key (possibly not found): " + ex.Message);
                    subKeyMissing = true;
                    valueMissing = true;
                }

                if (subKeyMissing || valueMissing)
                {
                    var regSoftware = Microsoft.Win32.Registry.CurrentUser.OpenSubKey("Software", true);

                    if (regSoftware == null)
                        throw new Exception("Unable to open the Software node in the registry");

                    Microsoft.Win32.RegistryKey regProteoWizard;

                    if (subKeyMissing)
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
                const string msg = "Error creating ProteoWizard registry key";
                Console.WriteLine(msg + ": " + ex.Message);
                OnErrorEvent(msg, ex);
                return false;
            }

            return true;
        }
    }
}
