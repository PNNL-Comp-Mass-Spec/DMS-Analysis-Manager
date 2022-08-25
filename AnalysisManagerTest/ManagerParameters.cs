using System.Collections.Generic;
using AnalysisManagerBase;
using PRISMDatabaseUtils.AppSettings;

namespace AnalysisManagerTest
{
    internal class ManagerParameters : MgrSettingsDB, IMgrParams
    {
        public void AckManagerUpdateRequired()
        {
        }

        public bool DisableManagerLocally()
        {
            return false;
        }

        public bool HasParam(string name)
        {
            return MgrParams.ContainsKey(name);
        }

        public void PauseManagerTaskRequests(int holdoffIntervalMinutes = 60)
        {
        }

        public bool LoadDBSettings()
        {
            return LoadMgrSettingsFromDB(retryCount: 6);
        }

        public bool LoadSettings(Dictionary<string, string> configFileSettings)
        {
            return LoadSettings(configFileSettings, true);
        }
    }
}
