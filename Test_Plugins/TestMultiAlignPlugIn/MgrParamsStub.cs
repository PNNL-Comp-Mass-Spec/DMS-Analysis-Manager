using System;
using System.Collections.Generic;
using AnalysisManagerBase;

namespace TestMultiAlignPlugIn
{
    class MgrParamsStub : IMgrParams
    {
        private Dictionary<string, string> mParms;

        public MgrParamsStub(Dictionary<string, string> parms)
        {

            ManagerName = "MgrParamsStub";

            if (!LoadSettings(parms))
            {
                throw new ApplicationException("Unable to initialize manager settings class");
            }
        }

        #region IMgrParams Properties

        public string ErrMsg => string.Empty;

        public string ManagerName { get; }

        #endregion

        #region IMgrParams Members

        public void AckManagerUpdateRequired()
        {
        }

        public bool DisableManagerLocally()
        {
            return true;
        }

        public bool LoadDBSettings()
        {
            throw new NotImplementedException();
        }

        public bool LoadSettings(Dictionary<string, string> ConfigFileSettings)
        {
            mParms = ConfigFileSettings;
            return true;
        }

        public string GetParam(string Name) {
            var val = "";
            if (mParms.ContainsKey(Name)) {
                val = mParms[Name];
            }
            return val;
        }

        public bool GetParam(string ItemKey, bool ValueIfMissing)
        {
            return Global.CBoolSafe(GetParam(ItemKey), ValueIfMissing);
        }

        public int GetParam(string ItemKey, int ValueIfMissing)
        {
            return Global.CIntSafe(GetParam(ItemKey), ValueIfMissing);
        }

        public string GetParam(string ItemKey, string ValueIfMissing)
        {
            var paramValue = GetParam(ItemKey);
            if (string.IsNullOrEmpty(paramValue))
            {
                return ValueIfMissing;
            }
            return paramValue;
        }
        public void SetParam(string ItemKey, string ItemValue) {
            throw new NotImplementedException();
        }

        #endregion
    }

}
