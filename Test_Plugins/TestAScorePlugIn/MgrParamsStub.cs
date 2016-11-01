using System;
using System.Collections.Generic;
using AnalysisManagerBase;


namespace TestAScorePlugIn {

    class MgrParamsStub : IMgrParams {

        private Dictionary<string, string> mParms;

        public MgrParamsStub(Dictionary<string, string> parms) {

            if (!LoadSettings(parms))
            {
                throw new ApplicationException("Unable to initialize manager settings class");
            }
        }

        #region IMgrParams Properties

        public string ErrMsg
        {
            get
            {
                return string.Empty;
            }
        }

        #endregion

        #region IMgrParams Members

        public void AckManagerUpdateRequired()
        {
            return;
        }

        public bool DisableManagerLocally()
        {
            return true;
        }

        public bool LoadDBSettings()
        {
            throw new NotImplementedException();
        }

        public bool LoadSettings(System.Collections.Generic.Dictionary<string, string> ConfigFileSettings)
        {
            mParms = ConfigFileSettings;
            return true;
        }

        public string GetParam(string Name) {
            string val = "";
            if (mParms.ContainsKey(Name)) {
                val = mParms[Name];
            }
            return val;
        }

        public bool GetParam(string ItemKey, bool ValueIfMissing)
        {
            return clsGlobal.CBoolSafe(GetParam(ItemKey), ValueIfMissing);
        }

        public int GetParam(string ItemKey, int ValueIfMissing)
        {
            return clsGlobal.CIntSafe(GetParam(ItemKey), ValueIfMissing);
        }

        public string GetParam(string ItemKey, string ValueIfMissing)
        {
            string strValue = GetParam(ItemKey);
            if (string.IsNullOrEmpty(strValue))
            {
                return ValueIfMissing;
            }
            else
            {
                return strValue;
            }
        }
        public void SetParam(string ItemKey, string ItemValue) {
            throw new NotImplementedException();
        }

        #endregion
    }

}
