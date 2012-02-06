using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using AnalysisManagerBase;


namespace TestApePlugIn {

    class MgrParamsStub : IMgrParams {

        private Dictionary<string, string> mParms;

        public MgrParamsStub(Dictionary<string, string> parms) {
            mParms = parms;
        }

        #region IMgrParams Members

        public string GetParam(string Name) {
            string val = "";
            if (mParms.ContainsKey(Name)) {
                val = mParms[Name];
            }
            return val;
        }

        public void SetParam(string ItemKey, string ItemValue) {
            throw new NotImplementedException();
        }

        #endregion
    }

}
