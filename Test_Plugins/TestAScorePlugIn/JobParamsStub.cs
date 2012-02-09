using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using AnalysisManagerBase;


namespace TestAScorePlugIn {

    class JobParamsStub : IJobParams {

        private Dictionary<string, string> mParms;

        public JobParamsStub(Dictionary<string, string> parms) {
            mParms = parms;
        }

        #region IJobParams Members

        public string GetParam(string Name) {
            string val = "";
            if (mParms.ContainsKey(Name)) {
                val = mParms[Name];
            }
            return val;
        }

        public bool AddAdditionalParameter(string ParamName, string ParamValue) {
            throw new NotImplementedException();
        }

        public string GetCurrentJobToolDescription() {
            return "Test stub";
        }

        public void SetParam(string KeyName, string Value) {
            throw new NotImplementedException();
        }

        #endregion

        #region IJobParams Members

        public bool AddAdditionalParameter(string ParamSection, string ParamName, string ParamValue) {
            throw new NotImplementedException();
        }

        public string GetParam(string Section, string Name) {
            string val = "";
            if (mParms.ContainsKey(Name)) {
                val = mParms[Name];
            }
            return val;
        }

        public void SetParam(string Section, string KeyName, string Value) {
            //throw new NotImplementedException();
        }

        #endregion
    }
}
