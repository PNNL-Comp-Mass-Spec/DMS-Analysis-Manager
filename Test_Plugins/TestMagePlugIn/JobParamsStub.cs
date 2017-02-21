using System;
using System.Collections.Generic;
using AnalysisManagerBase;


namespace TestMagePlugIn {

    class JobParamsStub : IJobParams {

        private Dictionary<string, string> mParms;

        // List of file extensions to NOT move to the result folder; comparison checks if the end of the filename matches any entry ResultFileExtensionsToSkip
        protected SortedSet<string> m_ResultFileExtensionsToSkip = new SortedSet<string>(StringComparer.CurrentCultureIgnoreCase);

        public JobParamsStub(Dictionary<string, string> parms) {
            mParms = parms;
        }

        #region IJobParams Properties

        public Dictionary<string, int> DatasetInfoList
        {
            get
            {
                return new Dictionary<string, int>();
            }
        }
        
        public SortedSet<string> ResultFilesToKeep
        {
            get
            {
                return new SortedSet<string>();
            }
        }

        public SortedSet<string> ResultFilesToSkip
        {
            get
            {
                return new SortedSet<string>();
            }
        }

        public SortedSet<string> ResultFileExtensionsToSkip
        {
            get
            {
                return m_ResultFileExtensionsToSkip;
            }
        }

        public SortedSet<string> ServerFilesToDelete
        {
            get
            {
                return new SortedSet<string>();
            }
        }

        #endregion

        #region IJobParams Members

        public bool AddAdditionalParameter(string ParamSection, string ParamName, string ParamValue)
        {
            this.SetParam(ParamSection, ParamName, ParamValue);
            return true;
        }

        public bool AddAdditionalParameter(string ParamName, string ParamValue) {
            throw new NotImplementedException();
        }

        public void AddDatasetInfo(string DatasetName, int DatasetID)
        {
            throw new NotImplementedException();
        }

        public void AddResultFileExtensionToSkip(string Extension)
        {
            if (!m_ResultFileExtensionsToSkip.Contains(Extension))
                m_ResultFileExtensionsToSkip.Add(Extension);
        }

        public void AddResultFileToKeep(string FileName)
        {
            throw new NotImplementedException();
        }

        public void AddResultFileToSkip(string FileName)
        {
            throw new NotImplementedException();
        }

        public void AddServerFileToDelete(string FileName)
        {
            throw new NotImplementedException();
        }

        public void CloseTask(CloseOutType CloseOut, string CompMsg)
        {
            throw new NotImplementedException();
        }

        public void CloseTask(CloseOutType CloseOut, string CompMsg, int EvalCode, string EvalMessage)
        {
            throw new NotImplementedException();
        }

        public string GetCurrentJobToolDescription() {
            return "Test stub";
        }

        public bool GetJobParameter(string Name, bool ValueIfMissing)
        {

            string strValue = null;

            try
            {
                strValue = this.GetParam(Name);

                if (string.IsNullOrEmpty(strValue))
                {
                    return ValueIfMissing;
                }

            }
            catch
            {
                return ValueIfMissing;
            }

            // Note: if strValue is not True or False, this will throw an exception; the calling procedure will need to handle that exception
            return Convert.ToBoolean(strValue);

        }

        public string GetJobParameter(string Name, string ValueIfMissing)
        {

            string strValue = null;

            try
            {
                strValue = this.GetParam(Name);

                if (string.IsNullOrEmpty(strValue))
                {
                    return ValueIfMissing;
                }

            }
            catch
            {
                return ValueIfMissing;
            }

            return strValue;
        }

        public float GetJobParameter(string Name, float ValueIfMissing)
        {
            string strValue = null;

            try
            {
                strValue = this.GetParam(Name);

                if (string.IsNullOrEmpty(strValue))
                {
                    return ValueIfMissing;
                }

            }
            catch
            {
                return ValueIfMissing;
            }

            // Note: if strValue is not a number, this will throw an exception; the calling procedure will need to handle that exception
            return Convert.ToSingle(strValue);
        }

        public int GetJobParameter(string Name, int ValueIfMissing)
        {
            string strValue = null;

            try
            {
                strValue = this.GetParam(Name);

                if (string.IsNullOrEmpty(strValue))
                {
                    return ValueIfMissing;
                }

            }
            catch
            {
                return ValueIfMissing;
            }

            // Note: if strValue is not a number, this will throw an exception; the calling procedure will need to handle that exception
            return Convert.ToInt32(strValue);

        }

        public short GetJobParameter(string Name, short ValueIfMissing)
        {
            return Convert.ToInt16(GetJobParameter(Name, Convert.ToInt32(ValueIfMissing)));
        }

        public bool GetJobParameter(string Section, string Name, bool ValueIfMissing)
        {
            return GetJobParameter(Name, ValueIfMissing);
        }

        public float GetJobParameter(string Section, string Name, float ValueIfMissing)
        {
            return GetJobParameter(Name, ValueIfMissing);
        }

        public int GetJobParameter(string Section, string Name, int ValueIfMissing)
        {
            return GetJobParameter(Name, ValueIfMissing);
        }

        public string GetJobParameter(string Section, string Name, string ValueIfMissing)
        {
            return GetJobParameter(Name, ValueIfMissing);
        }

        public string GetParam(string Name)
        {
            string val = "";
            if (mParms.ContainsKey(Name))
            {
                val = mParms[Name];
            }
            return val;
        }

        public string GetParam(string Section, string Name)
        {
            string val = "";
            if (mParms.ContainsKey(Name))
            {
                val = mParms[Name];
            }
            return val;
        }

        public void RemoveResultFileToSkip(string FileName)
        {
            throw new NotImplementedException();
        }

        public clsDBTask.RequestTaskResult RequestTask()
        {
            throw new NotImplementedException();
        }

        public void SetParam(string KeyName, string Value) {
            if (mParms.ContainsKey(KeyName))
                mParms[KeyName] = Value;
            else
                mParms.Add(KeyName, Value);
        }

        /// <summary>
        /// Add or update a parameter; note that Section is ignored by this class
        /// </summary>
        /// <param name="Section">Section name (ignored)</param>
        /// <param name="KeyName">Parameter name</param>
        /// <param name="Value">Parameter value</param>
        public void SetParam(string Section, string KeyName, string Value) {			
            SetParam(KeyName, Value);
        }

        /// <summary>
        /// Add or update a parameter; note that ParamSection is ignored by this class
        /// </summary>
        /// <param name="ParamSection">Section name (ignored)</param>
        /// <param name="ParamName">Parameter name</param>
        /// <param name="ParamValue">Parameter value</param>
        /// <returns></returns>
        public bool AddAdditionalParameter(string ParamSection, string ParamName, bool ParamValue)
        {
            SetParam(ParamSection, ParamName, ParamValue.ToString());
            return true;
        }

        #endregion
    }
}
