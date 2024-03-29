using System;
using System.Collections.Generic;
using AnalysisManagerBase;

namespace TestAScorePlugIn
{
    class JobParamsStub : IJobParams
    {
        private readonly Dictionary<string, string> mParms;

        // List of file extensions to NOT move to the result folder; comparison checks if the end of the filename matches any entry ResultFileExtensionsToSkip
        protected SortedSet<String> m_ResultFileExtensionsToSkip = new SortedSet<String>(StringComparer.CurrentCultureIgnoreCase);

        public JobParamsStub(Dictionary<string, string> parms) {
            mParms = parms;
        }

        public Dictionary<string, int> DatasetInfoList => new Dictionary<string, int>();

        public SortedSet<string> ResultFilesToKeep => new SortedSet<String>();

        public SortedSet<string> ResultFilesToSkip => new SortedSet<String>();

        public SortedSet<string> ResultFileExtensionsToSkip => m_ResultFileExtensionsToSkip;

        public SortedSet<string> ServerFilesToDelete => new SortedSet<String>();

        public bool TaskClosed { get; set; }

        public bool AddAdditionalParameter(string ParamSection, string ParamName, string ParamValue)
        {
            SetParam(ParamSection, ParamName, ParamValue);
            return true;
        }

        public bool AddAdditionalParameter(string sectionName, string paramName, bool paramValue)
        {
            SetParam(sectionName, paramName, paramValue.ToString());
            return true;
        }

        public bool AddAdditionalParameter(string ParamName, string ParamValue) {
            SetParam("", ParamName, ParamValue);
            return true;

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

        public void CloseTask(CloseOutType closeOut, string compMsg)
        {
            throw new NotImplementedException();
        }

        public void CloseTask(CloseOutType closeOut, string compMsg, int evalCode, string evalMsg)
        {
            throw new NotImplementedException();
        }

        public Dictionary<string, string> GetAllParametersForSection(string sectionName)
        {
            throw new NotImplementedException();
        }

        public List<string> GetAllSectionNames()
        {
            throw new NotImplementedException();
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

        public string GetCurrentJobToolDescription() {
            return "Test stub";
        }

        public string GetJobStepDescription()
        {
            throw new NotImplementedException();
        }

        public bool GetJobParameter(string Name, bool ValueIfMissing)
        {

            string paramValue;

            try
            {
                paramValue = GetParam(Name);

                if (string.IsNullOrEmpty(paramValue))
                {
                    return ValueIfMissing;
                }

            }
            catch
            {
                return ValueIfMissing;
            }

            // Note: if paramValue is not True or False, this will throw an exception; the calling procedure will need to handle that exception
            return Convert.ToBoolean(paramValue);

        }

        public string GetJobParameter(string Name, string ValueIfMissing)
        {

            string paramValue;

            try
            {
                paramValue = GetParam(Name);

                if (string.IsNullOrEmpty(paramValue))
                {
                    return ValueIfMissing;
                }

            }
            catch
            {
                return ValueIfMissing;
            }

            return paramValue;
        }

        public float GetJobParameter(string Name, float ValueIfMissing)
        {
            string paramValue;

            try
            {
                paramValue = GetParam(Name);

                if (string.IsNullOrEmpty(paramValue))
                {
                    return ValueIfMissing;
                }

            }
            catch
            {
                return ValueIfMissing;
            }

            // Note: if paramValue is not a number, this will throw an exception; the calling procedure will need to handle that exception
            return Convert.ToSingle(paramValue);
        }

        public int GetJobParameter(string Name, int ValueIfMissing)
        {
            string paramValue;

            try
            {
                paramValue = GetParam(Name);

                if (string.IsNullOrEmpty(paramValue))
                {
                    return ValueIfMissing;
                }

            }
            catch
            {
                return ValueIfMissing;
            }

            // Note: if paramValue is not a number, this will throw an exception; the calling procedure will need to handle that exception
            return Convert.ToInt32(paramValue);

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
            var val = "";

            if (mParms.ContainsKey(Name))
            {
                val = mParms[Name];
            }
            return val;
        }

        public string GetParam(string Section, string Name)
        {
            var val = "";

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

        public DBTask.RequestTaskResult RequestTask()
        {
            throw new NotImplementedException();
        }

        public void SetParam(string KeyName, string Value) {
            if (mParms.ContainsKey(KeyName))
                mParms[KeyName] = Value;
            else
                mParms.Add(KeyName, Value);
        }

        public void SetParam(string Section, string KeyName, string Value) {
            SetParam(KeyName, Value);
        }
    }
}
