using System.Collections.Generic;

//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 12/18/2007
//
//*********************************************************************************************************

namespace AnalysisManagerBase
{
    /// <summary>
    /// Interface for manager params storage class
    /// </summary>
    public interface IMgrParams
    {

        string ErrMsg { get; }
        #region "Methods"
        void AckManagerUpdateRequired();
        bool DisableManagerLocally();

        /// <summary>
        /// Gets a parameter from the manager parameters dictionary
        /// </summary>
        /// <param name="ItemKey">Key name for item</param>
        /// <returns>String value associated with specified key</returns>
        /// <remarks>Returns empty string if key isn't found</remarks>
        string GetParam(string ItemKey);

        /// <summary>
        /// Gets a parameter from the manager parameters dictionary
        /// </summary>
        /// <param name="ItemKey">Key name for item</param>
        /// <param name="ValueIfMissing">Value to return if the parameter is not found</param>
        /// <returns>Value for specified parameter; ValueIfMissing if not found</returns>
        string GetParam(string ItemKey, string ValueIfMissing);
        bool GetParam(string ItemKey, bool ValueIfMissing);
        int GetParam(string ItemKey, int ValueIfMissing);

        bool LoadDBSettings();
        bool LoadSettings(Dictionary<string, string> ConfigFileSettings);
        #endregion
        void SetParam(string ItemKey, string ItemValue);

    }

}