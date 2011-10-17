using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using AnalysisManagerBase;

namespace AnalysisManager_Mage_PlugIn {

    /// <summary>
    /// Get SQL for queries
    /// </summary>
    class SQL {

        /// <summary>
        /// Indexed list of sql templates
        /// </summary>
        private static Dictionary<string, string> sqlTemplates = new Dictionary<string, string> { 
            {"JobsFromDataPackageID", "SELECT * FROM V_Mage_Data_Package_Analysis_Jobs WHERE Data_Package_ID = {0}"},
            {"FactorsFromDataPackageID", "SELECT Dataset, Dataset_ID, Factor, Value FROM DMS5.dbo.V_Custom_Factors_List_Report WHERE Dataset IN (SELECT DISTINCT Dataset FROM V_Mage_Data_Package_Analysis_Jobs WHERE Data_Package_ID = {0})"} // FUTURE: better query
 
        };

        /// <summary>
        /// Indexed list of parameter names for templates
        /// </summary>
        private static Dictionary<string, string> sqlParamNames = new Dictionary<string, string> { 
            {"JobsFromDataPackageID", "DataPackageID"},
            {"FactorsFromDataPackageID", "DataPackageID"} 
 
        };

        /// <summary>
        /// Lookup sql template using key, and, if found, add in values from args
        /// </summary>
        /// <param name="key">Index to sql template</param>
        /// <param name="args">Runtime values to insert into template</param>
        /// <returns></returns>
        public static string GetSQL(string sourceParamName, IJobParams parms) {
            String sourceQueryName = parms.GetParam(sourceParamName);
            string sql = "";
            string sqlTemplate = sqlTemplates[sourceQueryName];
            if (sqlTemplate != "") {
                Object[] vals = GetParamValues(sourceQueryName, parms);
                sql = string.Format(sqlTemplate, vals);
            }
            return sql;
        }

        /// <summary>
        /// look up list of parameter names using key, and return array of corresponding values
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        private static object[] GetParamValues(string sourceQueryName, IJobParams parms) {
            List<string> paramValues = new List<string>();
            foreach (string paramName in sqlParamNames[sourceQueryName].Split(',')) {
                string val = parms.GetParam(paramName.Trim());
                paramValues.Add(val);
            }
            return paramValues.ToArray();
        }
    }
}
