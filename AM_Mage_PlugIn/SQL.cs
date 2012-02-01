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
    public class SQL {

        // holds a single query definition template
        public class QueryTemplate {
            public string templateSQL { get; set; }
            public string paramNameList { get; set; }
            public QueryTemplate(string sql, string list) {
                templateSQL = sql;
                paramNameList = list;
            }
        }

        // Definition of query templates
        private static Dictionary<string, QueryTemplate> queryTemplates = new Dictionary<string, QueryTemplate> {
            {"JobsFromDataPackageID", 
                new QueryTemplate("SELECT * FROM V_Mage_Data_Package_Analysis_Jobs WHERE Data_Package_ID = {0}", 
                    "DataPackageID") },
            {"JobsFromDataPackageIDForTool",
                new QueryTemplate("SELECT * FROM V_Mage_Data_Package_Analysis_Jobs WHERE Data_Package_ID = {0} AND Tool = '{1}'", 
                    "DataPackageID, Tool") },
            {"FactorsFromDataPackageID",
                new QueryTemplate("SELECT Dataset, Dataset_ID, Factor, Value FROM DMS5.dbo.V_Custom_Factors_List_Report WHERE Dataset IN (SELECT DISTINCT Dataset FROM V_Mage_Data_Package_Analysis_Jobs WHERE Data_Package_ID = {0})", 
                    "DataPackageID") },
            {"JobDatasetsFromDataPackageIDForTool",
                new QueryTemplate("SELECT * FROM V_Mage_Dataset_List WHERE Dataset IN (SELECT DISTINCT Dataset FROM S_V_Data_Package_Analysis_Jobs_Export WHERE Data_Package_ID = {0})", 
                    "DataPackageID, Tool") },
        };

        /// <summary>
        /// Get the query template for the given query name
        /// </summary>
        /// <param name="queryName">Name of query template</param>
        /// <returns>QueryTemplate object</returns>
        public static QueryTemplate GetQueryTemplate(string queryName) {
            return queryTemplates[queryName];
        }
 
        /// <summary>
        /// Build SQL statement from query template and parameter values
        /// </summary>
        /// <param name="qt">Query template object</param>
        /// <param name="paramVals">array of parameter values to be substituted into the query template</param>
        /// <returns>SQL</returns>
        public static string GetSQL(QueryTemplate qt, string[] paramVals) {
            return string.Format(qt.templateSQL, paramVals);
        }

    }
}
