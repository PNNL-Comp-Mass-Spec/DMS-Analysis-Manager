using System.Collections.Generic;

namespace AnalysisManager_Mage_PlugIn
{
    /// <summary>
    /// Get SQL for queries
    /// </summary>
    internal class SQL
    {
        // holds a single query definition template
        public class QueryTemplate
        {
            public string TemplateSQL { get; private set; }
            public string ParamNameList { get; private set; }
            public QueryTemplate(string sql, string list)
            {
                TemplateSQL = sql;
                ParamNameList = list;
            }
        }

        // Definition of query templates
        private static readonly Dictionary<string, QueryTemplate> QueryTemplates = new Dictionary<string, QueryTemplate> {
            {"JobsFromDataPackageID",
                new QueryTemplate("SELECT * FROM V_Mage_Data_Package_Analysis_Jobs WHERE Data_Package_ID = {0}",
                    "DataPackageID") },
            {"JobsFromDataPackageIDForTool",
                new QueryTemplate("SELECT * FROM V_Mage_Data_Package_Analysis_Jobs WHERE Data_Package_ID = {0} AND Tool = '{1}'",
                    "DataPackageID, Tool") },
            {"FactorsFromDataPackageID",
                new QueryTemplate(
                    "SELECT Dataset, Dataset_ID, Factor, Value " + 
                    "FROM DMS5.dbo.V_Custom_Factors_List_Report " + 
                    "WHERE Dataset IN (SELECT DISTINCT Dataset FROM V_Mage_Data_Package_Analysis_Jobs WHERE Data_Package_ID = {0})",
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
        public static QueryTemplate GetQueryTemplate(string queryName)
        {
            return QueryTemplates[queryName];
        }

        /// <summary>
        /// Build SQL statement from query template and parameter values
        /// </summary>
        /// <param name="qt">Query template object</param>
        /// <param name="paramVals">array of parameter values to be substituted into the query template</param>
        /// <returns>SQL</returns>
        public static string GetSQL(QueryTemplate qt, string[] paramVals)
        {
            return string.Format(qt.TemplateSQL, paramVals);
        }
    }
}
