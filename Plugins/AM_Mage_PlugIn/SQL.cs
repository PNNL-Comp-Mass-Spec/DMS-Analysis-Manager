using System.Collections.Generic;

namespace AnalysisManager_Mage_PlugIn
{
    /// <summary>
    /// Get SQL for queries
    /// </summary>
    internal static class SQL
    {
        // Ignore Spelling: mage, sql

        // holds a single query definition template
        public class QueryTemplate(string sql, string list)
        {
            public string TemplateSQL { get; } = sql;
            public string ParamNameList { get; } = list;
        }

        // Definition of query templates
        private static readonly Dictionary<string, QueryTemplate> QueryTemplates = new()
        {
            {
                "JobsFromDataPackageID",
                new QueryTemplate(
                    "SELECT * FROM V_Mage_Data_Package_Analysis_Jobs " +
                    "WHERE Data_Pkg_ID = {0} " +
                    "ORDER BY Dataset",
                    "DataPackageID")
            },
            {
                "JobsFromDataPackageIDForTool",
                new QueryTemplate(
                    "SELECT * FROM V_Mage_Data_Package_Analysis_Jobs " +
                    "WHERE Data_Pkg_ID = {0} AND Tool = '{1}' " +
                    "ORDER BY Dataset",
                    "DataPackageID, Tool")
            },
            {
                "FactorsFromDataPackageID",
                new QueryTemplate(
                    "SELECT Dataset, Dataset_ID, Factor, Value " +
                    "FROM V_Custom_Factors_List_Report " +
                    "WHERE Dataset IN (SELECT DISTINCT Dataset FROM V_Mage_Data_Package_Analysis_Jobs WHERE Data_Pkg_ID = {0}) " +
                    "ORDER BY Dataset",
                    "DataPackageID")
            },
            {
                "JobDatasetsFromDataPackageID",
                new QueryTemplate(
                    "SELECT * FROM V_Mage_Dataset_List " +
                    "WHERE Dataset_ID IN (SELECT DISTINCT Dataset_ID FROM V_Data_Package_Analysis_Jobs_Export WHERE Data_Pkg_ID = {0}) " +
                    "ORDER BY Dataset",
                    "DataPackageID")
            },
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
        /// <param name="paramValues">array of parameter values to be substituted into the query template</param>
        /// <returns>SQL</returns>
        public static string GetSQL(QueryTemplate qt, string[] paramValues)
        {
            return string.Format(qt.TemplateSQL, paramValues);
        }
    }
}
