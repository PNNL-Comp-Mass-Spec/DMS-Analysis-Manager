
using PRISM;
using System;
using System.Collections.Generic;

namespace AnalysisManager_RepoPkgr_PlugIn
{
    /// <summary>
    /// Class that allows SQL for database queries to be defined.
    /// Queries are defined as template objects that are accessed using a name tag.
    /// Clients obtain a template object using its name tag
    /// and use it to generate SQL for actual database query
    /// </summary>
    public class QueryDefinitions : EventNotifier
    {
        /// <summary>
        /// Data source enums
        /// </summary>
        public enum TagName { Undefined, Main, Broker };

        /// <summary>
        /// Internal list of connection strings
        /// </summary>
        private readonly Dictionary<TagName, string> mDbConnectionStrings = new Dictionary<TagName, string>();

        /// <summary>
        /// Set connection string for given database tag name
        /// </summary>
        /// <param name="dbTag"></param>
        /// <param name="connectionString"></param>
        public void SetCnStr(TagName dbTag, string connectionString)
        {
            // Add/update the dictionary
            mDbConnectionStrings[dbTag] = connectionString;
        }

        /// <summary>
        /// Get connection string for given database tag name
        /// </summary>
        /// <param name="dbTag"></param>
        public string GetCnStr(TagName dbTag)
        {
            if (mDbConnectionStrings.TryGetValue(dbTag, out var connectionString))
                return connectionString;

            OnErrorEvent(String.Format("{0} not found in the Connection Strings dictionary", dbTag));
            return string.Empty;
        }

        /// <summary>
        /// Get connection string for given query template
        /// </summary>
        /// <param name="queryTemplateName"></param>
        public string GetCnStr(string queryTemplateName)
        {
            var queryDef = GetQueryTemplate(queryTemplateName);
            if (queryDef.DatabaseTagName == TagName.Undefined)
                return string.Empty;

            return GetCnStr(queryDef.DatabaseTagName);
        }

        // SQL templates
        private readonly Dictionary<string, QueryDefinition> mQueryTemplates = new Dictionary<string, QueryDefinition>
                                                                         {
                {"DataPkgJobsQueryTemplate",  new QueryDefinition {
                  BaseSQL = "SELECT * FROM V_Mage_Data_Package_Analysis_Jobs WHERE {0} = {1}",
                  FilterSQL = "Tool LIKE '{0}%'"
                }},
                {"DataPkgDatasetsQueryTemplate",  new QueryDefinition {
                  BaseSQL = "SELECT * FROM V_Mage_Data_Package_Datasets WHERE {0} = {1}"
                }},
                {"DataPkgAggJobsQueryTemplate",  new QueryDefinition {
                  BaseSQL = "SELECT * FROM V_DMS_Data_Package_Aggregation_Jobs WHERE {0} = {1} ORDER BY Dataset, Tool, Job, Step",
                  DatabaseTagName = TagName.Broker
                }}
              };

        /// <summary>
        /// Get query definition template for given query name
        /// </summary>
        /// <param name="templateName">Name of query to use</param>
        /// <returns>Query template</returns>
        public QueryDefinition GetQueryTemplate(string templateName)
        {
            if (mQueryTemplates.TryGetValue(templateName, out var queryDef))
                return queryDef;

            OnErrorEvent(string.Format("{0} not found in the Query Templates dictionary", templateName));

            var undefinedQueryDef = new QueryDefinition {
                DatabaseTagName = TagName.Broker
            };
            return undefinedQueryDef;
        }

        /// <summary>
        /// Internal class to contain a single query definition
        /// </summary>
        public class QueryDefinition
        {
            public string BaseSQL { get; set; }

            private string IdColName { get; }

            public TagName DatabaseTagName { get; set; }

            public string FilterSQL { get; set; }

            /// <summary>
            /// Constructor for internal class
            /// </summary>
            public QueryDefinition()
            {
                // default values
                DatabaseTagName = TagName.Main;
                IdColName = "Data_Package_ID";
                BaseSQL = string.Empty;
            }

            /// <summary>
            /// build query from its parts and return it
            /// </summary>
            /// <param name="id">Value to use for primary identifier for query (typically data package ID)</param>
            /// <param name="filter">Value to use for supplemental filter (if one is defined in query template definition)</param>
            /// <returns>SQL</returns>
            public string Sql(string id, string filter = "")
            {
                if (string.IsNullOrWhiteSpace(BaseSQL))
                    return string.Empty;

                var sql = string.Format(BaseSQL, IdColName, id);
                if (string.IsNullOrEmpty(filter) || string.IsNullOrEmpty(FilterSQL))
                    return sql;

                var sqlWithSecondFilter = sql + " AND " + string.Format(FilterSQL, filter);
                return sqlWithSecondFilter;
            }
        } // end class QueryDefinition
    }
}
