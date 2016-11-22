
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
    public class QueryDefinitions
    {
        // set of tags for accessing connection strings
        public enum TagName { Main, Broker };

        // inernal list of connection strings
        private readonly Dictionary<QueryDefinitions.TagName, string> _dbConnectionStrings =
          new Dictionary<QueryDefinitions.TagName, string>();

        /// <summary>
        /// Set connection string for given database tag name
        /// </summary>
        /// <param name="dbTag"></param>
        /// <param name="connectionString"></param>
        public void SetCnStr(TagName dbTag, string connectionString)
        {
            _dbConnectionStrings[dbTag] = connectionString;
        }
        /// <summary>
        /// Get connection string for given database tag name
        /// </summary>
        /// <param name="dbTag"></param>
        /// <returns></returns>
        public string GetCnStr(QueryDefinitions.TagName dbTag)
        {
            return _dbConnectionStrings[dbTag];
        }
        /// <summary>
        /// Get connection string for given 
        /// </summary>
        /// <param name="queryTmpltName"></param>
        /// <returns></returns>
        public string GetCnStr(string queryTmpltName)
        {
            var queryDef = GetQueryTmplt(queryTmpltName);
            return GetCnStr(queryDef.DatabaseTagName);
        }

        // SQL templates
        private readonly Dictionary<string, QueryDefinition> _queryTmplt = new Dictionary<string, QueryDefinition>
                                                                         {
                {"DataPkgJobsQueryTemplate",  new QueryDefinition {
                  BaseSQL = "SELECT * FROM V_Mage_Data_Package_Analysis_Jobs WHERE {0} = {1}",
                  FilterSQL = "Tool LIKE '{0}%'"
                }},
                {"DataPkgDatasetsQueryTemplate",  new QueryDefinition {
                  BaseSQL = "SELECT * FROM V_Mage_Data_Package_Datasets WHERE {0} = {1}"
                }},
                {"DataPkgAggJobsQueryTemplate",  new QueryDefinition {
                  BaseSQL = "SELECT * FROM V_DMS_Data_Package_Aggregation_Jobs WHERE {0} = {1}",
                  DatabaseTagName = QueryDefinitions.TagName.Broker
                }}
              };

        /// <summary>
        /// Get query definition template for given query name
        /// </summary>
        /// <param name="queryName">Name of query to use</param>
        /// <returns>Query template</returns>
        public QueryDefinition GetQueryTmplt(string queryName)
        {
            return _queryTmplt[queryName];
        }

        /// <summary>
        /// Internal class to contain a single query definition
        /// </summary>
        public class QueryDefinition
        {
            public string BaseSQL { get; set; }
            private string IdColName { get; set; }
            public QueryDefinitions.TagName DatabaseTagName { get; set; }
            public string FilterSQL { get; set; }

            /// <summary>
            /// Constructor for internal class
            /// </summary>
            public QueryDefinition()
            {
                // default values
                DatabaseTagName = QueryDefinitions.TagName.Main;
                IdColName = "Data_Package_ID";
            }

            /// <summary>
            /// build query from its parts and return it
            /// </summary>
            /// <param name="id">Value to use for primary identifier for query (typically data package ID)</param>
            /// <param name="filter">Valsue to use for supplemental filter (if one is defined in query template definition)</param>
            /// <returns>SQL</returns>
            public string Sql(string id, string filter = "")
            {
                var sql = String.Format(BaseSQL, IdColName, id);
                if (!String.IsNullOrEmpty(filter) && !String.IsNullOrEmpty(FilterSQL))
                {
                    sql += " AND " + String.Format(FilterSQL, filter);
                }
                return sql;
            }

        } // end class QueryDefinition
    }
}
