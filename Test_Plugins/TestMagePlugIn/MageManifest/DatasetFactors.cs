using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Mage;
using log4net;

// accumulate a unique list of dataset IDs from standard tabular input
// and use to output factors for all of them on standard tabular output

namespace MageManifest {

    class DatasetFactors : BaseModule {

        private static readonly ILog traceLog = LogManager.GetLogger("TraceLog");

        #region Member Variables

        private Dictionary<string, string> IDList = new Dictionary<string, string>();

        // handle to the currently running sub-pipeline
        private ProcessingPipeline mPipeline = null;

        #endregion

        #region Properties

        public string DatasetColName { get; set; }

        public string Server { get; set; }

        public string Database { get; set; }

        public string OutputFilePath { get; set; }

        public string OutputTableName { get; set; }

        public string OutputType { get; set; }

        public string OutputFormat { get; set; }

        #endregion

        #region Constructors

        public DatasetFactors() {
            DatasetColName = "Dataset_ID";
            Server = "";
            Database = "";
            OutputFilePath = "";
        }

        #endregion

        #region IBaseModule Members

//        public override event DataRowHandler DataRowAvailable;
//        public override event ColumnDefHandler ColumnDefAvailable;
        public override event StatusMessageUpdated OnStatusMessageUpdated;


        public override void HandleDataRow(object[] vals, ref bool stop) {
            if (vals != null) {
                int DatasetIDIdx = InputColumnPos[DatasetColName];
                string dataset_id = vals[DatasetIDIdx].ToString();
                IDList[dataset_id] = "";
            } else {
                // build pipeline to get factors for all datasets in datasets buffer and output them to delimited file

                mPipeline = MakeDatasetQueryPipeline();
                mPipeline.OnStatusMessageUpdated += HandleStatusMessageUpdated;
                mPipeline.RunRoot(null); // we are already in a pipeline thread - don't run sub-pipeline in a new one
            }
        }

        #endregion

        #region Helper functions

        private string GetIDList() {
            return string.Join(",", IDList.Keys.ToArray());
        }

        #endregion

        #region Functions that build pipelines

        private ProcessingPipeline MakeDatasetQueryPipeline() {
            ProcessingPipeline pipeline = new ProcessingPipeline("DatasetQueryPipeline");

            // make source module in pipeline to read jobs from DMS as database query
            // and connect it to display
            pipeline.RootModule = pipeline.MakeModule("Reader", "MSSQLReader");
            //
            string sql = "";
            switch (OutputFormat) {
                case "Factors":
                    sql = "SELECT Dataset, Dataset_ID, Factor, Value FROM V_Custom_Factors_List_Report WHERE Dataset_ID IN (XXX)";
                    break;
                case "Dataset Metadata":
                    sql = sql = "SELECT * FROM V_Mage_Dataset_Factor_Metadata  WHERE ID IN (XXX)";
                    break;
            }
            sql = sql.Replace("XXX", GetIDList());  // FUTURE: make sure that sql isn't too big
            pipeline.SetModuleParameter("Reader", "server", Server);
            pipeline.SetModuleParameter("Reader", "database", Database);
            pipeline.SetModuleParameter("Reader", "sqlText", sql);

            switch (OutputType) {
                case "Tab-Delimited File":
                    pipeline.MakeModule("Writer", "DelimitedFileWriter");
                    pipeline.SetModuleParameter("Writer", "FilePath", OutputFilePath);
                    break;
                case "SQLite Database":
                    pipeline.MakeModule("Writer", "SQLiteWriter");
                    pipeline.SetModuleParameter("Writer", "DbPath", OutputFilePath);
                    pipeline.SetModuleParameter("Writer", "TableName", OutputTableName);
                    break;
            }
                 pipeline.ConnectModules("Reader", "Writer");

            return pipeline;
        }


        #endregion

        #region Event Handlers

        private void HandleStatusMessageUpdated(string Message) {
            if (OnStatusMessageUpdated != null) {
                OnStatusMessageUpdated(Message);
            }
        }

        #endregion

    }
}
