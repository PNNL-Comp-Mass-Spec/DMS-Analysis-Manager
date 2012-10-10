using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using MageClientLib;
using Mage;
using System.IO;
using log4net;

namespace MageFactors {

    public partial class Form1 : Form {

        private static readonly ILog traceLog = LogManager.GetLogger("TraceLog");

        ProcessingPipeline mPipeline = null;

        public Form1() {
            InitializeComponent();

            traceLog.Info("Starting");

            // wire me up to receive commands from other GUI components
            datasetListPanel1.OnAction += DoCommand;
            sqLiteOutputPanel1.OnAction += DoCommand;
            statusPanel1.OnAction += DoCommand;

            // tell pipeline processor where to look for loadable module DLLs
            FileInfo fi = new FileInfo(System.Windows.Forms.Application.ExecutablePath);
            ModuleDiscovery.ExternalModuleFolder = fi.DirectoryName;
        }

        private void DoCommand(string command) {

            // cancel the currently running pipeline
            if (command == "cancel_operation" && mPipeline != null && mPipeline.Running) {
                mPipeline.Cancel();
                return;
            }
            // don't allow another pipeline if one is currently running
            if (mPipeline != null && mPipeline.Running) {
                MessageBox.Show("Pipeline is already active");
                return;
            }
            // build and run the pipeline appropriate to the command
            switch (command) {
                case "get_dataset_metadata":
                    sqLiteOutputPanel1.TableName = "T_Mage_Dataset_Metadata";
                    mPipeline = MakeDatasetQueryPipeline("Dataset Metadata");
                    break;
                case "get_dataset_factor_summary":
                    sqLiteOutputPanel1.TableName = "T_Mage_Dataset_Factor_Summary";
                    mPipeline = MakeDatasetQueryPipeline("Dataset Factor Count");
                    break;
                case "get_factors":
                    sqLiteOutputPanel1.TableName = "T_Mage_Dataset_Factors";
                    mPipeline = MakeDatasetQueryPipeline("Factors");
                    break;
                case "get_factors_crosstab":
                    sqLiteOutputPanel1.TableName = "T_Mage_Dataset_Factors_Crosstab";
                    mPipeline = MakeXXXPipeline("Factors Crosstab");
                    break;
                case "output_all_to_sqlite_db":
                    mPipeline = MakeFactorOutputPipeline("Output", LVPipelineSource.Modes.All);
                    break;
                case "output_selected_to_sqlite_db":
                    mPipeline = MakeFactorOutputPipeline("Output", LVPipelineSource.Modes.Selected);
                    break;
                default:
                    return;
            }
            mPipeline.OnStatusMessageUpdated += HandleStatusMessageUpdated;
            mPipeline.Run();
        }

        #region Functions that build pipelines

        private ProcessingPipeline MakeDatasetQueryPipeline(string title) {
            ProcessingPipeline pipeline = new ProcessingPipeline("DatasetQueryPipeline");

            // make source module in pipeline to read jobs from DMS as database query
            // and connect it to display
            pipeline.RootModule = pipeline.MakeModule("Datasets", "MSSQLReader");
            ListDisplayControl.ConnectToPipeline(title, pipeline, "Datasets", listDisplayControl1, 50);

            // set module parameters and return pipeline
            pipeline.SetModuleParameters("Datasets", GetModuleParamsForReader(title));
            return pipeline;
        }

        private ProcessingPipeline MakeFactorOutputPipeline(string title, LVPipelineSource.Modes mode) {
            ProcessingPipeline pipeline = new ProcessingPipeline("FactorOutputPipeline");

            // make client module as a data source for a list view control and add it to pipeline
            LVPipelineSource source = new LVPipelineSource(listDisplayControl1, mode);
            pipeline.RootModule = pipeline.AddModule("Selected Items", source);

            pipeline.MakeModule("Writer", "SQLiteWriter");
            pipeline.ConnectModules("Selected Items", "Writer");

            // set module parameters and return pipeline
            pipeline.SetModuleParameters("Writer", GetModuleParamsForSQLite());
            return pipeline;
        }

        private ProcessingPipeline MakeXXXPipeline(string title) {
            ProcessingPipeline pipeline = new ProcessingPipeline("FactorsCrosstabPipeline");

            // make source module in pipeline to read jobs from DMS as database query
            // and connect it to display
            pipeline.RootModule = pipeline.MakeModule("Datasets", "MSSQLReader");

            pipeline.AddModule("CrossTab", new FactorCrosstab());
            pipeline.ConnectModules("Datasets", "CrossTab");

            ListDisplayControl.ConnectToPipeline(title, pipeline, "CrossTab", listDisplayControl1, 50);

            // set module parameters and return pipeline
            pipeline.SetModuleParameters("Datasets", GetModuleParamsForReader(title));
            return pipeline;
        }


        #endregion

        #region Helper functions for building pipelines

        private List<KeyValuePair<string, string>> GetModuleParamsForReader(string title) {
            string sql = "";
            switch (title) {
                case "Dataset Metadata":
                    sql = "SELECT * FROM V_Mage_Dataset_Factor_Metadata WHERE Dataset LIKE '%XXX%'";
                    break;
                case "Dataset Factor Count":
                    sql = "SELECT * FROM V_Mage_Dataset_Factor_Summary WHERE Dataset LIKE '%XXX%'";
                    break;
                case "Factors":
                case "Factors Crosstab":
                    sql = "SELECT Dataset, Dataset_ID, Factor, Value FROM V_Custom_Factors_List_Report WHERE Dataset LIKE '%XXX%'";
                    break;

            }
            string dataset = datasetListPanel1.DatasetName;
            sql = sql.Replace("XXX", datasetListPanel1.DatasetName);
            List<KeyValuePair<string, string>> moduleParams = new List<KeyValuePair<string, string>>();
            moduleParams.Add(new KeyValuePair<string, string>("server", "gigasax"));
            moduleParams.Add(new KeyValuePair<string, string>("database", "DMS5"));
            moduleParams.Add(new KeyValuePair<string, string>("sqlText", sql));
            return moduleParams;
        }

        private List<KeyValuePair<string, string>> GetModuleParamsForSQLite() {
            List<KeyValuePair<string, string>> moduleParams = new List<KeyValuePair<string, string>>();
            moduleParams.Add(new KeyValuePair<string, string>("DbPath", sqLiteOutputPanel1.DatabaseName));
            moduleParams.Add(new KeyValuePair<string, string>("TableName", sqLiteOutputPanel1.TableName));
            return moduleParams;
        }

        #endregion

        #region Functions for Handling Status Messages

        private void HandleStatusMessageUpdated(string Message) {
            // handle the status update messages from the currently running pipeline
            Console.WriteLine(Message); // FUTURE: logging?

            // the current pipleline will call this function from its own thread
            // we need to do the cross-thread thing to update the GUI
            StatusMessageUpdated ncb = SetStatusMessage;
            Invoke(ncb, new object[] { Message });
        }
        private void SetStatusMessage(string Message) {
            // this is targeted by the cross-thread invoke from HandleStatusMessageUpdated
            statusPanel1.SetStatusMessage(Message);
        }

        #endregion

    }
}
